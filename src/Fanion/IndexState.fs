/// F# indexing authority persisted in SQLite: hints never establish revisions,
/// builds are leased, and publication is fenced on the complete desired set.
module Fanion.IndexState

open System
open System.Collections.Generic
open System.IO
open System.Security.Cryptography
open System.Text.RegularExpressions
open System.Threading.Tasks
open Microsoft.Data.Sqlite
open Fidelity.Data.JSON
open Fanion.Canonical

exception StateError of string
exception BuildInProgress of string

let private idPattern = Regex(@"\A[A-Za-z0-9][A-Za-z0-9._:/\-]{0,127}\z", RegexOptions.Compiled)
let private revisionPattern = Regex(@"\A(?:[0-9a-f]{40}|[0-9a-f]{64})\z", RegexOptions.Compiled)
let private snapshotPattern = Regex(@"\A[0-9a-f]{64}\z", RegexOptions.Compiled)
let eventKinds = set [ "push"; "site_deploy"; "spec_deploy"; "delete"; "reconcile" ]
let futureTolerance = 5.0
let settleSecondsDefault = 0.25
let maxSettleSecondsDefault = 1.0

/// Hosted wakeups only: capture Watch before reading SQLite, then await if still
/// pending. The durable state remains authoritative, including after lost hints.
type Changes(path: string) =
    let fullPath = Path.GetFullPath path
    let gate = obj ()
    let completion () = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    let mutable changed = completion ()
    let mutable failure: exn option = None
    let failed = TaskCompletionSource<exn>(TaskCreationOptions.RunContinuationsAsynchronously)
    let watcher = new FileSystemWatcher(Path.GetDirectoryName fullPath)
    let relevant candidate = candidate = fullPath || candidate = fullPath + "-wal"
    let signal () =
        lock gate (fun () ->
            if failure.IsNone then
                let previous = changed
                changed <- completion ()
                previous.TrySetResult(()) |> ignore)
    let fail error =
        lock gate (fun () ->
            if failure.IsNone then
                failure <- Some error
                failed.TrySetResult error |> ignore
                changed.TrySetException(error: exn) |> ignore)
    do
        watcher.NotifyFilter <- NotifyFilters.FileName ||| NotifyFilters.LastWrite ||| NotifyFilters.Size
        watcher.Changed.Add(fun event -> if relevant event.FullPath then signal ())
        watcher.Created.Add(fun event -> if relevant event.FullPath then signal ())
        watcher.Deleted.Add(fun event -> if relevant event.FullPath then signal ())
        watcher.Renamed.Add(fun event -> if relevant event.FullPath || relevant event.OldFullPath then signal ())
        watcher.Error.Add(fun event -> fail (IOException("SQLite change notifications failed", event.GetException())))
        watcher.EnableRaisingEvents <- true

    member _.Watch() : Task<unit> = lock gate (fun () -> changed.Task)
    /// The hosted process supervises this terminal notification failure.
    member _.Failure : Task<exn> = failed.Task

    interface IDisposable with
        member _.Dispose() =
            fail (ObjectDisposedException(nameof Changes))
            watcher.Dispose()

let private identifier (value: string) (label: string) =
    if isNull value || not (idPattern.IsMatch value) then raise (StateError(sprintf "Invalid %s; use 1–128 ASCII identifier characters" label))
    value

let private number (value: float) (label: string) (positive: bool) =
    let message = sprintf "%s must be a finite %s number" label (if positive then "positive" else "nonnegative")
    if Double.IsNaN value || Double.IsInfinity value || value < 0.0 || (positive && value = 0.0) then raise (StateError message)
    value

/// json.dumps(value, sort_keys=True, separators=(",", ":")) — ASCII payloads, so canonical bytes agree.
let private jsonText (value: J) = toText canonicalStyle value

type Plan =
    { BaseGeneration: int64 option; DesiredEpoch: int64; DesiredRevisions: Map<string, string option>
      Targets: Map<string, string option>; ChangedSources: string list; ObservationRequired: string list
      SettleUntil: float option; PlanId: string }

let private revisionsJ (revisions: Map<string, string option>) =
    Obj [ for kv in revisions -> kv.Key, (match kv.Value with Some r -> Str r | None -> Null) ]

let planBody (plan: Plan) : J =
    Obj [ "base_generation", (match plan.BaseGeneration with Some g -> Int g | None -> Null)
          "desired_epoch", Int plan.DesiredEpoch
          "desired_revisions", revisionsJ plan.DesiredRevisions
          "targets", revisionsJ plan.Targets
          "changed_sources", Arr (plan.ChangedSources |> List.map Str)
          "observation_required", Arr (plan.ObservationRequired |> List.map Str)
          "settle_until", (match plan.SettleUntil with Some s -> Float s | None -> Null) ]

let planJ (plan: Plan) : J =
    match planBody plan with Obj pairs -> Obj (pairs @ [ "plan_id", Str plan.PlanId ]) | other -> other

type Generation = { Generation: int64; SnapshotId: string; Revisions: Map<string, string option>; PublishedAt: float }

type private Row = Dictionary<string, obj>

let private readRows (command: SqliteCommand) : Row list =
    use reader = command.ExecuteReader()
    [ while reader.Read() do
        let row = Row()
        for i in 0 .. reader.FieldCount - 1 do row.[reader.GetName i] <- (if reader.IsDBNull i then null else reader.GetValue i)
        yield row ]

let private str (row: Row) (name: string) = match row.[name] with null -> null | v -> string v
let private num (row: Row) (name: string) : float option = match row.[name] with null -> None | :? float as f -> Some f | :? int64 as i -> Some(float i) | v -> Some(Convert.ToDouble v)
let private int64Of (row: Row) (name: string) : int64 = match row.[name] with :? int64 as i -> i | v -> Convert.ToInt64 v

type State(path: string, allowedSources: (string * string list) list, ?leaseSeconds: float, ?clock: unit -> float, ?settleSeconds: float, ?maxSettleSeconds: float) =
    let leaseSeconds = number (defaultArg leaseSeconds 300.0) "lease_seconds" true
    let clock = defaultArg clock (fun () -> float (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()) / 1000.0)
    let settleSeconds = number (defaultArg settleSeconds settleSecondsDefault) "settle_seconds" false
    let maxSettleSeconds = number (defaultArg maxSettleSeconds maxSettleSecondsDefault) "max_settle_seconds" false
    do if maxSettleSeconds < settleSeconds then raise (StateError "settle window must be nonnegative and bounded by max_settle_seconds")
    let dependencies : Map<string, string list> =
        if allowedSources.Length < 1 || allowedSources.Length > 256 then raise (StateError "allowed_sources must be a dependency mapping with 1–256 sources")
        let table = Dictionary<string, string list>()
        for (source, upstream) in allowedSources do
            identifier source "source ID" |> ignore
            let validated = upstream |> List.map (fun item -> identifier item "dependency ID")
            if validated.Length <> (validated |> List.distinct).Length then raise (StateError "Dependencies must be a list or tuple without duplicates")
            table.[source] <- validated |> List.sortWith (fun a b -> String.CompareOrdinal(a, b))
        for kv in table do
            for item in kv.Value do if not (table.ContainsKey item) then raise (StateError "Dependencies must name explicitly allowed sources")
        let visiting, visited = HashSet<string>(), HashSet<string>()
        let rec visit source =
            if visiting.Contains source then raise (StateError "Source dependencies must form a DAG")
            if not (visited.Contains source) then
                visiting.Add source |> ignore
                for dependency in table.[source] do visit dependency
                visiting.Remove source |> ignore
                visited.Add source |> ignore
        for source in table.Keys do visit source
        table |> Seq.map (fun kv -> kv.Key, kv.Value) |> Map.ofSeq
    let configuration = jsonText (Obj [ for kv in dependencies -> kv.Key, Arr (kv.Value |> List.map Str) ])
    let connect () =
        let connection = new SqliteConnection(sprintf "Data Source=%s;Default Timeout=5" path)
        connection.Open()
        let pragma text = (use c = connection.CreateCommand() in c.CommandText <- text; c.ExecuteNonQuery() |> ignore)
        pragma "PRAGMA foreign_keys=ON"
        pragma "PRAGMA synchronous=FULL"
        connection
    let now () = number (clock ()) "clock timestamp" false
    do
        use connection = connect ()
        let exec text = (use c = connection.CreateCommand() in c.CommandText <- text; c.ExecuteNonQuery() |> ignore)
        exec "PRAGMA journal_mode=WAL"
        exec "BEGIN IMMEDIATE"
        for statement in
            [ "CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL)"
              "CREATE TABLE IF NOT EXISTS sources(source_id TEXT PRIMARY KEY, desired_revision TEXT, observed_at REAL, last_hint INTEGER NOT NULL DEFAULT 0, observed_hint INTEGER NOT NULL DEFAULT 0, indexed_revision TEXT, indexed_known INTEGER NOT NULL DEFAULT 0)"
              "CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT UNIQUE NOT NULL, source_id TEXT NOT NULL REFERENCES sources(source_id), event_kind TEXT NOT NULL, created_at REAL NOT NULL)"
              "CREATE INDEX IF NOT EXISTS source_events ON events(source_id,created_at,seq)"
              "CREATE TABLE IF NOT EXISTS invalidations(id INTEGER PRIMARY KEY AUTOINCREMENT, source_id TEXT NOT NULL REFERENCES sources(source_id), reason TEXT NOT NULL, created_at REAL NOT NULL)"
              "CREATE TABLE IF NOT EXISTS builds(token TEXT PRIMARY KEY, plan TEXT NOT NULL, status TEXT NOT NULL, started_at REAL NOT NULL, lease_until REAL NOT NULL, finished_at REAL, error TEXT, snapshot_id TEXT)"
              "CREATE UNIQUE INDEX IF NOT EXISTS one_running_build ON builds((1)) WHERE status='running'"
              "CREATE TABLE IF NOT EXISTS generations(generation INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_id TEXT NOT NULL, revisions TEXT NOT NULL, published_at REAL NOT NULL, build_token TEXT UNIQUE NOT NULL REFERENCES builds(token))" ] do exec statement
        let columns = (use c = connection.CreateCommand() in c.CommandText <- "PRAGMA table_info(sources)"; readRows c |> List.map (fun r -> str r "name"))
        if not (List.contains "changed_at" columns) then
            exec "ALTER TABLE sources ADD COLUMN changed_at REAL"
            exec "UPDATE sources SET changed_at=observed_at WHERE changed_at IS NULL"
        if not (List.contains "first_changed_at" columns) then
            exec "ALTER TABLE sources ADD COLUMN first_changed_at REAL"
            exec "UPDATE sources SET first_changed_at=changed_at WHERE indexed_known=0 OR desired_revision IS NOT indexed_revision"
        let existing = (use c = connection.CreateCommand() in c.CommandText <- "SELECT value FROM metadata WHERE key='configuration'"; readRows c)
        match existing with
        | row :: _ when str row "value" <> configuration -> raise (StateError "Allowed sources/dependencies differ from this state's configuration; use an explicit migration")
        | _ -> ()
        (use c = connection.CreateCommand() in c.CommandText <- "INSERT OR IGNORE INTO metadata VALUES ('configuration',$v)"; c.Parameters.AddWithValue("$v", configuration) |> ignore; c.ExecuteNonQuery() |> ignore)
        exec "INSERT OR IGNORE INTO metadata VALUES ('desired_epoch','0')"
        for source in dependencies.Keys do
            use c = connection.CreateCommand()
            c.CommandText <- "INSERT OR IGNORE INTO sources(source_id) VALUES ($s)"
            c.Parameters.AddWithValue("$s", source) |> ignore
            c.ExecuteNonQuery() |> ignore
        exec "COMMIT"

    member _.Dependencies = dependencies
    member _.LeaseSeconds = leaseSeconds
    member _.SettleSeconds = settleSeconds
    member _.MaxSettleSeconds = maxSettleSeconds
    member _.Now() = now ()

    /// BEGIN IMMEDIATE also fences reads behind any uncommitted writer: an early
    /// filesystem notification cannot consume a wakeup and observe pre-commit state.
    /// Expire stale leases, run `body`, and commit on success.
    member private _.Transaction(body: SqliteConnection -> float -> 'T) : 'T =
        use connection = connect ()
        let exec text = (use c = connection.CreateCommand() in c.CommandText <- text; c.ExecuteNonQuery() |> ignore)
        exec "BEGIN IMMEDIATE"
        try
            let current = now ()
            (use c = connection.CreateCommand()
             c.CommandText <- "UPDATE builds SET status='expired',finished_at=$n WHERE status='running' AND lease_until<=$n"
             c.Parameters.AddWithValue("$n", current) |> ignore
             c.ExecuteNonQuery() |> ignore)
            let result = body connection current
            exec "COMMIT"
            result
        with _ ->
            (try exec "ROLLBACK" with _ -> ())
            reraise ()

    member private _.Source(source: string) =
        identifier source "source ID" |> ignore
        if not (dependencies.ContainsKey source) then raise (StateError "Source is outside the explicit allowlist")

    member private this.Revisions(revisions: (string * string option) list) =
        for (source, revision) in revisions do
            this.Source source
            match revision with
            | Some r when isNull r || not (revisionPattern.IsMatch r) -> raise (StateError "Revision must be a lowercase 40/64-hex digest or a deletion tombstone (None)")
            | _ -> ()

    static member private Query (connection: SqliteConnection) (text: string) (parameters: (string * obj) list) =
        use c = connection.CreateCommand()
        c.CommandText <- text
        for (name, value) in parameters do c.Parameters.AddWithValue(name, (if isNull value then box DBNull.Value else value)) |> ignore
        readRows c

    static member private Execute (connection: SqliteConnection) (text: string) (parameters: (string * obj) list) =
        use c = connection.CreateCommand()
        c.CommandText <- text
        for (name, value) in parameters do c.Parameters.AddWithValue(name, (if isNull value then box DBNull.Value else value)) |> ignore
        c.ExecuteNonQuery() |> ignore

    static member private LastRowId (connection: SqliteConnection) : int64 =
        use c = connection.CreateCommand()
        c.CommandText <- "SELECT last_insert_rowid()"
        Convert.ToInt64(c.ExecuteScalar())

    member private _.ComputePlan(connection: SqliteConnection) : Plan =
        let rows = State.Query connection "SELECT * FROM sources ORDER BY source_id" [] |> List.map (fun r -> str r "source_id", r) |> Map.ofList
        let changed =
            rows |> Map.filter (fun _ row -> (num row "observed_at").IsSome && (int64Of row "indexed_known" = 0L || str row "desired_revision" <> str row "indexed_revision")) |> Map.keys |> Set.ofSeq
        let mutable targets = changed
        let mutable expanding = true
        while expanding do
            let expanded = Set.union targets (dependencies |> Map.filter (fun _ upstream -> upstream |> List.exists targets.Contains) |> Map.keys |> Set.ofSeq)
            if expanded = targets then expanding <- false else targets <- expanded
        let generation = State.Query connection "SELECT MAX(generation) AS g FROM generations" [] |> List.head
        let baseGeneration = match generation.["g"] with null -> None | v -> Some(Convert.ToInt64 v)
        let changeTimes = [ for source in changed -> (match num rows.[source] "changed_at" with Some c -> c | None -> (num rows.[source] "observed_at").Value) ]
        let firstChangeTimes = [ for source in changed ->
                                    num rows.[source] "first_changed_at"
                                    |> Option.orElseWith (fun () -> num rows.[source] "changed_at")
                                    |> Option.defaultWith (fun () -> (num rows.[source] "observed_at").Value) ]
        let settleUntil = if changeTimes.IsEmpty then None else Some(min (List.max changeTimes + settleSeconds) (List.min firstChangeTimes + maxSettleSeconds))
        let epoch = Int64.Parse(str (State.Query connection "SELECT value FROM metadata WHERE key='desired_epoch'" [] |> List.head) "value")
        let revisionOf (row: Row) = match str row "desired_revision" with null -> None | r -> Some r
        let plan =
            { BaseGeneration = baseGeneration; DesiredEpoch = epoch
              DesiredRevisions = rows |> Map.filter (fun _ row -> (num row "observed_at").IsSome) |> Map.map (fun _ row -> revisionOf row)
              Targets = targets |> Seq.map (fun s -> s, revisionOf rows.[s]) |> Map.ofSeq
              ChangedSources = changed |> Set.toList |> List.sortWith (fun a b -> String.CompareOrdinal(a, b))
              ObservationRequired = [ for kv in rows do if (num kv.Value "observed_at").IsNone || int64Of kv.Value "observed_hint" < int64Of kv.Value "last_hint" then yield kv.Key ]
              SettleUntil = settleUntil; PlanId = "" }
        let planId = Convert.ToHexString(SHA256.HashData(Text.Encoding.UTF8.GetBytes(jsonText (planBody plan)))).ToLowerInvariant()
        { plan with PlanId = planId }

    member this.PendingPlan() : Plan = this.Transaction(fun connection _ -> this.ComputePlan connection)

    member this.EventHint(eventId: string, sourceId: string, eventKind: string) : J =
        identifier eventId "event ID" |> ignore
        this.Source sourceId
        if isNull eventKind || not (eventKinds.Contains eventKind) then raise (StateError "Unknown event kind")
        this.Transaction(fun connection now ->
            match State.Query connection "SELECT source_id,event_kind FROM events WHERE event_id=$e" [ "$e", box eventId ] with
            | existing :: _ ->
                if str existing "source_id" <> sourceId || str existing "event_kind" <> eventKind then raise (StateError "Event ID reused with different contents")
                Obj [ "accepted", Bool true; "duplicate", Bool true ]
            | [] ->
                State.Execute connection "INSERT INTO events(event_id,source_id,event_kind,created_at) VALUES ($e,$s,$k,$t)" [ "$e", box eventId; "$s", box sourceId; "$k", box eventKind; "$t", box now ]
                let seq = State.LastRowId connection
                State.Execute connection "UPDATE sources SET last_hint=$h WHERE source_id=$s" [ "$h", box seq; "$s", box sourceId ]
                Obj [ "accepted", Bool true; "duplicate", Bool false ])

    /// Apply trusted observations stamped when authoritative fetching began. None is a confirmed deletion.
    member this.ObserveHeads(heads: (string * string option) list, observedAt: float) : J =
        this.Revisions heads
        let observedAt = number observedAt "observed_at" false
        this.Transaction(fun connection now ->
            if observedAt > now + futureTolerance then raise (StateError "Observation is too far in the future")
            let changed, ignored = ResizeArray<string>(), ResizeArray<string>()
            for (source, revision) in heads |> List.sortWith (fun (a, _) (b, _) -> String.CompareOrdinal(a, b)) do
                let row = State.Query connection "SELECT * FROM sources WHERE source_id=$s" [ "$s", box source ] |> List.head
                let previous = num row "observed_at"
                let desired = match str row "desired_revision" with null -> None | r -> Some r
                if previous.IsSome && observedAt < previous.Value then ignored.Add source
                else
                    if previous = Some observedAt && desired <> revision then raise (StateError "Conflicting revisions at the same observation timestamp")
                    if previous.IsNone || desired <> revision then
                        changed.Add source
                        let indexed = match str row "indexed_revision" with null -> None | value -> Some value
                        let dirty = int64Of row "indexed_known" = 0L || indexed <> revision
                        State.Execute connection "UPDATE sources SET changed_at=$t,first_changed_at=CASE WHEN $dirty THEN COALESCE(first_changed_at,$t) ELSE NULL END WHERE source_id=$s"
                            [ "$t", box observedAt; "$dirty", box dirty; "$s", box source ]
                    let hint = State.Query connection "SELECT COALESCE(MAX(seq),0) AS h FROM events WHERE source_id=$s AND created_at<=$t" [ "$s", box source; "$t", box observedAt ] |> List.head
                    State.Execute connection "UPDATE sources SET desired_revision=$r,observed_at=$t,observed_hint=$h WHERE source_id=$s"
                        [ "$r", (match revision with Some r -> box r | None -> null); "$t", box observedAt; "$h", box (int64Of hint "h"); "$s", box source ]
            if changed.Count > 0 then State.Execute connection "UPDATE metadata SET value=CAST(value AS INTEGER)+1 WHERE key='desired_epoch'" []
            Obj [ "changed", Arr [ for s in changed -> Str s ]; "ignored_older", Arr [ for s in ignored -> Str s ] ])

    member this.ActiveGeneration() : Generation option =
        this.Transaction(fun connection _ ->
            match State.Query connection "SELECT * FROM generations ORDER BY generation DESC LIMIT 1" [] with
            | [] -> None
            | row :: _ ->
                let revisions = str row "revisions" |> JsonInput.parse |> State.ParseRevisions
                Some { Generation = int64Of row "generation"; SnapshotId = str row "snapshot_id"; Revisions = revisions; PublishedAt = (num row "published_at").Value })

    member this.InvalidateSources(sourceIds: string list, reason: string) : J =
        if sourceIds.Length < 1 || sourceIds.Length > dependencies.Count then raise (StateError "Invalidation needs a nonempty bounded source list")
        for source in sourceIds do this.Source source
        if sourceIds.Length <> (sourceIds |> List.distinct).Length then raise (StateError "Invalidation source IDs must be unique")
        if isNull reason || reason.Trim() = "" || reason.Length > 1024 then raise (StateError "Invalidation reason must be 1–1024 characters")
        this.Transaction(fun connection now ->
            for source in sourceIds do
                State.Execute connection "UPDATE sources SET indexed_known=0,changed_at=$t,first_changed_at=COALESCE(first_changed_at,$t) WHERE source_id=$s" [ "$s", box source; "$t", box now ]
                State.Execute connection "INSERT INTO invalidations(source_id,reason,created_at) VALUES ($s,$r,$t)" [ "$s", box source; "$r", box reason; "$t", box now ]
            State.Execute connection "UPDATE metadata SET value=CAST(value AS INTEGER)+1 WHERE key='desired_epoch'" []
            let plan = this.ComputePlan connection
            Obj [ "desired_epoch", Int plan.DesiredEpoch; "pending_sources", Arr [ for s in plan.Targets.Keys -> Str s ] ])

    /// Lease one exact current plan; return an opaque completion token.
    member this.StartBuild(plan: Plan) : string =
        this.Transaction(fun connection now ->
            let current = this.ComputePlan connection
            if jsonText (planJ plan) <> jsonText (planJ current) then raise (StateError "Plan is stale or modified; request pending_plan again")
            if not current.ObservationRequired.IsEmpty then raise (StateError "Authoritative observations are required before building")
            if current.Targets.IsEmpty then raise (StateError "No pending indexing targets")
            if not (State.Query connection "SELECT 1 AS x FROM builds WHERE status='running'" []).IsEmpty then raise (BuildInProgress "An indexing build already holds the lease")
            let token = Convert.ToHexString(RandomNumberGenerator.GetBytes 32).ToLowerInvariant()
            State.Execute connection "INSERT INTO builds(token,plan,status,started_at,lease_until) VALUES ($t,$p,'running',$s,$l)"
                [ "$t", box token; "$p", box (jsonText (planJ current)); "$s", box now; "$l", box (now + leaseSeconds) ]
            token)

    member private _.Build(connection: SqliteConnection, token: string) : Row =
        if isNull token || not (snapshotPattern.IsMatch token) then raise (StateError "Invalid build token")
        match State.Query connection "SELECT * FROM builds WHERE token=$t" [ "$t", box token ] with
        | [] -> raise (StateError "Unknown build token")
        | row :: _ -> row

    member this.RenewBuild(token: string) : float =
        this.Transaction(fun connection now ->
            let row = this.Build(connection, token)
            if str row "status" <> "running" then raise (StateError "Build no longer holds its lease")
            let deadline = now + leaseSeconds
            State.Execute connection "UPDATE builds SET lease_until=$l WHERE token=$t" [ "$l", box deadline; "$t", box token ]
            deadline)

    static member private ParseRevisions(value: JsonValue) : Map<string, string option> =
        JsonInput.fields value
        |> List.map (fun (name, revision) -> name, match revision with JsonValue.Null -> None | value -> Some(JsonInput.string value))
        |> Map.ofList

    /// Commit a logical generation only if its entire desired fence holds.
    member this.CompleteBuild(token: string, snapshotId: string, indexedRevisions: (string * string option) list) : J =
        if isNull snapshotId || not (snapshotPattern.IsMatch snapshotId) then raise (StateError "snapshot_id must be a lowercase 64-hex digest")
        this.Revisions indexedRevisions
        let indexed = Map.ofList indexedRevisions
        this.Transaction(fun connection now ->
            let build = this.Build(connection, token)
            let plan = str build "plan" |> JsonInput.parse
            let planTargets = JsonInput.property "targets" plan |> State.ParseRevisions
            let planBase = match JsonInput.property "base_generation" plan with JsonValue.Null -> None | value -> Some(JsonInput.int64 value)
            let planEpoch = JsonInput.property "desired_epoch" plan |> JsonInput.int64
            let planDesired = JsonInput.property "desired_revisions" plan |> State.ParseRevisions
            if indexed <> planTargets then raise (StateError "Indexed revisions must cover exactly every planned target, including tombstones")
            let status = str build "status"
            if status = "completed" then
                if str build "snapshot_id" <> snapshotId then raise (StateError "Completed build token reused for a different snapshot")
                let generation = int64Of (State.Query connection "SELECT generation FROM generations WHERE build_token=$t" [ "$t", box token ] |> List.head) "generation"
                let current = Convert.ToInt64((State.Query connection "SELECT MAX(generation) AS g FROM generations" [] |> List.head).["g"])
                Obj [ "published", Bool false; "status", Str "completed"; "generation", Int generation; "snapshot_id", Str snapshotId; "replayed", Bool true; "current", Bool (generation = current) ]
            elif status <> "running" then Obj [ "published", Bool false; "status", Str status; "retry", Bool true ]
            else
                let current = this.ComputePlan connection
                if current.BaseGeneration <> planBase || current.DesiredEpoch <> planEpoch || current.DesiredRevisions <> planDesired || not current.ObservationRequired.IsEmpty then
                    State.Execute connection "UPDATE builds SET status='superseded',finished_at=$n WHERE token=$t" [ "$n", box now; "$t", box token ]
                    Obj [ "published", Bool false; "status", Str "superseded"; "retry", Bool true ]
                else
                    match State.Query connection "SELECT revisions FROM generations WHERE snapshot_id=$s" [ "$s", box snapshotId ] with
                    | existing :: _ when State.ParseRevisions(JsonInput.parse (str existing "revisions")) <> current.DesiredRevisions -> raise (StateError "Snapshot ID already identifies different source revisions")
                    | _ -> ()
                    for kv in indexed do
                        State.Execute connection "UPDATE sources SET indexed_revision=$r,indexed_known=1,changed_at=NULL,first_changed_at=NULL WHERE source_id=$s" [ "$r", (match kv.Value with Some r -> box r | None -> null); "$s", box kv.Key ]
                    State.Execute connection "INSERT INTO generations(snapshot_id,revisions,published_at,build_token) VALUES ($i,$r,$p,$t)"
                        [ "$i", box snapshotId; "$r", box (jsonText (revisionsJ current.DesiredRevisions)); "$p", box now; "$t", box token ]
                    let generation = State.LastRowId connection
                    State.Execute connection "UPDATE builds SET status='completed',snapshot_id=$i,finished_at=$n WHERE token=$t" [ "$i", box snapshotId; "$n", box now; "$t", box token ]
                    Obj [ "published", Bool true; "status", Str "completed"; "generation", Int generation; "snapshot_id", Str snapshotId; "replayed", Bool false ])

    member this.FailBuild(token: string, error: string) : J =
        if isNull error || error.Length > 1024 then raise (StateError "Build error must be at most 1024 characters")
        this.Transaction(fun connection now ->
            let row = this.Build(connection, token)
            match str row "status" with
            | "completed" -> raise (StateError "A completed generation cannot be failed")
            | "running" ->
                State.Execute connection "UPDATE builds SET status='failed',error=$e,finished_at=$n WHERE token=$t" [ "$e", box error; "$n", box now; "$t", box token ]
                Obj [ "status", Str "failed"; "retry", Bool true ]
            | other -> Obj [ "status", Str other; "retry", Bool true ])

    /// Fail-closed latest-only check.
    member this.Freshness(snapshotId: string, maxObservationAge: float, ?requiredRevisions: (string * string option) list) : J =
        if isNull snapshotId || not (snapshotPattern.IsMatch snapshotId) then raise (StateError "snapshot_id must be a lowercase 64-hex digest")
        let maximumAge = number maxObservationAge "max_observation_age" true
        let required = defaultArg requiredRevisions []
        this.Revisions required
        this.Transaction(fun connection now ->
            let plan = this.ComputePlan connection
            let latest = State.Query connection "SELECT * FROM generations ORDER BY generation DESC LIMIT 1" [] |> List.tryHead
            let selected = State.Query connection "SELECT * FROM generations WHERE snapshot_id=$s ORDER BY generation DESC LIMIT 1" [ "$s", box snapshotId ] |> List.tryHead
            let rows = State.Query connection "SELECT * FROM sources ORDER BY source_id" []
            let stale = [ for row in rows do
                            match num row "observed_at" with
                            | None -> yield str row "source_id"
                            | Some t when now - t > maximumAge || t > now + futureTolerance -> yield str row "source_id"
                            | _ -> () ]
            let reasons = ResizeArray<string>()
            if selected.IsNone then reasons.Add "unknown_snapshot"
            if latest.IsNone || str latest.Value "snapshot_id" <> snapshotId then reasons.Add "not_latest_generation"
            if not plan.ObservationRequired.IsEmpty then reasons.Add "authoritative_observation_required"
            if not stale.IsEmpty then reasons.Add "observation_stale_or_unknown"
            if not plan.Targets.IsEmpty then reasons.Add "indexing_pending"
            let selectedRevisions = match selected with Some s -> State.ParseRevisions(JsonInput.parse (str s "revisions")) | None -> Map.empty
            if required |> List.exists (fun (source, revision) ->
                   not (selectedRevisions.ContainsKey source) || selectedRevisions.[source] <> revision || (Map.tryFind source plan.DesiredRevisions |> Option.flatten) <> revision) then
                reasons.Add "required_revision_mismatch"
            Obj [ "fresh", Bool (reasons.Count = 0); "mode", Str "latest"; "snapshot_id", Str snapshotId
                  "current_snapshot_id", (match latest with Some l -> Str (str l "snapshot_id") | None -> Null)
                  "generation", (match selected with Some s -> Int (int64Of s "generation") | None -> Null)
                  "reasons", Arr [ for r in reasons -> Str r ]; "observation_required", Arr (plan.ObservationRequired |> List.map Str)
                  "stale_sources", Arr (stale |> List.map Str); "pending_sources", Arr [ for s in plan.Targets.Keys -> Str s ]
                  "checked_at", Float now; "max_observation_age", Float maximumAge ])
