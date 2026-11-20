/// F# indexing authority persisted in SQLite: hints never establish revisions,
/// builds are leased, and publication is fenced on the complete desired set.
module Fanion.IndexState

open System
open System.Collections.Generic
open System.Diagnostics
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
let settleSecondsDefault = 3.0
let maxSettleSecondsDefault = 15.0
let transitionRetentionDefault = 512
let maxObservationAgeDefault = 90.0
let maxActiveFences = 8
let releasedFenceRetention = 256
let defaultFenceTtlSeconds = 60.0
let maxFenceTtlSeconds = 300.0
let manifestIdentityErrorMessage = "Corpus manifest identity differs from this state's configuration; use an explicit migration"

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
                changed.TrySetException(error: exn) |> ignore
                changed.Task.Exception |> ignore)
    do
        watcher.NotifyFilter <- NotifyFilters.FileName ||| NotifyFilters.LastWrite ||| NotifyFilters.Size
        watcher.Changed.Add(fun event -> if relevant event.FullPath then signal ())
        watcher.Created.Add(fun event -> if relevant event.FullPath then signal ())
        watcher.Deleted.Add(fun event -> if relevant event.FullPath then signal ())
        watcher.Renamed.Add(fun event -> if relevant event.FullPath || relevant event.OldFullPath then signal ())
        watcher.Error.Add(fun event -> fail (IOException("SQLite change notifications failed", event.GetException())))
        watcher.EnableRaisingEvents <- true

    member _.Watch() : Task<unit> = lock gate (fun () -> changed.Task)
    // Registration and publication linearize against terminal notification failure.
    member _.WhileHealthy(action: unit -> unit) =
        lock gate (fun () -> if failure.IsNone then action ())

    // The hosted process supervises this terminal notification failure.
    member _.Failure : Task<exn> = failed.Task

    interface IDisposable with
        member _.Dispose() =
            fail (ObjectDisposedException(nameof Changes))
            watcher.Dispose()

let private identifier (value: string) (label: string) =
    if isNull value || not (idPattern.IsMatch value) then raise (StateError(sprintf "Invalid %s; use 1–128 ASCII identifier characters" label))
    value

let validateIdentifier (value: string) (label: string) = ignore (identifier value label)

let private number (value: float) (label: string) (positive: bool) =
    let message = sprintf "%s must be a finite %s number" label (if positive then "positive" else "nonnegative")
    if Double.IsNaN value || Double.IsInfinity value || value < 0.0 || (positive && value = 0.0) then raise (StateError message)
    value

/// json.dumps(value, sort_keys=True, separators=(",", ":")) — ASCII payloads, so canonical bytes agree.
let private jsonText (value: J) = toText canonicalStyle value

type CrossingHold =
    { ActiveFences: string list; ExpectedSources: string list; PendingSources: string list
      HeldUntil: float; EarliestHeldUntil: float; Reasons: string list; ChangedAt: float }

type Plan =
    { BaseGeneration: int64 option; DesiredEpoch: int64; DesiredRevisions: Map<string, string option>
      Targets: Map<string, string option>; ChangedSources: string list; ObservationRequired: string list
      SettleUntil: float option; Crossing: CrossingHold option; PlanId: string }

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

// ConfigurationSha256 is the corpus configuration identity the publisher bound
// this generation to; None marks a row published before the column existed.
type Generation =
    { Generation: int64; SnapshotId: string; Revisions: Map<string, string option>; PublishedAt: float
      ConfigurationSha256: string option }

// One coherent read for readiness observers. Freshness keeps the exact shape
// returned by State.Freshness; the timing fields never enter response JSON.
type Observation =
    { Freshness: J; Active: Generation option; ChangedAt: float option; Deadline: float option; CheckedAt: float
      Crossing: CrossingHold option }

// Readiness commits since the caller's cursor, or explicit truncation when
// intermediate rows have already been pruned from the bounded journal.
type ObservationHistory =
    | Transitions of Observation list
    | Truncated

// History contains unseen commits at their commit clocks. Current is evaluated at the read clock.
type ObservationBatch =
    { Sequence: int64; History: ObservationHistory; Current: Observation; SampledAt: int64 }

type private ObservationFacts =
    { ObservedAt: (string * float option) list; Required: string list; Pending: string list
      ChangedAt: float option; Crossing: CrossingHold option }

// Readiness-affecting projection used to dedupe steady-state observation ticks;
// observation age is compared separately at the commit clock.
let private readinessKey (facts: ObservationFacts) =
    [ for source, at in facts.ObservedAt -> source, at.IsSome ], facts.Required, facts.Pending, facts.ChangedAt, facts.Crossing

let private earliest values = match values with [] -> None | values -> Some(List.min values)

let private crossingJ (crossing: CrossingHold) =
    let strings values = Arr (List.map Str values)
    Obj [ "active_fences", strings crossing.ActiveFences
          "expected_sources", strings crossing.ExpectedSources
          "pending_sources", strings crossing.PendingSources
          "held_until", Float crossing.HeldUntil
          "earliest_held_until", Float crossing.EarliestHeldUntil
          "reasons", strings crossing.Reasons
          "changed_at", Float crossing.ChangedAt ]

let private factsJ (facts: ObservationFacts) =
    let strings values = Arr (List.map Str values)
    let timestamp = function Some t -> Float t | None -> Null
    Obj ([ "observed_at", Obj [ for source, at in facts.ObservedAt -> source, timestamp at ]
           "required", strings facts.Required; "pending", strings facts.Pending; "changed_at", timestamp facts.ChangedAt ]
         @ (match facts.Crossing with Some crossing -> [ "crossing", crossingJ crossing ] | None -> []))

let private parseCrossing (value: JsonValue) : CrossingHold =
    let strings name = JsonInput.property name value |> JsonInput.items |> List.map JsonInput.string
    let heldUntil = JsonInput.property "held_until" value |> JsonInput.number
    let earliestHeldUntil =
        match Json.prop "earliest_held_until" value with
        | Some v -> JsonInput.number v
        | None -> heldUntil
    { ActiveFences = strings "active_fences"
      ExpectedSources = strings "expected_sources"
      PendingSources = strings "pending_sources"
      HeldUntil = heldUntil
      EarliestHeldUntil = earliestHeldUntil
      Reasons = strings "reasons"
      ChangedAt = JsonInput.property "changed_at" value |> JsonInput.number }

let private parseFacts text =
    let document = JsonInput.parse text
    let property name = JsonInput.property name document
    let strings name = JsonInput.items (property name) |> List.map JsonInput.string
    let timestamp = function
        | JsonValue.Null -> None
        | value -> Some(JsonInput.number value)
    let crossing =
        match Json.prop "crossing" document with
        | None | Some JsonValue.Null -> None
        | Some value -> Some(parseCrossing value)
    { ObservedAt = JsonInput.fields (property "observed_at") |> List.map (fun (source, at) -> source, timestamp at)
      Required = strings "required"; Pending = strings "pending"; ChangedAt = timestamp (property "changed_at")
      Crossing = crossing }

// The one observation-age rule shared by freshness, replay and the readiness journal.
let private staleSources now maximumAge (facts: ObservationFacts) =
    [ for source, observedAt in facts.ObservedAt do
        match observedAt with
        | None -> yield source
        | Some t when now - t > maximumAge || t > now + futureTolerance -> yield source
        | _ -> () ]

let private assess now maximumAge snapshotId (latest: Generation option) (selected: Generation option) mismatch (facts: ObservationFacts) =
    let stale = staleSources now maximumAge facts
    let reasons =
        [ if selected.IsNone then yield "unknown_snapshot"
          if latest |> Option.forall (fun g -> g.SnapshotId <> snapshotId) then yield "not_latest_generation"
          if not facts.Required.IsEmpty then yield "authoritative_observation_required"
          if not stale.IsEmpty then yield "observation_stale_or_unknown"
          if not facts.Pending.IsEmpty then yield "indexing_pending"
          if facts.Crossing.IsSome then yield "crossing_held"
          if mismatch then yield "required_revision_mismatch" ]
    let strings values = Arr (List.map Str values)
    let freshness =
        Obj [ "fresh", Bool reasons.IsEmpty; "mode", Str "latest"; "snapshot_id", Str snapshotId
              "current_snapshot_id", (match latest with Some g -> Str g.SnapshotId | None -> Null)
              "generation", (match selected with Some g -> Int g.Generation | None -> Null)
              "reasons", strings reasons; "observation_required", strings facts.Required
              "stale_sources", strings stale; "pending_sources", strings facts.Pending
              "checked_at", Float now; "max_observation_age", Float maximumAge ]
    let since =
        (facts.ChangedAt |> Option.toList)
        @ (facts.Crossing |> Option.map (fun c -> c.ChangedAt) |> Option.toList)
        @ [ for _, at in facts.ObservedAt do
                match at with Some t when now - t > maximumAge -> yield t + maximumAge | _ -> () ]
    let deadlines =
        (facts.Crossing |> Option.map (fun c -> c.EarliestHeldUntil) |> Option.toList)
        @ [ for _, at in facts.ObservedAt do
                match at with
                | Some t when t > now + futureTolerance -> yield t - futureTolerance
                | Some t when now - t <= maximumAge -> yield t + maximumAge
                | _ -> () ]
    { Freshness = freshness; Active = latest; ChangedAt = earliest since; Deadline = earliest deadlines; CheckedAt = now; Crossing = facts.Crossing }

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

// maxObservationAge is the journal's observation-age horizon: it must not exceed
// any observer's max_observation_age (both default to the serving 90 seconds).
type State(path: string, allowedSources: (string * string list) list, ?leaseSeconds: float, ?clock: unit -> float, ?settleSeconds: float, ?maxSettleSeconds: float, ?transitionRetention: int, ?maxObservationAge: float) =
    let leaseSeconds = number (defaultArg leaseSeconds 300.0) "lease_seconds" true
    let clock = defaultArg clock (fun () -> float (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()) / 1000.0)
    let settleSeconds = number (defaultArg settleSeconds settleSecondsDefault) "settle_seconds" false
    let maxSettleSeconds = number (defaultArg maxSettleSeconds maxSettleSecondsDefault) "max_settle_seconds" false
    let transitionRetention = defaultArg transitionRetention transitionRetentionDefault
    let journalAge = number (defaultArg maxObservationAge maxObservationAgeDefault) "max_observation_age" true
    do
        if maxSettleSeconds < settleSeconds then raise (StateError "settle window must be nonnegative and bounded by max_settle_seconds")
        if transitionRetention < 1 then raise (StateError "transition_retention must be a positive integer")
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
              "CREATE TABLE IF NOT EXISTS builds(token TEXT PRIMARY KEY, plan TEXT NOT NULL, status TEXT NOT NULL, started_at REAL NOT NULL, lease_until REAL NOT NULL, finished_at REAL, error TEXT, snapshot_id TEXT, invalidation_watermark INTEGER NOT NULL DEFAULT 0)"
              "CREATE UNIQUE INDEX IF NOT EXISTS one_running_build ON builds((1)) WHERE status='running'"
              "CREATE TABLE IF NOT EXISTS generations(generation INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_id TEXT NOT NULL, revisions TEXT NOT NULL, published_at REAL NOT NULL, build_token TEXT UNIQUE NOT NULL REFERENCES builds(token))"
              "CREATE TABLE IF NOT EXISTS readiness_transitions(seq INTEGER PRIMARY KEY AUTOINCREMENT, generation INTEGER NOT NULL REFERENCES generations(generation), committed_at REAL NOT NULL, facts TEXT NOT NULL)"
              "CREATE TABLE IF NOT EXISTS crossing_fences(fence_id TEXT PRIMARY KEY, expected_sources TEXT NOT NULL, arrived_sources TEXT NOT NULL, baseline_revisions TEXT NOT NULL, baseline_last_hints TEXT NOT NULL DEFAULT '{}', baseline_observed_hints TEXT NOT NULL DEFAULT '{}', reason TEXT NOT NULL, created_at REAL NOT NULL, held_until REAL NOT NULL, released_at REAL, release_reason TEXT)"
              "CREATE INDEX IF NOT EXISTS active_crossing_fences ON crossing_fences(held_until) WHERE released_at IS NULL" ] do exec statement
        let transitionsSql =
            State.Query connection "SELECT sql FROM sqlite_master WHERE type='table' AND name='readiness_transitions'" []
            |> List.tryHead |> Option.map (fun r -> str r "sql") |> Option.defaultValue ""
        if not (transitionsSql.Contains("AUTOINCREMENT", StringComparison.OrdinalIgnoreCase)) then
            exec "CREATE TABLE readiness_transitions_new(seq INTEGER PRIMARY KEY AUTOINCREMENT, generation INTEGER NOT NULL REFERENCES generations(generation), committed_at REAL NOT NULL, facts TEXT NOT NULL)"
            exec "INSERT INTO readiness_transitions_new(seq,generation,committed_at,facts) SELECT seq,generation,committed_at,facts FROM readiness_transitions ORDER BY seq"
            exec "DROP TABLE readiness_transitions"
            exec "ALTER TABLE readiness_transitions_new RENAME TO readiness_transitions"
        let columns = (use c = connection.CreateCommand() in c.CommandText <- "PRAGMA table_info(sources)"; readRows c |> List.map (fun r -> str r "name"))
        let generationColumns = (use c = connection.CreateCommand() in c.CommandText <- "PRAGMA table_info(generations)"; readRows c |> List.map (fun r -> str r "name"))
        let buildColumns = (use c = connection.CreateCommand() in c.CommandText <- "PRAGMA table_info(builds)"; readRows c |> List.map (fun r -> str r "name"))
        let fenceColumns = (use c = connection.CreateCommand() in c.CommandText <- "PRAGMA table_info(crossing_fences)"; readRows c |> List.map (fun r -> str r "name"))
        // Legacy generations keep NULL: replay cannot prove their configuration and fails closed.
        if not (List.contains "configuration_sha256" generationColumns) then
            exec "ALTER TABLE generations ADD COLUMN configuration_sha256 TEXT"
        if not (List.contains "invalidation_watermark" buildColumns) then
            exec "ALTER TABLE builds ADD COLUMN invalidation_watermark INTEGER NOT NULL DEFAULT 0"
        if not (List.contains "baseline_last_hints" fenceColumns) then
            exec "ALTER TABLE crossing_fences ADD COLUMN baseline_last_hints TEXT NOT NULL DEFAULT '{}'"
        if not (List.contains "baseline_observed_hints" fenceColumns) then
            exec "ALTER TABLE crossing_fences ADD COLUMN baseline_observed_hints TEXT NOT NULL DEFAULT '{}'"
        if not (List.contains "changed_at" columns) then
            exec "ALTER TABLE sources ADD COLUMN changed_at REAL"
            exec "UPDATE sources SET changed_at=observed_at WHERE changed_at IS NULL"
        if not (List.contains "first_changed_at" columns) then
            exec "ALTER TABLE sources ADD COLUMN first_changed_at REAL"
            exec "UPDATE sources SET first_changed_at=changed_at WHERE indexed_known=0 OR desired_revision IS NOT indexed_revision"
        if not (List.contains "settle_bypassed" columns) then
            exec "ALTER TABLE sources ADD COLUMN settle_bypassed INTEGER NOT NULL DEFAULT 0"
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
    member _.TransitionRetention = transitionRetention
    member _.MaxObservationAge = journalAge
    member _.Now() = now ()

    /// BEGIN IMMEDIATE also fences reads behind any uncommitted writer: an early
    /// filesystem notification cannot consume a wakeup and observe pre-commit state.
    /// Expire stale leases, run `body`, and commit on success.
    member private this.TransactionWithExpiry(body: SqliteConnection -> float -> int64 -> int -> 'T) : 'T =
        use connection = connect ()
        let exec text = (use c = connection.CreateCommand() in c.CommandText <- text; c.ExecuteNonQuery() |> ignore)
        exec "BEGIN IMMEDIATE"
        try
            let sampledAt = Stopwatch.GetTimestamp()
            let current = now ()
            (use c = connection.CreateCommand()
             c.CommandText <- "UPDATE builds SET status='expired',finished_at=$n WHERE status='running' AND lease_until<=$n"
             c.Parameters.AddWithValue("$n", current) |> ignore
             c.ExecuteNonQuery() |> ignore)
            let expiredFences =
                use c = connection.CreateCommand()
                c.CommandText <- "UPDATE crossing_fences SET released_at=$n,release_reason='ttl_expired' WHERE released_at IS NULL AND held_until<=$n"
                c.Parameters.AddWithValue("$n", current) |> ignore
                c.ExecuteNonQuery()
            if expiredFences > 0 then
                State.PruneReleasedFences connection
                this.RecordReadiness(connection, current)
            let result = body connection current sampledAt expiredFences
            exec "COMMIT"
            result
        with _ ->
            (try exec "ROLLBACK" with _ -> ())
            reraise ()

    member private this.Transaction(body: SqliteConnection -> float -> int64 -> 'T) : 'T =
        this.TransactionWithExpiry(fun connection current sampledAt _ -> body connection current sampledAt)

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

    static member private ParseStringList(text: string) : string list =
        JsonInput.parse text |> JsonInput.items |> List.map JsonInput.string

    static member private StringListJson(values: string list) : string =
        jsonText (Arr (List.map Str values))

    static member private ParseHintMap(text: string) : Map<string, int64> =
        JsonInput.parse text
        |> JsonInput.fields
        |> List.map (fun (name, value) -> name, JsonInput.int64 value)
        |> Map.ofList

    static member private HintMapJson(values: Map<string, int64>) : string =
        jsonText (Obj [ for kv in values -> kv.Key, Int kv.Value ])

    static member private PruneReleasedFences(connection: SqliteConnection) =
        State.Execute connection
            "DELETE FROM crossing_fences WHERE released_at IS NOT NULL AND fence_id NOT IN (SELECT fence_id FROM crossing_fences WHERE released_at IS NOT NULL ORDER BY released_at DESC, created_at DESC, fence_id DESC LIMIT $limit)"
            [ "$limit", box releasedFenceRetention ]

    static member private ClearSettleForSources(connection: SqliteConnection, sourceIds: string seq) =
        for source in sourceIds |> Set.ofSeq do
            State.Execute connection
                "UPDATE sources SET settle_bypassed=1 WHERE source_id=$s AND observed_at IS NOT NULL AND (indexed_known=0 OR desired_revision IS NOT indexed_revision)"
                [ "$s", box source ]

    static member private CheckManifestIdentity(connection: SqliteConnection, identity: string, throwOnMismatch: bool) : bool =
        if isNull identity || not (snapshotPattern.IsMatch identity) then
            raise (StateError "manifest_identity must be a lowercase 64-hex digest")
        match State.Query connection "SELECT value FROM metadata WHERE key='manifest_identity'" [] with
        | row :: _ ->
            if str row "value" = identity then true
            elif throwOnMismatch then raise (StateError manifestIdentityErrorMessage)
            else false
        | [] ->
            let conflictingGeneration =
                State.Query connection "SELECT configuration_sha256 FROM generations ORDER BY generation DESC LIMIT 1" []
                |> List.tryHead
                |> Option.bind (fun r -> match str r "configuration_sha256" with null -> None | v -> Some v)
                |> Option.exists (fun recorded -> recorded <> identity)
            if conflictingGeneration then
                if throwOnMismatch then raise (StateError manifestIdentityErrorMessage)
                else false
            else
                true

    member private _.ActiveCrossing(connection: SqliteConnection, obsRequired: Set<string>) : CrossingHold option =
        let rows = State.Query connection "SELECT * FROM crossing_fences WHERE released_at IS NULL ORDER BY created_at, fence_id" []
        if rows.IsEmpty then None
        else
            let sortUnique (values: string seq) = values |> Set.ofSeq |> Set.toList |> List.sortWith (fun a b -> String.CompareOrdinal(a, b))
            let expected = ResizeArray<string>()
            let pending = ResizeArray<string>()
            let reasons = ResizeArray<string>()
            let fences = ResizeArray<string>()
            let mutable heldUntil = Double.NegativeInfinity
            let mutable earliestHeldUntil = Double.PositiveInfinity
            let mutable changedAt = Double.PositiveInfinity
            for row in rows do
                fences.Add(str row "fence_id")
                let exp = State.ParseStringList(str row "expected_sources")
                let arr = State.ParseStringList(str row "arrived_sources") |> Set.ofList
                expected.AddRange exp
                for s in exp do
                    if not (arr.Contains s) || obsRequired.Contains s then
                        pending.Add s
                let reason = str row "reason"
                if not (reasons.Contains reason) then reasons.Add reason
                let rowHeldUntil = (num row "held_until").Value
                heldUntil <- max heldUntil rowHeldUntil
                earliestHeldUntil <- min earliestHeldUntil rowHeldUntil
                changedAt <- min changedAt (num row "created_at").Value
            Some
                { ActiveFences = fences |> Seq.sortWith (fun a b -> String.CompareOrdinal(a, b)) |> Seq.toList
                  ExpectedSources = sortUnique expected
                  PendingSources = sortUnique pending
                  HeldUntil = heldUntil
                  EarliestHeldUntil = earliestHeldUntil
                  Reasons = List.ofSeq reasons
                  ChangedAt = changedAt }

    member private this.ComputePlan(connection: SqliteConnection) : Plan =
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
        let unsettled = changed |> Set.filter (fun source -> int64Of rows.[source] "settle_bypassed" = 0L)
        let changeTimes = [ for source in unsettled -> (match num rows.[source] "changed_at" with Some c -> c | None -> (num rows.[source] "observed_at").Value) ]
        let firstChangeTimes = [ for source in unsettled ->
                                    num rows.[source] "first_changed_at"
                                    |> Option.orElseWith (fun () -> num rows.[source] "changed_at")
                                    |> Option.defaultWith (fun () -> (num rows.[source] "observed_at").Value) ]
        let settleUntil = if changeTimes.IsEmpty then None else Some(min (List.max changeTimes + settleSeconds) (List.min firstChangeTimes + maxSettleSeconds))
        let epoch = Int64.Parse(str (State.Query connection "SELECT value FROM metadata WHERE key='desired_epoch'" [] |> List.head) "value")
        let revisionOf (row: Row) = match str row "desired_revision" with null -> None | r -> Some r
        let observationRequired = [ for kv in rows do if (num kv.Value "observed_at").IsNone || int64Of kv.Value "observed_hint" < int64Of kv.Value "last_hint" then yield kv.Key ]
        let crossing = this.ActiveCrossing(connection, Set.ofList observationRequired)
        let plan =
            { BaseGeneration = baseGeneration; DesiredEpoch = epoch
              DesiredRevisions = rows |> Map.filter (fun _ row -> (num row "observed_at").IsSome) |> Map.map (fun _ row -> revisionOf row)
              Targets = targets |> Seq.map (fun s -> s, revisionOf rows.[s]) |> Map.ofSeq
              ChangedSources = changed |> Set.toList |> List.sortWith (fun a b -> String.CompareOrdinal(a, b))
              ObservationRequired = observationRequired
              SettleUntil = settleUntil; Crossing = crossing; PlanId = "" }
        let planId = Convert.ToHexString(SHA256.HashData(Text.Encoding.UTF8.GetBytes(jsonText (planBody plan)))).ToLowerInvariant()
        { plan with PlanId = planId }

    member private _.ObservationFacts(connection: SqliteConnection, plan: Plan) =
        let rows = State.Query connection "SELECT * FROM sources ORDER BY source_id" []
        let hinted =
            State.Query connection "SELECT MIN(e.created_at) AS t FROM events e JOIN sources s ON s.source_id=e.source_id WHERE e.seq>s.observed_hint" []
            |> List.collect (fun row -> num row "t" |> Option.toList)
        let changed = set plan.ChangedSources
        let since =
            hinted
            @ [ for row in rows do
                    let source = str row "source_id"
                    if changed.Contains source then
                        match num row "observed_at" with
                        | Some t -> yield num row "first_changed_at" |> Option.orElseWith (fun () -> num row "changed_at") |> Option.defaultValue t
                        | None -> () ]
        { ObservedAt = [ for row in rows -> str row "source_id", num row "observed_at" ]
          Required = plan.ObservationRequired; Pending = List.ofSeq plan.Targets.Keys; ChangedAt = earliest since; Crossing = plan.Crossing }

    // Only readiness-changing writes call this, before their existing COMMIT.
    // The journal references generation rows and keeps only a bounded suffix.
    // A tick refreshes the latest row in place (facts and committed_at together,
    // so replay at committed_at still sees the same readiness) only when the
    // readiness-affecting fields match and observation age has not moved the
    // row's stale set since it was committed. Otherwise an expiry that this
    // write heals would leave its restored state out of replay.
    member private this.RecordReadiness(connection: SqliteConnection, now: float) =
        let plan = this.ComputePlan connection
        match plan.BaseGeneration with
        | None -> ()
        | Some generation ->
            let currentFacts = this.ObservationFacts(connection, plan)
            let facts = currentFacts |> factsJ |> jsonText
            let previous =
                State.Query connection "SELECT seq,generation,committed_at,facts FROM readiness_transitions ORDER BY seq DESC LIMIT 1" []
                |> List.tryHead
            let refreshable =
                match previous with
                | Some row when int64Of row "generation" = generation ->
                    let stored = parseFacts (str row "facts")
                    let committedAt = (num row "committed_at").Value
                    let staleNow = staleSources now journalAge stored
                    if readinessKey stored = readinessKey currentFacts
                       && staleSources committedAt journalAge stored = staleNow
                       && staleSources now journalAge currentFacts = staleNow then Some(int64Of row "seq")
                    else None
                | _ -> None
            match refreshable with
            | Some seq ->
                State.Execute connection
                    "UPDATE readiness_transitions SET committed_at=$t,facts=$f WHERE seq=$seq"
                    [ "$t", box now; "$f", box facts; "$seq", box seq ]
            | None ->
                State.Execute connection
                    "INSERT INTO readiness_transitions(generation,committed_at,facts) VALUES ($g,$t,$f)"
                    [ "$g", box generation; "$t", box now; "$f", box facts ]
                State.Execute connection
                    "DELETE FROM readiness_transitions WHERE seq < (SELECT MIN(seq) FROM (SELECT seq FROM readiness_transitions ORDER BY seq DESC LIMIT $limit))"
                    [ "$limit", box transitionRetention ]

    member this.PendingPlan() : Plan = this.Transaction(fun connection _ _ -> this.ComputePlan connection)

    member this.ManifestIdentity() : string option =
        this.Transaction(fun connection _ _ ->
            State.Query connection "SELECT value FROM metadata WHERE key='manifest_identity'" []
            |> List.tryHead
            |> Option.map (fun row -> str row "value"))

    member this.RebindManifestIdentity(identity: string, sourceIds: string list, reason: string) : bool =
        if isNull identity || not (snapshotPattern.IsMatch identity) then
            raise (StateError "manifest_identity must be a lowercase 64-hex digest")
        if sourceIds.Length < 1 || sourceIds.Length > dependencies.Count then
            raise (StateError "Invalidation needs a nonempty bounded source list")
        for source in sourceIds do this.Source source
        if sourceIds.Length <> (sourceIds |> List.distinct).Length then
            raise (StateError "Invalidation source IDs must be unique")
        if isNull reason || reason.Trim() = "" || reason.Length > 1024 then
            raise (StateError "Invalidation reason must be 1–1024 characters")
        this.Transaction(fun connection now _ ->
            let invalidateAll () =
                for source in sourceIds do
                    State.Execute connection
                        "UPDATE sources SET indexed_known=0,changed_at=$t,first_changed_at=COALESCE(first_changed_at,$t),settle_bypassed=0 WHERE source_id=$s"
                        [ "$s", box source; "$t", box now ]
                    State.Execute connection
                        "INSERT INTO invalidations(source_id,reason,created_at) VALUES ($s,$r,$t)"
                        [ "$s", box source; "$r", box reason; "$t", box now ]
                State.Execute connection "UPDATE metadata SET value=CAST(value AS INTEGER)+1 WHERE key='desired_epoch'" []
                this.RecordReadiness(connection, now)
            match State.Query connection "SELECT value FROM metadata WHERE key='manifest_identity'" [] with
            | row :: _ when str row "value" = identity -> false
            | row :: _ ->
                State.Execute connection "UPDATE metadata SET value=$id WHERE key='manifest_identity'" [ "$id", box identity ]
                invalidateAll ()
                true
            | [] ->
                let conflictingGeneration =
                    State.Query connection "SELECT configuration_sha256 FROM generations ORDER BY generation DESC LIMIT 1" []
                    |> List.tryHead
                    |> Option.bind (fun r -> match str r "configuration_sha256" with null -> None | v -> Some v)
                    |> Option.exists (fun recorded -> recorded <> identity)
                State.Execute connection "INSERT INTO metadata VALUES ('manifest_identity',$id)" [ "$id", box identity ]
                if conflictingGeneration then
                    invalidateAll ()
                    true
                else
                    false)

    member this.VerifyManifestIdentity(identity: string) : bool =
        this.Transaction(fun connection _ _ ->
            State.CheckManifestIdentity(connection, identity, false))

    member this.NextCrossingDelay() : TimeSpan option =
        this.TransactionWithExpiry(fun connection now _ expired ->
            match State.Query connection "SELECT MIN(held_until) AS h FROM crossing_fences WHERE released_at IS NULL" [] with
            | row :: _ ->
                match num row "h" with
                | Some heldUntil -> Some(TimeSpan.FromMilliseconds(Math.Min(Math.Ceiling(max (heldUntil - now) 0.0 * 1000.0) + 1.0, 86_400_000.0)))
                | None -> if expired > 0 then Some TimeSpan.Zero else None
            | [] -> if expired > 0 then Some TimeSpan.Zero else None)

    member this.RunningBuildTokens() : Set<string> =
        this.Transaction(fun connection _ _ ->
            State.Query connection "SELECT token FROM builds WHERE status='running'" []
            |> List.map (fun row -> str row "token")
            |> Set.ofList)

    member this.ReclaimOrphanedBuilds(reason: string) : int =
        if isNull reason || reason.Trim() = "" || reason.Length > 1024 then
            raise (StateError "Reclaim reason must be 1–1024 characters")
        this.Transaction(fun connection now _ ->
            use c = connection.CreateCommand()
            c.CommandText <- "UPDATE builds SET status='abandoned',error=$e,finished_at=$n WHERE status='running'"
            c.Parameters.AddWithValue("$e", box reason) |> ignore
            c.Parameters.AddWithValue("$n", box now) |> ignore
            c.ExecuteNonQuery())

    member this.HoldCrossing(fenceId: string, expectedSources: string list, ttlSeconds: float, reason: string, ?expectedManifestIdentity: string) : J =
        identifier fenceId "fence ID" |> ignore
        if expectedSources.IsEmpty then raise (StateError "Crossing fence requires at least one expected source")
        for source in expectedSources do this.Source source
        if expectedSources.Length <> (expectedSources |> List.distinct).Length then
            raise (StateError "Crossing fence expected_sources must not contain duplicates")
        let sortedExpected = expectedSources |> List.sortWith (fun a b -> String.CompareOrdinal(a, b))
        let ttlSeconds = number ttlSeconds "ttl_seconds" true
        if ttlSeconds > maxFenceTtlSeconds then
            raise (StateError (sprintf "ttl_seconds must be at most %.0f" maxFenceTtlSeconds))
        if isNull reason || reason.Trim() = "" || reason.Length > 512 then
            raise (StateError "Crossing fence reason must be 1–512 characters")
        let receipt (expected: string list) (arrived: string list) (heldUntil: float) (createdAt: float) (fenceReason: string) (duplicate: bool) =
            let arrivedSet = Set.ofList arrived
            let pending = expected |> List.filter (fun s -> not (arrivedSet.Contains s))
            Obj [ "fence_id", Str fenceId
                  "status", Str "held"
                  "held_until", Float heldUntil
                  "created_at", Float createdAt
                  "expected_sources", Arr (List.map Str expected)
                  "arrived_sources", Arr (List.map Str arrived)
                  "pending_sources", Arr (List.map Str pending)
                  "reason", Str fenceReason
                  "duplicate", Bool duplicate ]
        this.Transaction(fun connection now _ ->
            expectedManifestIdentity |> Option.iter (fun id -> State.CheckManifestIdentity(connection, id, true) |> ignore)
            match State.Query connection "SELECT * FROM crossing_fences WHERE fence_id=$id" [ "$id", box fenceId ] with
            | existing :: _ ->
                if not (isNull existing.["released_at"]) then
                    raise (StateError "Fence ID has already been released")
                let existingExpected = State.ParseStringList(str existing "expected_sources")
                let existingReason = str existing "reason"
                if existingExpected <> sortedExpected || existingReason <> reason then
                    raise (StateError "Fence ID reused with different contents")
                let existingArrived = State.ParseStringList(str existing "arrived_sources")
                receipt existingExpected existingArrived (num existing "held_until").Value (num existing "created_at").Value existingReason true
            | [] ->
                let activeCount = State.Query connection "SELECT COUNT(*) AS c FROM crossing_fences WHERE released_at IS NULL" [] |> List.head |> fun r -> int64Of r "c"
                if activeCount >= int64 maxActiveFences then
                    raise (StateError "Maximum active crossing fences reached")
                let baselines =
                    [ for source in sortedExpected ->
                        let row = State.Query connection "SELECT desired_revision,last_hint,observed_hint FROM sources WHERE source_id=$s" [ "$s", box source ] |> List.head
                        source, (match str row "desired_revision" with null -> None | r -> Some r), int64Of row "last_hint", int64Of row "observed_hint" ]
                let baselineRevisions = baselines |> List.map (fun (s, r, _, _) -> s, r) |> Map.ofList
                let baselineLastHints = baselines |> List.map (fun (s, _, l, _) -> s, l) |> Map.ofList
                let baselineObservedHints = baselines |> List.map (fun (s, _, _, o) -> s, o) |> Map.ofList
                let heldUntil = now + ttlSeconds
                State.Execute connection
                    "INSERT INTO crossing_fences(fence_id,expected_sources,arrived_sources,baseline_revisions,baseline_last_hints,baseline_observed_hints,reason,created_at,held_until) VALUES ($id,$exp,'[]',$base,$baseLast,$baseObs,$reason,$created,$until)"
                    [ "$id", box fenceId
                      "$exp", box (State.StringListJson sortedExpected)
                      "$base", box (jsonText (revisionsJ baselineRevisions))
                      "$baseLast", box (State.HintMapJson baselineLastHints)
                      "$baseObs", box (State.HintMapJson baselineObservedHints)
                      "$reason", box reason
                      "$created", box now
                      "$until", box heldUntil ]
                this.RecordReadiness(connection, now)
                receipt sortedExpected [] heldUntil now reason false)

    member this.ReleaseCrossing(fenceId: string, ?reason: string, ?expectedManifestIdentity: string) : J =
        identifier fenceId "fence ID" |> ignore
        let releaseReason = defaultArg reason "manual_release"
        if isNull releaseReason || releaseReason.Trim() = "" || releaseReason.Length > 512 then
            raise (StateError "Release reason must be 1–512 characters")
        this.Transaction(fun connection now _ ->
            expectedManifestIdentity |> Option.iter (fun id -> State.CheckManifestIdentity(connection, id, true) |> ignore)
            match State.Query connection "SELECT * FROM crossing_fences WHERE fence_id=$id" [ "$id", box fenceId ] with
            | [] -> raise (StateError "Unknown crossing fence")
            | row :: _ ->
                let expected = State.ParseStringList(str row "expected_sources")
                let arrived = State.ParseStringList(str row "arrived_sources")
                match num row "released_at" with
                | Some releasedAt ->
                    Obj [ "fence_id", Str fenceId
                          "status", Str "released"
                          "released_at", Float releasedAt
                          "release_reason", Str (str row "release_reason")
                          "expected_sources", Arr (List.map Str expected)
                          "arrived_sources", Arr (List.map Str arrived)
                          "already_released", Bool true ]
                | None ->
                    State.Execute connection
                        "UPDATE crossing_fences SET released_at=$n,release_reason=$r WHERE fence_id=$id"
                        [ "$n", box now; "$r", box releaseReason; "$id", box fenceId ]
                    let arrivedSet = Set.ofList arrived
                    for sourceId in expected do
                        if not (arrivedSet.Contains sourceId) then
                            let hintId = sprintf "crossing-release:%s:%s:%s" fenceId sourceId (Guid.NewGuid().ToString("N"))
                            State.Execute connection
                                "INSERT INTO events(event_id,source_id,event_kind,created_at) VALUES ($e,$s,'reconcile',$t)"
                                [ "$e", box hintId; "$s", box sourceId; "$t", box now ]
                            let seq = State.LastRowId connection
                            State.Execute connection
                                "UPDATE sources SET last_hint=$h WHERE source_id=$s"
                                [ "$h", box seq; "$s", box sourceId ]
                    State.ClearSettleForSources(connection, expected)
                    State.PruneReleasedFences connection
                    this.RecordReadiness(connection, now)
                    Obj [ "fence_id", Str fenceId
                          "status", Str "released"
                          "released_at", Float now
                          "release_reason", Str releaseReason
                          "expected_sources", Arr (List.map Str expected)
                          "arrived_sources", Arr (List.map Str arrived)
                          "already_released", Bool false ])

    member this.EventHint(eventId: string, sourceId: string, eventKind: string, ?expectedManifestIdentity: string) : J =
        identifier eventId "event ID" |> ignore
        this.Source sourceId
        if isNull eventKind || not (eventKinds.Contains eventKind) then raise (StateError "Unknown event kind")
        this.Transaction(fun connection now _ ->
            expectedManifestIdentity |> Option.iter (fun id -> State.CheckManifestIdentity(connection, id, true) |> ignore)
            match State.Query connection "SELECT source_id,event_kind FROM events WHERE event_id=$e" [ "$e", box eventId ] with
            | existing :: _ ->
                if str existing "source_id" <> sourceId || str existing "event_kind" <> eventKind then raise (StateError "Event ID reused with different contents")
                Obj [ "accepted", Bool true; "duplicate", Bool true ]
            | [] ->
                State.Execute connection "INSERT INTO events(event_id,source_id,event_kind,created_at) VALUES ($e,$s,$k,$t)" [ "$e", box eventId; "$s", box sourceId; "$k", box eventKind; "$t", box now ]
                let seq = State.LastRowId connection
                State.Execute connection "UPDATE sources SET last_hint=$h WHERE source_id=$s" [ "$h", box seq; "$s", box sourceId ]
                this.RecordReadiness(connection, now)
                Obj [ "accepted", Bool true; "duplicate", Bool false ])

    /// Apply trusted observations stamped when authoritative fetching began. None is a confirmed deletion.
    member this.ObserveHeads(heads: (string * string option) list, observedAt: float) : J =
        this.Revisions heads
        let observedAt = number observedAt "observed_at" false
        this.Transaction(fun connection now _ ->
            if observedAt > now + futureTolerance then raise (StateError "Observation is too far in the future")
            let changed, ignored = ResizeArray<string>(), ResizeArray<string>()
            let mutable updated = false
            let mutable fenceUpdated = false
            let mutable fenceReleased = false
            let releasedSources = ResizeArray<string>()
            let activeFences =
                State.Query connection "SELECT * FROM crossing_fences WHERE released_at IS NULL ORDER BY created_at, fence_id" []
                |> List.map (fun row ->
                    {| FenceId = str row "fence_id"
                       Expected = State.ParseStringList(str row "expected_sources")
                       Arrived = HashSet<string>(State.ParseStringList(str row "arrived_sources"))
                       Baseline = JsonInput.parse (str row "baseline_revisions") |> State.ParseRevisions
                       BaselineLastHints = State.ParseHintMap(str row "baseline_last_hints")
                       BaselineObservedHints = State.ParseHintMap(str row "baseline_observed_hints")
                       CreatedAt = (num row "created_at").Value
                       Dirty = ref false |})
            for (source, revision) in heads |> List.sortWith (fun (a, _) (b, _) -> String.CompareOrdinal(a, b)) do
                let row = State.Query connection "SELECT * FROM sources WHERE source_id=$s" [ "$s", box source ] |> List.head
                let previous = num row "observed_at"
                let desired = match str row "desired_revision" with null -> None | r -> Some r
                if previous.IsSome && observedAt < previous.Value then ignored.Add source
                else
                    if previous = Some observedAt && desired <> revision then raise (StateError "Conflicting revisions at the same observation timestamp")
                    let prevHint = int64Of row "observed_hint"
                    let hint = State.Query connection "SELECT COALESCE(MAX(seq),0) AS h FROM events WHERE source_id=$s AND created_at<=$t" [ "$s", box source; "$t", box observedAt ] |> List.head
                    let newHint = int64Of hint "h"
                    let releaseBypassSeq =
                        if newHint > prevHint then
                            State.Query connection
                                "SELECT COALESCE(MAX(seq),0) AS s FROM events WHERE source_id=$s AND event_kind='reconcile' AND event_id LIKE 'crossing-release:%' AND seq>$prev AND seq<=$obs"
                                [ "$s", box source; "$prev", box prevHint; "$obs", box newHint ]
                            |> List.head
                            |> fun r -> int64Of r "s"
                        else 0L
                    let keepReleaseBypass =
                        releaseBypassSeq > 0L
                        && (State.Query connection
                                "SELECT 1 AS x FROM events WHERE source_id=$s AND event_kind='push' AND seq>$rel AND seq<=$obs LIMIT 1"
                                [ "$s", box source; "$rel", box releaseBypassSeq; "$obs", box newHint ]).IsEmpty
                    if previous.IsNone || desired <> revision then
                        changed.Add source
                        let indexed = match str row "indexed_revision" with null -> None | value -> Some value
                        let dirty = int64Of row "indexed_known" = 0L || indexed <> revision
                        let bypassed = if dirty && keepReleaseBypass then 1 else 0
                        State.Execute connection "UPDATE sources SET changed_at=$t,first_changed_at=CASE WHEN $dirty THEN COALESCE(first_changed_at,$t) ELSE NULL END,settle_bypassed=$b WHERE source_id=$s"
                            [ "$t", box observedAt; "$dirty", box dirty; "$b", box bypassed; "$s", box source ]
                    updated <- updated || previous <> Some observedAt || newHint <> prevHint
                    State.Execute connection "UPDATE sources SET desired_revision=$r,observed_at=$t,observed_hint=$h WHERE source_id=$s"
                        [ "$r", (match revision with Some r -> box r | None -> null); "$t", box observedAt; "$h", box newHint; "$s", box source ]
                    for fence in activeFences do
                        if observedAt >= fence.CreatedAt && List.contains source fence.Expected && not (fence.Arrived.Contains source) then
                            let baseLastHint = Map.tryFind source fence.BaselineLastHints |> Option.defaultValue 0L
                            let baseObsHint = Map.tryFind source fence.BaselineObservedHints |> Option.defaultValue baseLastHint
                            let observedPostHoldPush =
                                not (State.Query connection
                                         "SELECT 1 AS x FROM events WHERE source_id=$s AND event_kind='push' AND created_at>=$c AND seq>$base AND seq<=$obs LIMIT 1"
                                         [ "$s", box source; "$c", box fence.CreatedAt; "$base", box baseLastHint; "$obs", box newHint ]).IsEmpty
                            let changedFromBaselineWithoutPreHoldPending =
                                revision <> Map.find source fence.Baseline && baseObsHint >= baseLastHint
                            if observedPostHoldPush || changedFromBaselineWithoutPreHoldPending then
                                fence.Arrived.Add source |> ignore
                                fence.Dirty.Value <- true
                                fenceUpdated <- true
            if not activeFences.IsEmpty then
                let sourceHints =
                    State.Query connection "SELECT source_id,observed_hint,last_hint FROM sources" []
                    |> List.map (fun r -> str r "source_id", (int64Of r "observed_hint", int64Of r "last_hint"))
                    |> Map.ofList
                for fence in activeFences do
                    let allExpectedArrived =
                        fence.Expected |> List.forall (fun s ->
                            let obsHint, lastHint = sourceHints.[s]
                            fence.Arrived.Contains s && obsHint >= lastHint)
                    if allExpectedArrived then
                        let sortedArrived = fence.Arrived |> Seq.sortWith (fun a b -> String.CompareOrdinal(a, b)) |> Seq.toList
                        State.Execute connection
                            "UPDATE crossing_fences SET arrived_sources=$arr,released_at=$n,release_reason='all_arrived' WHERE fence_id=$id"
                            [ "$arr", box (State.StringListJson sortedArrived); "$n", box now; "$id", box fence.FenceId ]
                        releasedSources.AddRange fence.Expected
                        fenceReleased <- true
                    elif fence.Dirty.Value then
                        let sortedArrived = fence.Arrived |> Seq.sortWith (fun a b -> String.CompareOrdinal(a, b)) |> Seq.toList
                        State.Execute connection
                            "UPDATE crossing_fences SET arrived_sources=$arr WHERE fence_id=$id"
                            [ "$arr", box (State.StringListJson sortedArrived); "$id", box fence.FenceId ]
            if fenceReleased then
                State.ClearSettleForSources(connection, releasedSources)
                State.PruneReleasedFences connection
            if changed.Count > 0 then State.Execute connection "UPDATE metadata SET value=CAST(value AS INTEGER)+1 WHERE key='desired_epoch'" []
            if updated || fenceUpdated || fenceReleased then this.RecordReadiness(connection, now)
            Obj [ "changed", Arr [ for s in changed -> Str s ]; "ignored_older", Arr [ for s in ignored -> Str s ] ])

    member this.ActiveGeneration() : Generation option =
        this.Transaction(fun connection _ _ -> this.Latest connection)

    member this.InvalidateSources(sourceIds: string list, reason: string) : J =
        if sourceIds.Length < 1 || sourceIds.Length > dependencies.Count then raise (StateError "Invalidation needs a nonempty bounded source list")
        for source in sourceIds do this.Source source
        if sourceIds.Length <> (sourceIds |> List.distinct).Length then raise (StateError "Invalidation source IDs must be unique")
        if isNull reason || reason.Trim() = "" || reason.Length > 1024 then raise (StateError "Invalidation reason must be 1–1024 characters")
        this.Transaction(fun connection now _ ->
            for source in sourceIds do
                State.Execute connection "UPDATE sources SET indexed_known=0,changed_at=$t,first_changed_at=COALESCE(first_changed_at,$t),settle_bypassed=0 WHERE source_id=$s" [ "$s", box source; "$t", box now ]
                State.Execute connection "INSERT INTO invalidations(source_id,reason,created_at) VALUES ($s,$r,$t)" [ "$s", box source; "$r", box reason; "$t", box now ]
            State.Execute connection "UPDATE metadata SET value=CAST(value AS INTEGER)+1 WHERE key='desired_epoch'" []
            let plan = this.ComputePlan connection
            this.RecordReadiness(connection, now)
            Obj [ "desired_epoch", Int plan.DesiredEpoch; "pending_sources", Arr [ for s in plan.Targets.Keys -> Str s ] ])

    /// Lease one exact current plan; return an opaque completion token.
    member this.StartBuild(plan: Plan) : string =
        this.Transaction(fun connection now _ ->
            let current = this.ComputePlan connection
            if jsonText (planJ plan) <> jsonText (planJ current) then raise (StateError "Plan is stale or modified; request pending_plan again")
            if current.Crossing.IsSome then raise (StateError "Crossing fence is held; build cannot start until crossing releases")
            if not current.ObservationRequired.IsEmpty then raise (StateError "Authoritative observations are required before building")
            if current.Targets.IsEmpty then raise (StateError "No pending indexing targets")
            if not (State.Query connection "SELECT 1 AS x FROM builds WHERE status='running'" []).IsEmpty then raise (BuildInProgress "An indexing build already holds the lease")
            let invWatermark =
                State.Query connection "SELECT COALESCE(MAX(id),0) AS m FROM invalidations" []
                |> List.head
                |> fun r -> int64Of r "m"
            let token = Convert.ToHexString(RandomNumberGenerator.GetBytes 32).ToLowerInvariant()
            State.Execute connection "INSERT INTO builds(token,plan,status,started_at,lease_until,invalidation_watermark) VALUES ($t,$p,'running',$s,$l,$w)"
                [ "$t", box token; "$p", box (jsonText (planJ current)); "$s", box now; "$l", box (now + leaseSeconds); "$w", box invWatermark ]
            token)

    member private _.Build(connection: SqliteConnection, token: string) : Row =
        if isNull token || not (snapshotPattern.IsMatch token) then raise (StateError "Invalid build token")
        match State.Query connection "SELECT * FROM builds WHERE token=$t" [ "$t", box token ] with
        | [] -> raise (StateError "Unknown build token")
        | row :: _ -> row

    member this.RenewBuild(token: string) : float =
        this.Transaction(fun connection now _ ->
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
    member this.CompleteBuild(token: string, snapshotId: string, indexedRevisions: (string * string option) list, ?configurationSha256: string) : J =
        if isNull snapshotId || not (snapshotPattern.IsMatch snapshotId) then raise (StateError "snapshot_id must be a lowercase 64-hex digest")
        match configurationSha256 with
        | Some identity when isNull identity || not (snapshotPattern.IsMatch identity) -> raise (StateError "configuration_sha256 must be a lowercase 64-hex digest")
        | _ -> ()
        this.Revisions indexedRevisions
        let indexed = Map.ofList indexedRevisions
        this.Transaction(fun connection now _ ->
            let build = this.Build(connection, token)
            let plan = str build "plan" |> JsonInput.parse
            let planTargets = JsonInput.property "targets" plan |> State.ParseRevisions
            let planBase = match JsonInput.property "base_generation" plan with JsonValue.Null -> None | value -> Some(JsonInput.int64 value)
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
                if current.BaseGeneration <> planBase then
                    State.Execute connection "UPDATE builds SET status='superseded',finished_at=$n WHERE token=$t" [ "$n", box now; "$t", box token ]
                    Obj [ "published", Bool false; "status", Str "superseded"; "retry", Bool true ]
                else
                    match State.Query connection "SELECT revisions FROM generations WHERE snapshot_id=$s" [ "$s", box snapshotId ] with
                    | existing :: _ when State.ParseRevisions(JsonInput.parse (str existing "revisions")) <> planDesired -> raise (StateError "Snapshot ID already identifies different source revisions")
                    | _ -> ()
                    let invWatermark = int64Of build "invalidation_watermark"
                    for kv in indexed do
                        let reinvalidated =
                            not (State.Query connection "SELECT 1 AS x FROM invalidations WHERE source_id=$s AND id>$w LIMIT 1" [ "$s", box kv.Key; "$w", box invWatermark ]).IsEmpty
                        if reinvalidated then
                            State.Execute connection
                                "UPDATE sources SET indexed_revision=$r,indexed_known=0 WHERE source_id=$s"
                                [ "$r", (match kv.Value with Some r -> box r | None -> null); "$s", box kv.Key ]
                        else
                            State.Execute connection
                                "UPDATE sources SET indexed_revision=$r,indexed_known=1,changed_at=CASE WHEN desired_revision IS $r THEN NULL ELSE changed_at END,first_changed_at=CASE WHEN desired_revision IS $r THEN NULL ELSE first_changed_at END,settle_bypassed=CASE WHEN desired_revision IS $r THEN 0 ELSE settle_bypassed END WHERE source_id=$s"
                                [ "$r", (match kv.Value with Some r -> box r | None -> null); "$s", box kv.Key ]
                    State.Execute connection "INSERT INTO generations(snapshot_id,revisions,published_at,build_token,configuration_sha256) VALUES ($i,$r,$p,$t,$c)"
                        [ "$i", box snapshotId; "$r", box (jsonText (revisionsJ planDesired)); "$p", box now; "$t", box token
                          "$c", (match configurationSha256 with Some identity -> box identity | None -> null) ]
                    let generation = State.LastRowId connection
                    State.Execute connection "UPDATE builds SET status='completed',snapshot_id=$i,finished_at=$n WHERE token=$t" [ "$i", box snapshotId; "$n", box now; "$t", box token ]
                    this.RecordReadiness(connection, now)
                    Obj [ "published", Bool true; "status", Str "completed"; "generation", Int generation; "snapshot_id", Str snapshotId; "replayed", Bool false ])

    member this.FailBuild(token: string, error: string) : J =
        if isNull error || error.Length > 1024 then raise (StateError "Build error must be at most 1024 characters")
        this.Transaction(fun connection now _ ->
            let row = this.Build(connection, token)
            match str row "status" with
            | "completed" -> raise (StateError "A completed generation cannot be failed")
            | "running" ->
                State.Execute connection "UPDATE builds SET status='failed',error=$e,finished_at=$n WHERE token=$t" [ "$e", box error; "$n", box now; "$t", box token ]
                Obj [ "status", Str "failed"; "retry", Bool true ]
            | other -> Obj [ "status", Str other; "retry", Bool true ])

    static member private GenerationRow(row: Row) =
        { Generation = int64Of row "generation"; SnapshotId = str row "snapshot_id"; PublishedAt = (num row "published_at").Value
          Revisions = str row "revisions" |> JsonInput.parse |> State.ParseRevisions
          ConfigurationSha256 = match str row "configuration_sha256" with null -> None | identity -> Some identity }

    member private _.Latest(connection: SqliteConnection) =
        State.Query connection "SELECT * FROM generations ORDER BY generation DESC LIMIT 1" []
        |> List.tryHead |> Option.map State.GenerationRow

    // Fail-closed latest-only check.
    member this.Freshness(snapshotId: string, maxObservationAge: float, ?requiredRevisions: (string * string option) list) : J =
        if isNull snapshotId || not (snapshotPattern.IsMatch snapshotId) then raise (StateError "snapshot_id must be a lowercase 64-hex digest")
        let maximumAge = number maxObservationAge "max_observation_age" true
        let required = defaultArg requiredRevisions []
        this.Revisions required
        this.Transaction(fun connection now _ ->
            let latest = this.Latest connection
            let selected =
                if latest |> Option.exists (fun g -> g.SnapshotId = snapshotId) then latest
                else
                    State.Query connection "SELECT * FROM generations WHERE snapshot_id=$s ORDER BY generation DESC LIMIT 1" [ "$s", box snapshotId ]
                    |> List.tryHead |> Option.map State.GenerationRow
            let plan = this.ComputePlan connection
            let mismatch = required |> List.exists (fun (source, revision) ->
                (selected |> Option.bind (fun g -> Map.tryFind source g.Revisions)) <> Some revision
                || (Map.tryFind source plan.DesiredRevisions |> Option.flatten) <> revision)
            (assess now maximumAge snapshotId latest selected mismatch (this.ObservationFacts(connection, plan))).Freshness)

    member this.ObserveSince(maxObservationAge: float, after: int64 option) : ObservationBatch =
        let maximumAge = number maxObservationAge "max_observation_age" true
        this.Transaction(fun connection now sampledAt ->
            let bounds =
                State.Query connection "SELECT COALESCE(MIN(seq),0) AS min_seq, COALESCE(MAX(seq),0) AS max_seq FROM readiness_transitions" []
                |> List.head
            let oldest = int64Of bounds "min_seq"
            let sequence = int64Of bounds "max_seq"
            let current =
                match this.Latest connection with
                | Some generation as active ->
                    let plan = this.ComputePlan connection
                    assess now maximumAge generation.SnapshotId active active false (this.ObservationFacts(connection, plan))
                | None ->
                    { Freshness = Obj [ "fresh", Bool false; "reasons", Arr [ Str "no_published_generation" ] ]
                      Active = None; ChangedAt = None; Deadline = None; CheckedAt = now; Crossing = None }
            // None is a brand-new read; Some cursor (including Some 0L, held after an
            // unpublished sample) is an established cursor whose pruned successors truncate.
            let history =
                match after with
                | None -> Transitions []
                | Some cursor when cursor < sequence && oldest > cursor + 1L -> Truncated
                | Some cursor when cursor < sequence ->
                    // The newest commit can differ from Current after an observation expires.
                    let readiness (observation: Observation) =
                        let freshness =
                            match observation.Freshness with
                            | Obj pairs -> Obj (pairs |> List.filter (fun (key, _) -> key <> "checked_at"))
                            | other -> other
                        observation.Active, observation.ChangedAt, observation.Crossing, freshness
                    let currentReadiness = readiness current
                    State.Query connection
                        "SELECT g.*, t.seq, t.committed_at, t.facts FROM readiness_transitions t JOIN generations g USING (generation) WHERE t.seq>$after AND t.seq<=$sequence ORDER BY t.seq"
                        [ "$after", box cursor; "$sequence", box sequence ]
                    |> List.choose (fun row ->
                        let generation = State.GenerationRow row
                        let committed = assess (num row "committed_at").Value maximumAge generation.SnapshotId (Some generation) (Some generation) false (parseFacts (str row "facts"))
                        if int64Of row "seq" = sequence && readiness committed = currentReadiness then None
                        else Some committed)
                    |> Transitions
                | _ -> Transitions []
            { Sequence = sequence; History = history; Current = current; SampledAt = sampledAt })

    member this.Observe(maxObservationAge: float) = (this.ObserveSince(maxObservationAge, None)).Current
