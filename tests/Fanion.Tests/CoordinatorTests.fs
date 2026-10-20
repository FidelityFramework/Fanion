/// Coordinator integration against local fixture repositories and the real engine.
/// Requires RETRIEVAL_TEST_EXTENSIONS (verified DuckPGQ directory) and RETRIEVAL_TEST_LIBDUCKDB.
module Fanion.Tests.CoordinatorTests

open System
open System.Diagnostics
open System.IO
open System.Threading
open Xunit
open Fanion.Canonical
open Fanion.Tests.IndexStateTests

let private git (repo: string) (args: string list) =
    let info = ProcessStartInfo("git")
    for a in [ "-C"; repo ] @ args do info.ArgumentList.Add a
    info.RedirectStandardOutput <- true; info.RedirectStandardError <- true; info.UseShellExecute <- false
    info.Environment.["GIT_CONFIG_NOSYSTEM"] <- "1"; info.Environment.["GIT_CONFIG_GLOBAL"] <- "/dev/null"
    use p = Process.Start info
    let out = p.StandardOutput.ReadToEnd()
    p.WaitForExit()
    if p.ExitCode <> 0 then failwithf "git %A failed: %s" args (p.StandardError.ReadToEnd())
    out.Trim()

let private environment () =
    match Environment.GetEnvironmentVariable "RETRIEVAL_TEST_EXTENSIONS", Environment.GetEnvironmentVariable "RETRIEVAL_TEST_LIBDUCKDB" with
    | null, _ | _, null -> None
    | extensions, libduckdb -> Some(extensions, libduckdb)

type Lab() =
    let temp = Path.GetFullPath(Path.Combine(__SOURCE_DIRECTORY__, "../../.scratch/tests", "coordinator-" + Guid.NewGuid().ToString("N")))
    let fixtures = Path.Combine(temp, "fixtures", "FidelityFramework")
    do Directory.CreateDirectory fixtures |> ignore
    let repo (name: string) (path: string) (text: string) =
        let directory = Path.Combine(fixtures, name)
        Directory.CreateDirectory directory |> ignore
        git directory [ "init"; "--quiet"; "--initial-branch=main" ] |> ignore
        git directory [ "config"; "user.name"; "Retrieval Test" ] |> ignore
        git directory [ "config"; "user.email"; "retrieval@example.invalid" ] |> ignore
        let target = Path.Combine(directory, path)
        Directory.CreateDirectory(Path.GetDirectoryName target) |> ignore
        File.WriteAllText(target, text)
        File.WriteAllText(Path.Combine(directory, "README.md"), sprintf "# Fixture %s\n[Source](%s)\n" name path)
        git directory [ "add"; "." ] |> ignore
        git directory [ "commit"; "--quiet"; "--no-gpg-sign"; "-m"; "Initial evidence" ] |> ignore
        directory
    let spec = repo "spec-fixture" "spec/chapter.md" "# Specification\nInitial specification evidence.\n"
    let _site = repo "site-fixture" "docs/guide.md" "# Guide\n[Specification](https://forge.spkez.dev/FidelityFramework/spec-fixture/src/branch/main/spec/chapter.md)\n"
    let _other = repo "other-fixture" "src/Main.fs" "let marker = \"unrelated implementation\"\n"
    let config = Path.Combine(temp, "corpus.json")
    do File.WriteAllText(config, """{"schema_version":1,"sources":[{"id":"spec","repo":"FidelityFramework/spec-fixture","ref":"refs/heads/main","depends_on":[]},{"id":"site","repo":"FidelityFramework/site-fixture","ref":"refs/heads/main","depends_on":["spec"]},{"id":"other","repo":"FidelityFramework/other-fixture","ref":"refs/heads/main","depends_on":[]}]}""")
    member _.Root = Path.Combine(temp, "state")
    member _.Config = config
    member _.Fixtures = Path.Combine(temp, "fixtures")
    member _.Spec = spec
    member _.UpdateSpec(text: string) =
        File.WriteAllText(Path.Combine(spec, "spec", "chapter.md"), text)
        git spec [ "commit"; "--quiet"; "--no-gpg-sign"; "-am"; "Update" ] |> ignore
        git spec [ "rev-parse"; "HEAD" ]
    interface IDisposable with
        member _.Dispose() = try Directory.Delete(temp, true) with _ -> ()

let private options (lab: Lab) (extensions: string) (libduckdb: string) : Fanion.Coordinator.Options =
    let lock name = Path.GetFullPath(Path.Combine(__SOURCE_DIRECTORY__, "../../config", name))
    let duckpgq = File.ReadAllBytes(lock "duckpgq-lock.json") |> JsonInput.parseBytes
    let lib = File.ReadAllBytes(lock "libduckdb-lock.json") |> JsonInput.parseBytes
    { Root = lab.Root; Config = lab.Config
      Engine = { LibDuckDb = libduckdb; LibDuckDbSha256 = JsonInput.property "libduckdb_so_sha256" lib |> JsonInput.string; DuckDbVersion = JsonInput.property "duckdb_version" lib |> JsonInput.string; DuckDbSourceId = JsonInput.property "duckdb_source_id" lib |> JsonInput.string
                 DuckPgqVersion = JsonInput.property "extension_version" duckpgq |> JsonInput.string; Threads = 8; MemoryLimit = "1GB" }
      Budgets = { MaxOutputBytes = 384L * 1024L * 1024L; MaxNodes = 100_000; MaxEdges = 200_000; MaxTreeEntries = 100_000L; MaxFiles = 20_000; MaxSourceBytes = 128L * 1024L * 1024L; MaxDerivedEdges = 50_000; MaxCapabilityNodes = 2048; MaxMetadataBytes = 32768 }
      ExtractLimits = { MaxRepositories = 64; MaxTreeEntries = 100_000L; MaxTreeBytes = 16 * 1024 * 1024; MaxFiles = 20_000; MaxFileBytes = 1024 * 1024; MaxSourceBytes = 128L * 1024L * 1024L; MaxNodes = 100_000; MaxEdges = 200_000; MaxOutputBytes = 384L * 1024L * 1024L; MaxLayerRules = 64; MaxRulePatterns = 32; MaxTotalRulePatterns = 256; MaxLinksPerFile = 1024; MaxBuildSeconds = 600.0; GitTimeoutSeconds = 15.0 }
      RemoteBase = lab.Fixtures; LeaseSeconds = 900.0; SettleSeconds = Some 0.0; MaxSettleSeconds = Some 0.0
      LockFiles = [ lock "duckpgq-lock.json"; lock "libduckdb-lock.json" ] }

[<Fact>]
let ``changed peers refresh all crosslinks while unchanged dependent components are reused`` () =
    match environment () with
    | None -> ()  // engine not configured for this run
    | Some(extensions, libduckdb) ->
        use lab = new Lab()
        let configured = options lab extensions libduckdb
        let coordinator = Fanion.Coordinator.Coordinator(configured, CancellationToken.None)
        Assert.Empty(coordinator.Observe())
        let first = coordinator.RunOnce true
        Assert.True(bool' first "published")
        Assert.Equal(3, (strings first "rebuilt").Length)
        Assert.Equal("idle", str' (coordinator.RunOnce true) "status")
        let generation = coordinator.State.ActiveGeneration().Value
        Assert.True(File.Exists(Path.Combine(lab.Root, "generations", generation.SnapshotId + ".duckdb")))
        Assert.True(bool' (coordinator.State.Freshness(generation.SnapshotId, 90.0)) "fresh")
        let link snapshot =
            use connection = Fanion.Load.openEngine configured.Engine (Path.Combine(lab.Root, "generations", snapshot + ".duckdb"))
            use command = connection.CreateCommand()
            command.CommandText <- """SELECT s.id,d.id,d.revision,e.evidence_id,e.evidence_line_start
                FROM edges e JOIN nodes s ON s.id=e.src JOIN nodes d ON d.id=e.dst
                WHERE e.kind='links_to' AND s.repo='FidelityFramework/site-fixture' AND s.path='docs/guide.md'
                AND d.repo='FidelityFramework/spec-fixture' AND d.path='spec/chapter.md'"""
            use reader = command.ExecuteReader()
            Assert.True(reader.Read(), "Expected cross-repository specification link")
            let row = reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.GetInt64(4)
            Assert.False(reader.Read(), "Cross-repository link must remain deduplicated")
            row
        let sourceId, oldTarget, _, evidenceId, evidenceLine = link generation.SnapshotId
        Assert.Equal(sourceId, evidenceId)
        Assert.Equal(2L, evidenceLine)
        let dependentRevision = coordinator.State.PendingPlan().DesiredRevisions.["site"].Value
        let dependentComponent = coordinator.ComponentPath("site", dependentRevision)
        let componentBytes = File.ReadAllBytes dependentComponent
        let retainedTime = DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc)
        File.SetLastWriteTimeUtc(dependentComponent, retainedTime)
        let revision = lab.UpdateSpec "# Specification\nUpdated specification evidence.\n"
        Assert.Empty(coordinator.Observe())
        Assert.Equal(Some revision, coordinator.State.PendingPlan().DesiredRevisions.["spec"])
        let second = coordinator.RunOnce true
        Assert.True(bool' second "published")
        Assert.Equal<string list>([ "spec" ], strings second "rebuilt")
        Assert.Equal<Set<string>>(set [ "other"; "site" ], strings second "reused" |> Set.ofList)
        Assert.Equal(retainedTime, File.GetLastWriteTimeUtc dependentComponent)
        Assert.Equal<byte>(componentBytes, File.ReadAllBytes dependentComponent)
        let current = coordinator.State.ActiveGeneration().Value
        Assert.Equal(2L, current.Generation)
        let currentSource, target, targetRevision, currentEvidence, currentLine = link current.SnapshotId
        Assert.Equal(sourceId, currentSource)
        Assert.NotEqual<string>(oldTarget, target)
        Assert.Equal(revision, targetRevision)
        Assert.Equal(evidenceId, currentEvidence)
        Assert.Equal(evidenceLine, currentLine)
        Assert.Empty(coordinator.State.PendingPlan().Targets)
        // Explicit invalidation still requests extraction even when its revision
        // and fingerprint remain unchanged.
        coordinator.State.InvalidateSources(["site"], "Revalidate cached component") |> ignore
        let invalidated = coordinator.RunOnce true
        Assert.Equal<string list>([ "site" ], strings invalidated "rebuilt")
        Assert.NotEqual(retainedTime, File.GetLastWriteTimeUtc dependentComponent)
        Assert.Equal(current.SnapshotId, coordinator.State.ActiveGeneration().Value.SnapshotId)

[<Fact>]
let ``existing target with another engine stays unpublished`` () =
    match environment () with
    | None -> Assert.Skip "Verified native engine required"
    | Some(extensions, libduckdb) ->
        use lab = new Lab()
        let configured = options lab extensions libduckdb
        let coordinator = Fanion.Coordinator.Coordinator(configured, CancellationToken.None)
        Assert.Empty(coordinator.Observe())
        coordinator.RunOnce true |> ignore
        let before = coordinator.State.ActiveGeneration().Value
        lab.UpdateSpec "# Specification\nNext unpublished target.\n" |> ignore
        let candidateRoot = Path.Combine(lab.Root, "candidate")
        let candidate = Fanion.Coordinator.Coordinator({ configured with Root = candidateRoot }, CancellationToken.None)
        Assert.Empty(candidate.Observe())
        candidate.RunOnce true |> ignore
        let next = candidate.State.ActiveGeneration().Value
        let path = Path.Combine(lab.Root, "generations", next.SnapshotId + ".duckdb")
        File.Copy(Path.Combine(candidateRoot, "generations", next.SnapshotId + ".duckdb"), path)
        do
            use connection = Fanion.Load.openEngine configured.Engine path
            Fanion.Load.execute connection
                "UPDATE metadata SET value=json_merge_patch(value, '{\"engine\":{\"libduckdb_sha256\":\"wrong-artifact\"}}')"
        Assert.Empty(coordinator.Observe())
        let error = Assert.Throws<Fanion.Load.LoadError>(fun () -> coordinator.RunOnce true |> ignore)
        Assert.Contains("Generation engine identity differs", error.Message)
        let after = coordinator.State.ActiveGeneration().Value
        Assert.Equal(before.SnapshotId, after.SnapshotId)
        Assert.Equal(before.Generation, after.Generation)
        Assert.NotEmpty(coordinator.State.PendingPlan().Targets)

[<Fact>]
let ``unchanged active generation resolves while another process holds its write lock`` () =
    match environment () with
    | None -> Assert.Skip "Verified native engine required"
    | Some(extensions, libduckdb) ->
        use lab = new Lab()
        let configured = options lab extensions libduckdb
        let coordinator = Fanion.Coordinator.Coordinator(configured, CancellationToken.None)
        Assert.Empty(coordinator.Observe())
        coordinator.RunOnce true |> ignore
        let before = coordinator.State.ActiveGeneration().Value
        let database = Path.Combine(lab.Root, "generations", before.SnapshotId + ".duckdb")
        let quoted (value: string) = toText defaultStyle (Str value)
        let assembly = typeof<Fanion.Load.Engine>.Assembly.Location
        let folder = Path.GetDirectoryName assembly
        let e = configured.Engine
        let script = Path.Combine(lab.Root, "hold-active.fsx")
        let content =
            [ for name in ["DuckDB.NET.Bindings.dll"; "DuckDB.NET.Data.dll"; Path.GetFileName assembly] -> "#r " + quoted(Path.Combine(folder, name)) ]
            @ [ "open Fanion.Load"
                sprintf "let engine:Engine={LibDuckDb=%s;LibDuckDbSha256=%s;DuckDbVersion=%s;DuckDbSourceId=%s;DuckPgqVersion=%s;Threads=1;MemoryLimit=\"256MB\"}"
                    (quoted e.LibDuckDb) (quoted e.LibDuckDbSha256) (quoted e.DuckDbVersion) (quoted e.DuckDbSourceId) (quoted e.DuckPgqVersion)
                "let connection=openEngine engine " + quoted database
                "printfn \"READY\""
                "System.Console.ReadLine() |> ignore"
                "connection.Dispose()" ]
        File.WriteAllLines(script, content)
        let start = ProcessStartInfo("dotnet", UseShellExecute=false, RedirectStandardInput=true, RedirectStandardOutput=true, RedirectStandardError=true)
        for arg in ["fsi"; "--exec"; script] do start.ArgumentList.Add arg
        use child = Process.Start start
        let errors = child.StandardError.ReadToEndAsync()
        try
            let ready = child.StandardOutput.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds 30.).GetAwaiter().GetResult()
            Assert.True((ready = "READY"), "Child did not acquire database lock: " + (if child.HasExited then errors.Result else string ready))
            coordinator.State.InvalidateSources(["spec"], "same revisions, serving process holds active graph") |> ignore
            let result = coordinator.RunOnce true
            Assert.True(bool' result "published")
            Assert.Equal(before.SnapshotId, coordinator.State.ActiveGeneration().Value.SnapshotId)
            Assert.Empty(coordinator.State.PendingPlan().Targets)
        finally
            if not child.HasExited then
                child.StandardInput.WriteLine()
                child.StandardInput.Flush()
                if not(child.WaitForExit 5000) then child.Kill(true); child.WaitForExit()
