/// Coordinator integration against local fixture repositories and the real engine.
/// Requires RETRIEVAL_TEST_EXTENSIONS (verified DuckPGQ directory) and RETRIEVAL_TEST_LIBDUCKDB.
module Retrieval.Native.Tests.CoordinatorTests

open System
open System.Diagnostics
open System.IO
open System.Text.Json
open System.Threading
open Xunit
open Retrieval.Native.Canonical
open Retrieval.Native.Tests.IndexStateTests

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
    let temp = Path.Combine(Path.GetTempPath(), "coordinator-" + Guid.NewGuid().ToString("N"))
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
    let _site = repo "site-fixture" "docs/guide.md" "# Guide\nSite documentation evidence.\n"
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

let private options (lab: Lab) (extensions: string) (libduckdb: string) : Retrieval.Native.Coordinator.Options =
    let lock name = Path.GetFullPath(Path.Combine(__SOURCE_DIRECTORY__, "../../config", name))
    let duckpgq = JsonDocument.Parse(File.ReadAllBytes(lock "duckpgq-lock.json")).RootElement
    let lib = JsonDocument.Parse(File.ReadAllBytes(lock "libduckdb-lock.json")).RootElement
    { Root = lab.Root; Config = lab.Config; Extensions = extensions
      Engine = { Extensions = extensions; LibDuckDb = libduckdb; LibDuckDbSha256 = lib.GetProperty("libduckdb_so_sha256").GetString()
                 DuckPgqVersion = duckpgq.GetProperty("extension_version").GetString(); DuckPgqSha256 = duckpgq.GetProperty("sha256").GetString(); Threads = 8; MemoryLimit = "1GB" }
      Budgets = { MaxOutputBytes = 384L * 1024L * 1024L; MaxNodes = 100_000; MaxEdges = 200_000; MaxTreeEntries = 100_000L; MaxFiles = 20_000; MaxSourceBytes = 128L * 1024L * 1024L; MaxDerivedEdges = 50_000; MaxCapabilityNodes = 2048; MaxMetadataBytes = 32768 }
      ExtractLimits = { MaxRepositories = 64; MaxTreeEntries = 100_000L; MaxTreeBytes = 16 * 1024 * 1024; MaxFiles = 20_000; MaxFileBytes = 1024 * 1024; MaxSourceBytes = 128L * 1024L * 1024L; MaxNodes = 100_000; MaxEdges = 200_000; MaxOutputBytes = 384L * 1024L * 1024L; MaxLayerRules = 64; MaxRulePatterns = 32; MaxTotalRulePatterns = 256; MaxLinksPerFile = 1024; MaxBuildSeconds = 600.0; GitTimeoutSeconds = 15.0 }
      RemoteBase = lab.Fixtures; LeaseSeconds = 900.0; SettleSeconds = Some 0.0; MaxSettleSeconds = Some 0.0
      LockFiles = [ lock "duckpgq-lock.json"; lock "libduckdb-lock.json" ] }

[<Fact>]
let ``local fixtures build, publish, reuse unchanged components and republish on change`` () =
    match environment () with
    | None -> ()  // engine not configured for this run
    | Some(extensions, libduckdb) ->
        use lab = new Lab()
        let coordinator = Retrieval.Native.Coordinator.Coordinator(options lab extensions libduckdb, CancellationToken.None)
        Assert.Empty(coordinator.Observe())
        let first = coordinator.RunOnce true
        Assert.True(bool' first "published")
        Assert.Equal(3, (strings first "rebuilt").Length)
        Assert.Equal("idle", str' (coordinator.RunOnce true) "status")
        let generation = coordinator.State.ActiveGeneration().Value
        Assert.True(File.Exists(Path.Combine(lab.Root, "generations", generation.SnapshotId + ".duckdb")))
        Assert.True(bool' (coordinator.State.Freshness(generation.SnapshotId, 90.0)) "fresh")
        let revision = lab.UpdateSpec "# Specification\nUpdated specification evidence.\n"
        Assert.Empty(coordinator.Observe())
        Assert.Equal(Some revision, coordinator.State.PendingPlan().DesiredRevisions.["spec"])
        let second = coordinator.RunOnce true
        Assert.True(bool' second "published")
        Assert.Equal<Set<string>>(set [ "spec"; "site" ], strings second "rebuilt" |> Set.ofList)  // site depends on spec
        Assert.Equal<string list>([ "other" ], strings second "reused")
        Assert.Equal(2L, coordinator.State.ActiveGeneration().Value.Generation)
