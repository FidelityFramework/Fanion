/// Read-only Git plumbing with bounded output and no lazy fetch, matching the
/// reference extractor's environment. One streaming `cat-file --batch` per
/// repository replaces a process per blob.
module Fanion.Git

open System
open System.Collections.Generic
open System.Diagnostics
open System.IO
open System.Text

exception IngestError of string

let private environment (info: ProcessStartInfo) =
    for key in [ for k in info.Environment.Keys -> k ] do
        if key.StartsWith("GIT_", StringComparison.Ordinal) then info.Environment.Remove(key) |> ignore
    for (k, v) in [ "GIT_CONFIG_NOSYSTEM", "1"; "GIT_CONFIG_GLOBAL", "/dev/null"; "GIT_NO_REPLACE_OBJECTS", "1"
                    "GIT_NO_LAZY_FETCH", "1"; "GIT_TERMINAL_PROMPT", "0"; "LC_ALL", "C" ] do
        info.Environment.[k] <- v

let private start (repo: string) (args: string list) (local: bool) =
    let info = ProcessStartInfo("git")
    let prefix = [ "--no-pager" ] @ (if local then [ "-c"; "protocol.allow=never" ] else [ "-c"; "credential.helper="; "-c"; "core.hooksPath=/dev/null" ])
                 @ (if isNull repo then [] else [ "-C"; repo ])
    for a in prefix @ args do info.ArgumentList.Add(a)
    info.RedirectStandardInput <- true
    info.RedirectStandardOutput <- true
    info.RedirectStandardError <- true
    info.UseShellExecute <- false
    environment info
    Process.Start(info)

/// Run one plumbing command; output bounded by `cap`; errors are concise (no paths).
let private runWith (repo: string) (args: string list) (cap: int) (deadline: DateTime) (timeoutSeconds: float) (local: bool) : byte[] =
    let finish = min deadline (DateTime.UtcNow.AddSeconds timeoutSeconds)
    if DateTime.UtcNow >= finish then raise (IngestError "snapshot build exceeded its time limit")
    use p = start repo args local
    p.StandardInput.Close()
    let stderrTask = p.StandardError.BaseStream.CopyToAsync(Stream.Null)
    let output = new MemoryStream()
    let buffer = Array.zeroCreate<byte> 65536
    let stream = p.StandardOutput.BaseStream
    let mutable finished = false
    while not finished do
        let remaining = finish - DateTime.UtcNow
        if remaining <= TimeSpan.Zero then
            (try p.Kill(true) with _ -> ())
            raise (IngestError "git object read exceeded its time limit")
        let readTask = stream.ReadAsync(buffer, 0, buffer.Length)
        if not (readTask.Wait(remaining)) then
            (try p.Kill(true) with _ -> ())
            raise (IngestError "git object read exceeded its time limit")
        let count = readTask.Result
        if count = 0 then finished <- true
        else
            output.Write(buffer, 0, count)
            if output.Length > int64 cap then
                (try p.Kill(true) with _ -> ())
                raise (IngestError "git object read exceeded its output limit")
    let remaining = finish - DateTime.UtcNow
    if not (p.WaitForExit(max 10 (int remaining.TotalMilliseconds))) then
        (try p.Kill(true) with _ -> ())
        raise (IngestError "git object read exceeded its time limit")
    stderrTask.Wait(1000) |> ignore
    if p.ExitCode <> 0 then raise (IngestError "git object read failed; verify repository and complete commit objects")
    output.ToArray()

/// Local read-only plumbing (protocol.allow=never).
let run (repo: string) (args: string list) (cap: int) (deadline: DateTime) (timeoutSeconds: float) : byte[] =
    runWith repo args cap deadline timeoutSeconds true

/// Network commands against the operator allowlist (ls-remote, fetch); `repo` may be null.
let runNetwork (repo: string) (args: string list) (cap: int) (timeoutSeconds: float) : byte[] =
    runWith repo args cap DateTime.MaxValue timeoutSeconds false

/// Streaming blob reader over `git cat-file --batch`.
type BatchReader(repo: string, deadline: DateTime, timeoutSeconds: float) =
    let p = start repo [ "cat-file"; "--batch" ] true
    let stdin = p.StandardInput
    let stdout = p.StandardOutput.BaseStream
    let stderrTask = p.StandardError.BaseStream.CopyToAsync(Stream.Null)
    let readLine () =
        let sb = StringBuilder()
        let mutable b = stdout.ReadByte()
        while b <> -1 && b <> int '\n' do
            sb.Append(char b) |> ignore
            b <- stdout.ReadByte()
        if b = -1 && sb.Length = 0 then raise (IngestError "git object read failed; verify repository and complete commit objects")
        sb.ToString()
    let readExactly (count: int) =
        let data = Array.zeroCreate<byte> count
        let mutable offset = 0
        while offset < count do
            if DateTime.UtcNow >= deadline then raise (IngestError "snapshot build exceeded its time limit")
            let n = stdout.Read(data, offset, count - offset)
            if n <= 0 then raise (IngestError "git object read failed; verify repository and complete commit objects")
            offset <- offset + n
        data
    /// Read one blob; the caller already knows the size from ls-tree and the cap.
    member _.Blob(objectId: string, expectedSize: int, cap: int) : byte[] =
        if DateTime.UtcNow >= deadline then raise (IngestError "snapshot build exceeded its time limit")
        stdin.Write(objectId)
        stdin.Write('\n')
        stdin.Flush()
        let header = readLine ()
        let parts = header.Split(' ')
        if parts.Length <> 3 || parts.[1] <> "blob" then raise (IngestError "git object read failed; verify repository and complete commit objects")
        let size = Int32.Parse(parts.[2], Globalization.CultureInfo.InvariantCulture)
        if size > cap then raise (IngestError "git object read exceeded its output limit")
        let data = readExactly size
        stdout.ReadByte() |> ignore // trailing newline
        if size <> expectedSize then raise (IngestError "git object size changed during read")
        data
    interface IDisposable with
        member _.Dispose() =
            (try stdin.Close() with _ -> ())
            (try (if not (p.WaitForExit(2000)) then p.Kill(true)) with _ -> ())
            (try stderrTask.Wait(500) |> ignore with _ -> ())
            p.Dispose()
    member this.Dispose() = (this :> IDisposable).Dispose()
