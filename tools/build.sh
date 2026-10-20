#!/usr/bin/env bash
# Publish the .NET-hosted service without deleting the separately pinned native library.
# Output: .scratch/publish/retrieval-native (the existing deployment executable contract).
set -euo pipefail
cd "$(dirname "$0")/.."
dotnet publish src/Fanion/Fanion.fsproj -c Release -nologo -v q -o .scratch/publish
sha256sum .scratch/publish/retrieval-native
