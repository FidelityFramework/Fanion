#!/usr/bin/env bash
# Publish the native retrieval toolchain (relationship pass, generation build) as a self-contained linux-x64 single file.
# Output: native/publish/retrieval-native (consumed by the indexer and the release bundle).
set -euo pipefail
cd "$(dirname "$0")"
rm -rf publish
dotnet publish Retrieval.Native/Retrieval.Native.fsproj -c Release -nologo -v q -o publish
sha256sum publish/retrieval-native
