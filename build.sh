#!/bin/bash
set -euo pipefail

dotnet tool restore
dotnet paket restore
dotnet restore src/Uncodium.SimpleStore.sln
dotnet build src/Uncodium.SimpleStore.sln --configuration Release --no-restore
