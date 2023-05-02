#!/bin/bash

dotnet tool restore
dotnet paket restore
dotnet build src/Uncodium.SimpleStore.sln --configuration Release
