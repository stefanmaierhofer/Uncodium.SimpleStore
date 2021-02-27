@echo off
SETLOCAL
PUSHD %~dp0

dotnet tool restore
dotnet paket restore
dotnet build src/Uncodium.SimpleStore.sln --configuration Release