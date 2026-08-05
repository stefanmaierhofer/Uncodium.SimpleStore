@echo off
SETLOCAL
PUSHD %~dp0

dotnet tool restore || EXIT /B 1
dotnet paket restore || EXIT /B 1
dotnet restore src/Uncodium.SimpleStore.sln || EXIT /B 1
dotnet test src/Uncodium.SimpleStore.Tests/Uncodium.SimpleStore.Tests.csproj -c Release -v m --no-restore
