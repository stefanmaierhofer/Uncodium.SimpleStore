@echo off
SETLOCAL
PUSHD %~dp0

dotnet tool restore
dotnet paket restore
dotnet test src -c Release -v m