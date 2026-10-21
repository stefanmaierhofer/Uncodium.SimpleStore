# Repository Guidelines

## Project Structure & Module Organization

The solution is `src/Uncodium.SimpleStore.sln`. Core storage interfaces and the memory, folder, and disk implementations live in `src/Uncodium.SimpleStore/`. Azure Blob Storage support is isolated in `src/Uncodium.SimpleStore.AzureBlobStorage/`. NUnit coverage is under `src/Uncodium.SimpleStore.Tests/`. Root-level scripts handle builds and tests; `.github/workflows/` contains Windows, Linux, and macOS CI builds. Treat `bin/`, `obj/`, and restored Paket artifacts as generated files.

## Build, Test, and Development Commands

- `dotnet tool restore` installs the repository-pinned Paket tool.
- `dotnet paket restore` restores dependencies from `paket.lock`; `dotnet restore src/Uncodium.SimpleStore.sln` restores the complete solution.
- `.\build.cmd` builds the solution in Release mode on Windows; use `./build.sh` on Unix-like systems.
- `.\test.cmd` restores dependencies and runs all test targets in Release mode.
- `dotnet test src/Uncodium.SimpleStore.Tests/Uncodium.SimpleStore.Tests.csproj -c Release` runs the NUnit project directly.

Use a stable .NET 10 SDK compatible with the CI configuration. The libraries target `netstandard2.0`; tests target `net472`, `net48`, and `net10.0`, so running all test targets requires Windows and the corresponding .NET Framework reference assemblies.

## Coding Style & Naming Conventions

Use four-space indentation, braces on their own lines, and the latest C# language version. Preserve the surrounding namespace style when editing older files. Use PascalCase for types, methods, properties, and test names; use camelCase for parameters and locals. Existing private fields use both `m_name` and `_name`, so follow the convention of the file being changed. Keep nullable annotations accurate in library projects and document new public APIs with concise XML comments. No repository-wide formatter is configured; avoid unrelated reformatting.

## Testing Guidelines

Tests use NUnit 4 with `[TestFixture]` and `[Test]`. Name fixtures `*Tests` and use behavior-oriented names such as `CanOpenDiskStoreTwiceReadonly`. Add regression tests beside the closest existing fixture, clean up temporary stores and `.log` files, and keep tests deterministic. No coverage threshold is enforced, but changes to persistence, concurrency, or conversion logic should exercise success and failure paths.

## Dependencies, Security & Configuration

Manage library dependencies through `paket.dependencies`, project `paket.references`, and `paket.lock`; test-runner packages are direct references in the test project. Never commit live Azure SAS tokens, connection strings, local drive paths, or generated store data.

## Releases

A release is a commit on `main` that adds a `### x.y.z` section with its bullets at the top of `RELEASE_NOTES.md`. Pushing it runs `.github/workflows/publish.yml`: unless a GitHub release of that version exists, it runs the build-and-test workflow on Windows, Linux and macOS, packs both packages with `dotnet aardpack` (version from `RELEASE_NOTES.md`; it creates the GitHub release) and pushes them to nuget.org with trusted publishing (`NuGet/login`, repository secret `NUGET_USER` = the nuget.org account whose trusted publishing policy names this repository and `publish.yml`). No API key is stored anywhere. nuget.org lists the packages a few minutes after the push.

## Commit & Pull Request Guidelines

History favors short, direct subjects, commonly lowercase (for example, `fix test project`), with version-only commits reserved for releases. Keep each commit focused and use an imperative summary. Pull requests should explain the behavior change, identify storage-format or API compatibility effects, link relevant issues, and list the commands and target frameworks tested. Call out platform-specific behavior explicitly; screenshots are only useful for documentation changes with visual output.
