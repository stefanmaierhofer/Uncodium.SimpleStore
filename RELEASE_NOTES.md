### 3.0.31
- Read-only snapshots (`SimpleDiskStore.OpenReadOnlySnapshot`) work on Linux and macOS: they threw "Read-only snapshot not supported on Linux" before trying. Only Windows falls back to a writer's named memory map; elsewhere a store another writer holds open fails with its own error.
- Releases are published by GitHub Actions (`publish.yml`) with NuGet trusted publishing; `pushpackages.cmd` and its API key are gone.
