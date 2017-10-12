#load @"paket-files/build/aardvark-platform/aardvark.fake/DefaultSetup.fsx"

open Fake
open System
open System.IO
open System.Diagnostics
open Aardvark.Fake
open Fake.Testing

do MSBuildDefaults <- { MSBuildDefaults with Verbosity = Some Minimal }
do Environment.CurrentDirectory <- __SOURCE_DIRECTORY__

DefaultSetup.install ["src/Uncodium.SimpleStore.sln"]

#if DEBUG
do System.Diagnostics.Debugger.Launch() |> ignore
#endif

entry()