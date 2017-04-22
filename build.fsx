#load @"paket-files/build/vrvis/Aardvark.Fake/DefaultSetup.fsx"

open Fake
open System
open System.IO
open System.Diagnostics
open Aardvark.Fake
open Fake.Testing
open Mono.Cecil
open System.IO.Compression


do Environment.CurrentDirectory <- __SOURCE_DIRECTORY__

DefaultSetup.install ["src/Uncodium.SimpleStore.sln"]

#if DEBUG
do System.Diagnostics.Debugger.Launch() |> ignore
#endif


entry()