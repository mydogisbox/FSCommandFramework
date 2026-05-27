namespace FSCommandFramework.Http

open System.Text.Json

[<CLIMutable>]
type CommandEnvelope = { Type: string; Payload: JsonElement }

[<CLIMutable>]
type CommandBatch =
    { AggregateId: string
      Commands: CommandEnvelope array }

type CommandSuccess =
    { Index: int
      AggregateId: string
      Events: string list }

type CommandFailure = { Error: string }
