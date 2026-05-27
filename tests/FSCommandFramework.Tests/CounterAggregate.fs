namespace FSCommandFramework.Tests

open FSCommandFramework.Core
open FSCommandFramework.Http

// ── State ────────────────────────────────────────────────────────────────────

type CounterState = { Value: int }

// ── Events ───────────────────────────────────────────────────────────────────

type CounterEvent =
    | CounterIncremented of by: int
    | CounterDecremented of by: int
    | CounterReset

// ── Commands ─────────────────────────────────────────────────────────────────

type CounterCommand =
    | Increment of by: int
    | Decrement of by: int
    | Reset

// ── Aggregate ────────────────────────────────────────────────────────────────

module CounterAggregate =

    let dispatch (_: CounterState option) (command: CounterCommand) : Result<CounterEvent list, string> =
        match command with
        | Increment by when by <= 0 -> Error "Increment must be positive."
        | Increment by -> Ok [ CounterIncremented by ]
        | Decrement by when by <= 0 -> Error "Decrement must be positive."
        | Decrement by -> Ok [ CounterDecremented by ]
        | Reset -> Ok [ CounterReset ]

    let apply (state: CounterState option) (event: CounterEvent) : CounterState =
        match event with
        | CounterIncremented by -> { Value = (state |> Option.map _.Value |> Option.defaultValue 0) + by }
        | CounterDecremented by -> { Value = (state |> Option.map _.Value |> Option.defaultValue 0) - by }
        | CounterReset -> { Value = 0 }

    let deserializeCommand = ReflectionDeserializer.forCommands<CounterCommand> None
    let deserializeEvent = ReflectionDeserializer.forEvents<CounterEvent> None

    let definition = { Dispatch = dispatch; Apply = apply }
