namespace FSCommandFramework.Core

type AggregateDefinition<'State, 'Event, 'Command> =
    { Dispatch: 'State option -> 'Command -> Result<'Event list, string>
      Apply: 'State option -> 'Event -> 'State }

module Aggregate =

    let fold (events: 'Event seq) (apply: 'State option -> 'Event -> 'State) : 'State option =
        events |> Seq.fold (fun state event -> Some(apply state event)) None

    let applyBatch
            (definition: AggregateDefinition<'State, 'Event, 'Command>)
            (initialState: 'State option)
            (commands: 'Command list)
            : Result<'Event list, string> =
        let rec loop state remaining acc =
            match remaining with
            | [] -> Ok (List.rev acc)
            | cmd :: rest ->
                match definition.Dispatch state cmd with
                | Error e    -> Error e
                | Ok newEvts ->
                    let nextState = newEvts |> List.fold (fun s e -> Some(definition.Apply s e)) state
                    loop nextState rest (List.rev newEvts @ acc)
        loop initialState commands []
