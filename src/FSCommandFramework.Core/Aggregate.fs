namespace FSCommandFramework.Core

type AggregateDefinition<'State, 'Event, 'Command> =
    { Dispatch: 'State option -> 'Command -> Result<'Event list, string>
      Apply: 'State option -> 'Event -> 'State }

module Aggregate =

    let fold (events: 'Event seq) (apply: 'State option -> 'Event -> 'State) : 'State option =
        events |> Seq.fold (fun state event -> Some(apply state event)) None
