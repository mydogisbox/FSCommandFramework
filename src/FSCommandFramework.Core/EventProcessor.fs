namespace FSCommandFramework.Core

open System
open System.Data
open System.Threading.Tasks

type EventReaction =
    { EventType: Type
      Handle: obj -> IDbTransaction -> Task<unit> }

module EventReaction =

    let on<'Event> (handle: 'Event -> IDbTransaction -> Task<unit>) : EventReaction =
        { EventType = typeof<'Event>
          Handle = fun e tx -> handle (unbox<'Event> e) tx }

type EventProcessor(reactions: EventReaction list) =

    member _.ProcessAsync(events: obj seq, transaction: IDbTransaction) =
        task {
            for e in events do
                let eventType = e.GetType()

                for reaction in reactions |> List.filter (fun r -> r.EventType.IsAssignableFrom eventType) do
                    do! reaction.Handle e transaction
        }
