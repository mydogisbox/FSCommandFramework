module FSCommandFramework.Http.DcbAggregate

open FsToolkit.ErrorHandling
open FSCommandFramework.Core

let execute
        (dcbStore: IDcbEventStore)
        (definition: AggregateDefinition<'State, 'Event, 'Command>)
        (deserialize: string -> string -> 'Event)
        (tags: Tag list)
        (eventTags: 'Event -> 'State option -> Tag list)
        (command: 'Command)
        : System.Threading.Tasks.Task<Result<'Event list, string>> =
    taskResult {
        let! read     = dcbStore.ReadAsync tags
        let rawState  =
            read.Events
            |> Seq.map (fun e -> deserialize e.EventType e.Payload)
            |> fun evts -> Aggregate.fold evts definition.Apply
        let! events   = definition.Dispatch rawState command
        let serialized =
            events |> List.map (fun e ->
                let typeName, payload = EventSerializer.serialize e
                typeName, payload, eventTags e rawState)
        let condition = { FailIfEventsMatch = tags; After = Some read.ConsistencyMarker }
        let! _        = dcbStore.AppendAsync(serialized, Some condition)
        return events
    }

let executeBatch
        (dcbStore: IDcbEventStore)
        (definition: AggregateDefinition<'State, 'Event, 'Command>)
        (deserialize: string -> string -> 'Event)
        (tags: Tag list)
        (eventTags: 'Event -> Tag list)
        (commands: 'Command list)
        : System.Threading.Tasks.Task<Result<'Event list, string>> =
    taskResult {
        let! read        = dcbStore.ReadAsync tags
        let initialState =
            read.Events
            |> Seq.map (fun e -> deserialize e.EventType e.Payload)
            |> fun evts -> Aggregate.fold evts definition.Apply
        let! allEvents   = Aggregate.applyBatch definition initialState commands
        let serialized   =
            allEvents |> List.map (fun e ->
                let typeName, payload = EventSerializer.serialize e
                typeName, payload, eventTags e)
        let condition    = { FailIfEventsMatch = tags; After = Some read.ConsistencyMarker }
        let! _           = dcbStore.AppendAsync(serialized, Some condition)
        return allEvents
    }
