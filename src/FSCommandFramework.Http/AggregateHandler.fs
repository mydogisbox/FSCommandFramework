namespace FSCommandFramework.Http

open System
open System.Text.Json
open System.Text.Json.Serialization
open FSCommandFramework.Core

module EventSerializer =

    let private jsonOptions =
        let opts =
            JsonSerializerOptions(PropertyNamingPolicy = JsonNamingPolicy.CamelCase, PropertyNameCaseInsensitive = true)
        opts.Converters.Add(JsonFSharpConverter(JsonUnionEncoding.AdjacentTag ||| JsonUnionEncoding.NamedFields))
        opts

    let serialize (event: 'Event) : string * string =
        let json = JsonSerializer.Serialize(event, jsonOptions)
        use doc = JsonDocument.Parse json
        let typeName = doc.RootElement.GetProperty("Case").GetString()
        let payload =
            match doc.RootElement.TryGetProperty "Fields" with
            | true, fields -> fields.GetRawText()
            | false, _     -> "{}"
        typeName, payload

type AggregateHandler<'State, 'Event, 'Command>
    (
        definition: AggregateDefinition<'State, 'Event, 'Command>,
        eventStore: IEventStore,
        aggregateName: string,
        ?eventProcessor: EventProcessor,
        ?maxRetries: int
    ) =

    let maxRetries = defaultArg maxRetries 3

    member _.Apply = definition.Apply
    member _.EventStore = eventStore

    member _.ExecuteAsync
        (
            batch: CommandBatch,
            deserializeCommand: string -> JsonElement -> 'Command,
            deserializeEvent: string -> string -> 'Event
        ) =
        task {
            let aggregateId =
                if String.IsNullOrEmpty batch.AggregateId then
                    Guid.NewGuid().ToString()
                else
                    batch.AggregateId

            let streamId = $"{aggregateName}/{aggregateId}"

            let attempt () = task {
                let! stored = eventStore.LoadAsync streamId

                let currentSequence =
                    match stored with
                    | [] -> -1
                    | _ -> (List.last stored).Sequence

                let mutable state =
                    match stored with
                    | [] -> None
                    | _ ->
                        Aggregate.fold
                            (stored |> Seq.map (fun e -> deserializeEvent e.EventType e.Payload))
                            definition.Apply

                let results = ResizeArray<CommandSuccess>()
                let pendingEvents = ResizeArray<string * string>()
                let domainEvents = ResizeArray<obj>()
                let mutable error: string option = None
                let mutable i = 0

                while i < batch.Commands.Length && error.IsNone do
                    let envelope = batch.Commands.[i]
                    let command = deserializeCommand envelope.Type envelope.Payload

                    match definition.Dispatch state command with
                    | Error err -> error <- Some $"Command {i} ('{envelope.Type}') failed: {err}"
                    | Ok events ->
                        let serialized =
                            events |> List.map EventSerializer.serialize

                        for e in events do
                            state <- Some(definition.Apply state e)
                            domainEvents.Add(box e)

                        pendingEvents.AddRange serialized

                        results.Add
                            { Index = i
                              AggregateId = aggregateId
                              Events = serialized |> List.map fst }

                    i <- i + 1

                match error with
                | Some err -> return Error err
                | None ->
                    let! appendResult =
                        eventStore.AppendAsync(
                            streamId,
                            currentSequence,
                            pendingEvents,
                            eventProcessor,
                            Some(domainEvents :> obj seq)
                        )

                    return appendResult |> Result.map (fun _ -> results |> Seq.toList)
            }

            let! initial = attempt()
            let mutable result: Result<CommandSuccess list, string> = initial
            let mutable attempts = 1

            while attempts <= maxRetries &&
                  (match result with
                   | Error msg -> msg.StartsWith "Concurrency conflict"
                   | Ok _ -> false) do
                let! r = attempt()
                result <- r
                attempts <- attempts + 1

            return result
        }
