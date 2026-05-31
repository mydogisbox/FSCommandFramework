namespace FSCommandFramework.Tests

open System
open System.Net
open System.Net.Http
open System.Text
open System.Text.Json
open System.Threading.Tasks
open Dapper
open Npgsql
open Xunit
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.TestHost
open FSCommandFramework.Core
open FSCommandFramework.Http
open FSCommandFramework.Postgres

module Helpers =

    let json value =
        JsonSerializer.SerializeToElement(
            value,
            JsonSerializerOptions(PropertyNamingPolicy = JsonNamingPolicy.CamelCase)
        )

    let expectOk result =
        Result.defaultWith (fun e -> failwith $"Expected Ok but got Error: {e}") result

    let expectError = function
        | Error e -> e
        | Ok _ -> failwith "Expected Error but got Ok"

    let Ignore(t: Task<_>) = (t :> Task)

open Helpers

// ── Counter Tests ────────────────────────────────────────────────────────────

type CounterTests() =

    let db = TestDatabase()

    let handler =
        AggregateHandler(
            CounterAggregate.definition,
            PostgresEventStore db.ConnectionString,
            "counters"
        )

    member private _.ExecuteAsync(aggregateId, commands) =
        handler.ExecuteAsync(
            { AggregateId = aggregateId
              Commands = commands },
            CounterAggregate.deserializeCommand,
            CounterAggregate.deserializeEvent
        )

    member private this.IncrementAsync by =
        task {
            let aggregateId = Guid.NewGuid().ToString()

            let! result =
                this.ExecuteAsync(
                    aggregateId,
                    [| { Type = "Increment"
                         Payload = json {| by = by |} } |]
                )

            expectOk result |> ignore
            return aggregateId
        }

    member private _.GetStateAsync aggregateId =
        task {
            let store = PostgresEventStore db.ConnectionString
            let! stored = (store :> IEventStore).LoadAsync($"counters/{aggregateId}")

            return
                Aggregate.fold
                    (stored
                        |> Seq.map (fun e -> CounterAggregate.deserializeEvent e.EventType e.Payload))
                    CounterAggregate.apply
        }

    [<Fact>]
    member this.``Increment succeeds``() =
        task {
            let! result =
                this.ExecuteAsync(
                    null,
                    [| { Type = "Increment"
                         Payload = json {| by = 5 |} } |]
                )

            let value = expectOk result
            Assert.Single value |> ignore
            Assert.Equal("CounterIncremented", value.[0].Events.[0])
        }

    [<Fact>]
    member this.``Increment fails with non-positive value``() =
        task {
            let! result =
                this.ExecuteAsync(
                    null,
                    [| { Type = "Increment"
                         Payload = json {| by = 0 |} } |]
                )

            Assert.Contains("positive", expectError result)
        }

    [<Fact>]
    member this.``Decrement succeeds``() =
        task {
            let! aggregateId = this.IncrementAsync 10

            let! result =
                this.ExecuteAsync(
                    aggregateId,
                    [| { Type = "Decrement"
                         Payload = json {| by = 3 |} } |]
                )

            expectOk result |> ignore
            let! state = this.GetStateAsync aggregateId
            Assert.Equal(7, state.Value.Value)
        }

    [<Fact>]
    member this.``Decrement fails with non-positive value``() =
        task {
            let! aggregateId = this.IncrementAsync 10

            let! result =
                this.ExecuteAsync(
                    aggregateId,
                    [| { Type = "Decrement"
                         Payload = json {| by = -1 |} } |]
                )

            Assert.Contains("positive", expectError result)
        }

    [<Fact>]
    member this.``Reset succeeds``() =
        task {
            let! aggregateId = this.IncrementAsync 10
            let! result = this.ExecuteAsync(aggregateId, [| { Type = "Reset"; Payload = json {| |} } |])

            expectOk result |> ignore
            let! state = this.GetStateAsync aggregateId
            Assert.Equal(0, state.Value.Value)
        }

    [<Fact>]
    member this.``Batch increment and decrement in single request``() =
        task {
            let aggregateId = Guid.NewGuid().ToString()

            let! result =
                this.ExecuteAsync(
                    aggregateId,
                    [| { Type = "Increment"
                         Payload = json {| by = 10 |} }
                       { Type = "Decrement"
                         Payload = json {| by = 3 |} } |]
                )

            let value = expectOk result
            Assert.Equal(2, value.Length)

            let! state = this.GetStateAsync aggregateId
            Assert.Equal(7, state.Value.Value)
        }

    [<Fact>]
    member this.``Batch rolls back if any command fails``() =
        task {
            let aggregateId = Guid.NewGuid().ToString()

            let! result =
                this.ExecuteAsync(
                    aggregateId,
                    [| { Type = "Increment"
                         Payload = json {| by = 10 |} }
                       { Type = "Decrement"
                         Payload = json {| by = 0 |} } |]
                )

            expectError result |> ignore
            let! state = this.GetStateAsync aggregateId
            Assert.True state.IsNone
        }

// ── EventProcessor Tests ─────────────────────────────────────────────────────

type EventProcessorTests() =

    let db = TestDatabase()

    [<Fact>]
    member _.``Sync reaction writes to outbox in same transaction``() =
        task {
            let aggregateId = Guid.NewGuid().ToString()
            let streamId = $"counters/{aggregateId}"

            let processor =
                EventProcessor
                    [ EventReaction.on<CounterEvent> (fun e tx ->
                          task {
                              match e with
                              | CounterIncremented by ->
                                  let conn = (tx :?> NpgsqlTransaction).Connection

                                  do! conn.ExecuteAsync(
                                          "INSERT INTO outbox (stream_id, event_type, payload, created_at) VALUES (@streamId, @eventType, @payload::jsonb, @createdAt)",
                                          {| streamId = streamId
                                             eventType = "CounterIncremented"
                                             payload =
                                              JsonSerializer.Serialize(
                                                  {| by = by |},
                                                  JsonSerializerOptions(
                                                      PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                                                  )
                                              )
                                             createdAt = DateTimeOffset.UtcNow |},
                                          tx
                                      ) |> Ignore
                              | _ -> ()
                          }) ]

            let handler =
                AggregateHandler(
                    CounterAggregate.definition,
                    PostgresEventStore db.ConnectionString,
                    "counters",
                    processor
                )

            let! result =
                handler.ExecuteAsync(
                    { AggregateId = aggregateId
                      Commands =
                        [| { Type = "Increment"
                             Payload = json {| by = 1 |} } |] },
                    CounterAggregate.deserializeCommand,
                    CounterAggregate.deserializeEvent
                )

            expectOk result |> ignore

            use conn = new NpgsqlConnection(db.ConnectionString)
            do! conn.OpenAsync()

            let! count =
                conn.ExecuteScalarAsync<int64>(
                    "SELECT COUNT(*) FROM outbox WHERE stream_id = @streamId",
                    {| streamId = streamId |}
                )

            Assert.Equal(1L, count)
        }

    [<Fact>]
    member _.``Sync reaction rolls back with failed append``() =
        task {
            let eventStore = PostgresEventStore db.ConnectionString :> IEventStore
            let aggregateId = Guid.NewGuid().ToString()
            let streamId = $"counters/{aggregateId}"

            let processor =
                EventProcessor
                    [ EventReaction.on<CounterEvent> (fun e tx ->
                          task {
                              match e with
                              | CounterIncremented by ->
                                  let conn = (tx :?> NpgsqlTransaction).Connection

                                  do! conn.ExecuteAsync(
                                          "INSERT INTO outbox (stream_id, event_type, payload, created_at) VALUES (@streamId, @eventType, @payload::jsonb, @createdAt)",
                                          {| streamId = streamId
                                             eventType = "CounterIncremented"
                                             payload =
                                              JsonSerializer.Serialize(
                                                  {| by = by |},
                                                  JsonSerializerOptions(
                                                      PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                                                  )
                                              )
                                             createdAt = DateTimeOffset.UtcNow |},
                                          tx
                                      ) |> Ignore
                              | _ -> ()
                          }) ]

            let! _ = eventStore.AppendAsync(streamId, -1, [ ("CounterIncremented", """{"by":1}""") ], None, None)

            let! result =
                eventStore.AppendAsync(
                    streamId,
                    -1,
                    [ ("CounterIncremented", """{"by":1}""") ],
                    Some processor,
                    Some([ CounterIncremented 1 |> box ] :> obj seq)
                )

            expectError result |> ignore

            use conn = new NpgsqlConnection(db.ConnectionString)
            do! conn.OpenAsync()

            let! count =
                conn.ExecuteScalarAsync<int64>(
                    "SELECT COUNT(*) FROM outbox WHERE stream_id = @streamId",
                    {| streamId = streamId |}
                )

            Assert.Equal(0L, count)
        }

// ── Edge Case Tests ──────────────────────────────────────────────────────────

type EdgeCaseTests() =

    let db = TestDatabase()

    [<Fact>]
    member _.``Reaction that throws rolls back events``() =
        task {
            let processor =
                EventProcessor
                    [ EventReaction.on<CounterEvent> (fun _ _ -> raise (InvalidOperationException "reaction failed")) ]

            let handler =
                AggregateHandler(
                    CounterAggregate.definition,
                    PostgresEventStore db.ConnectionString,
                    "counters",
                    processor
                )

            let aggregateId = Guid.NewGuid().ToString()

            let! _ =
                Assert.ThrowsAsync<InvalidOperationException>(fun () ->
                    handler.ExecuteAsync(
                        { AggregateId = aggregateId
                          Commands =
                            [| { Type = "Increment"
                                 Payload = json {| by = 1 |} } |] },
                        CounterAggregate.deserializeCommand,
                        CounterAggregate.deserializeEvent
                    ))

            let store = PostgresEventStore db.ConnectionString :> IEventStore
            let! stored = store.LoadAsync $"counters/{aggregateId}"
            Assert.Empty stored
        }

    [<Fact>]
    member _.``Appending zero events is a noop``() =
        task {
            let store = PostgresEventStore db.ConnectionString :> IEventStore
            let streamId = $"counters/{Guid.NewGuid()}"

            let! result = store.AppendAsync(streamId, -1, [], None, None)
            Assert.Empty(expectOk result)

            let! stored = store.LoadAsync streamId
            Assert.Empty stored
        }

    [<Fact>]
    member _.``Unknown command type throws and nothing is written``() =
        task {
            let handler =
                AggregateHandler(
                    CounterAggregate.definition,
                    PostgresEventStore db.ConnectionString,
                    "counters"
                )

            let aggregateId = Guid.NewGuid().ToString()

            let! _ =
                Assert.ThrowsAnyAsync<Exception>(fun () ->
                    handler.ExecuteAsync(
                        { AggregateId = aggregateId
                          Commands =
                            [| { Type = "Increment";      Payload = json {| by = 1 |} }
                               { Type = "UnknownCommand"; Payload = json {| |} } |] },
                        CounterAggregate.deserializeCommand,
                        CounterAggregate.deserializeEvent
                    ))

            let store = PostgresEventStore db.ConnectionString
            let! stored = (store :> IEventStore).LoadAsync $"counters/{aggregateId}"
            Assert.Empty stored
        }

    [<Fact>]
    member _.``Malformed payload mid batch nothing is written``() =
        task {
            let handler =
                AggregateHandler(
                    CounterAggregate.definition,
                    PostgresEventStore db.ConnectionString,
                    "counters"
                )

            let aggregateId = Guid.NewGuid().ToString()

            let! _ =
                Assert.ThrowsAnyAsync<Exception>(fun () ->
                    handler.ExecuteAsync(
                        { AggregateId = aggregateId
                          Commands =
                            [| { Type = "Increment"; Payload = json {| by = 1 |} }
                               { Type = "Decrement"; Payload = JsonSerializer.SerializeToElement "not-an-object" } |] },
                        CounterAggregate.deserializeCommand,
                        CounterAggregate.deserializeEvent
                    ))

            let store = PostgresEventStore db.ConnectionString
            let! stored = (store :> IEventStore).LoadAsync $"counters/{aggregateId}"
            Assert.Empty stored
        }

    [<Fact>]
    member _.``Intra-batch conflict nothing is written``() =
        task {
            let handler =
                AggregateHandler(
                    CounterAggregate.definition,
                    PostgresEventStore db.ConnectionString,
                    "counters"
                )

            let aggregateId = Guid.NewGuid().ToString()

            let! result =
                handler.ExecuteAsync(
                    { AggregateId = aggregateId
                      Commands =
                        [| { Type = "Increment"; Payload = json {| by = 5 |} }
                           { Type = "Decrement"; Payload = json {| by = 0 |} } |] },
                    CounterAggregate.deserializeCommand,
                    CounterAggregate.deserializeEvent
                )

            expectError result |> ignore

            let store = PostgresEventStore db.ConnectionString
            let! stored = (store :> IEventStore).LoadAsync $"counters/{aggregateId}"
            Assert.Empty stored
        }

    [<Fact>]
    member _.``Concurrent appends to same stream one succeeds one fails``() =
        task {
            let eventStore = PostgresEventStore db.ConnectionString :> IEventStore
            let streamId = $"counters/{Guid.NewGuid()}"

            let task1 =
                eventStore.AppendAsync(streamId, -1, [ ("CounterIncremented", """{"by":1}""") ], None, None)

            let task2 =
                eventStore.AppendAsync(streamId, -1, [ ("CounterIncremented", """{"by":2}""") ], None, None)

            let! results = Task.WhenAll(task1, task2)
            Assert.Equal(1, results |> Array.filter Result.isOk |> Array.length)
            Assert.Equal(1, results |> Array.filter Result.isError |> Array.length)

            let! stored = eventStore.LoadAsync streamId
            Assert.Single stored |> ignore
        }

    [<Fact>]
    member _.``Cross-aggregate reaction writes both streams atomically``() =
        task {
            let streamBId = $"counters/{Guid.NewGuid()}"

            let processor =
                EventProcessor
                    [ EventReaction.on<CounterEvent> (fun e tx ->
                          task {
                              match e with
                              | CounterIncremented by ->
                                  let npgsqlTx = tx :?> NpgsqlTransaction

                                  do! npgsqlTx.Connection.ExecuteAsync(
                                          "INSERT INTO events (stream_id, sequence, event_type, payload, occurred_at) VALUES (@streamId, 0, @eventType, @payload::jsonb, @occurredAt)",
                                          {| streamId = streamBId
                                             eventType = "CounterIncremented"
                                             payload = $"""{{"by":{by}}}"""
                                             occurredAt = DateTimeOffset.UtcNow |},
                                          npgsqlTx
                                      ) |> Ignore
                                  ()
                              | _ -> ()
                          }) ]

            let handler =
                AggregateHandler(
                    CounterAggregate.definition,
                    PostgresEventStore db.ConnectionString,
                    "counters",
                    processor
                )

            let aggregateIdA = Guid.NewGuid().ToString()

            let! result =
                handler.ExecuteAsync(
                    { AggregateId = aggregateIdA
                      Commands =
                        [| { Type = "Increment"
                             Payload = json {| by = 5 |} } |] },
                    CounterAggregate.deserializeCommand,
                    CounterAggregate.deserializeEvent
                )

            expectOk result |> ignore

            let eventStore = PostgresEventStore db.ConnectionString :> IEventStore
            let! streamAEvents = eventStore.LoadAsync $"counters/{aggregateIdA}"
            let! streamBEvents = eventStore.LoadAsync streamBId

            Assert.Single streamAEvents |> ignore
            Assert.Single streamBEvents |> ignore
        }

    [<Fact>]
    member _.``Cross-aggregate reaction rolls back both streams on failure``() =
        task {
            let streamBId = $"counters/{Guid.NewGuid()}"

            let processor =
                EventProcessor
                    [ EventReaction.on<CounterEvent> (fun e tx ->
                          task {
                              match e with
                              | CounterIncremented by ->
                                  let npgsqlTx = tx :?> NpgsqlTransaction

                                  let! _ =
                                      npgsqlTx.Connection.ExecuteAsync(
                                          "INSERT INTO events (stream_id, sequence, event_type, payload, occurred_at) VALUES (@streamId, 0, @eventType, @payload::jsonb, @occurredAt)",
                                          {| streamId = streamBId
                                             eventType = "CounterIncremented"
                                             payload = $"""{{"by":{by}}}"""
                                             occurredAt = DateTimeOffset.UtcNow |},
                                          npgsqlTx
                                      )

                                  raise (InvalidOperationException "downstream write failed")
                              | _ -> ()
                          }) ]

            let handler =
                AggregateHandler(
                    CounterAggregate.definition,
                    PostgresEventStore db.ConnectionString,
                    "counters",
                    processor
                )

            let aggregateIdA = Guid.NewGuid().ToString()

            let! _ =
                Assert.ThrowsAsync<InvalidOperationException>(fun () ->
                    handler.ExecuteAsync(
                        { AggregateId = aggregateIdA
                          Commands =
                            [| { Type = "Increment"
                                 Payload = json {| by = 5 |} } |] },
                        CounterAggregate.deserializeCommand,
                        CounterAggregate.deserializeEvent
                    ))

            let eventStore = PostgresEventStore db.ConnectionString :> IEventStore
            let! streamAEvents = eventStore.LoadAsync($"counters/{aggregateIdA}")
            let! streamBEvents = eventStore.LoadAsync(streamBId)

            Assert.Empty(streamAEvents)
            Assert.Empty(streamBEvents)
        }

// ── HTTP Layer Tests ─────────────────────────────────────────────────────────

type HttpLayerTests() =

    let db = TestDatabase()

    [<Fact>]
    member _.``Malformed json body returns 400``() =
        task {
            let builder = WebApplication.CreateBuilder()
            builder.WebHost.UseTestServer() |> ignore

            let handler =
                AggregateHandler(
                    CounterAggregate.definition,
                    { new IEventStore with
                        member _.AppendAsync(_, _, _, _, _) = raise (NotImplementedException())
                        member _.LoadAsync _ = raise (NotImplementedException()) },
                    "counters"
                )

            let app = builder.Build()

            app.MapAggregate(
                "counters",
                handler,
                CounterAggregate.deserializeCommand,
                CounterAggregate.deserializeEvent
            )
            |> ignore

            do! app.StartAsync()

            let client = app.GetTestClient()

            let! response =
                client.PostAsync(
                    "/counters/commands",
                    new StringContent("this is not json", Encoding.UTF8, "application/json")
                )

            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode)
            do! app.StopAsync()
        }

    [<Fact>]
    member _.``Get returns 404 when fold produces null``() =
        task {
            let aggregateId = Guid.NewGuid().ToString()
            let streamId = $"nullcounters/{aggregateId}"

            let store = PostgresEventStore db.ConnectionString
            let eventStore = store :> IEventStore
            let! _ = eventStore.AppendAsync(streamId, -1, [ ("CounterIncremented", """{"by":1}""") ], None, None)

            let nullDefinition =
                { Dispatch = CounterAggregate.dispatch
                  Apply = fun _ _ -> Unchecked.defaultof<CounterState> }

            let builder = WebApplication.CreateBuilder()
            builder.WebHost.UseTestServer() |> ignore

            let app = builder.Build()

            app.MapAggregate(
                "nullcounters",
                AggregateHandler(nullDefinition, store, "nullcounters"),
                CounterAggregate.deserializeCommand,
                CounterAggregate.deserializeEvent
            )
            |> ignore

            do! app.StartAsync()

            let client = app.GetTestClient()
            let! response = client.GetAsync $"/nullcounters/{aggregateId}"

            Assert.Equal(HttpStatusCode.NotFound, response.StatusCode)
            do! app.StopAsync()
        }

// ── Concurrency Tests ────────────────────────────────────────────────────────

type ConcurrencyTests() =

    let db = TestDatabase()

    [<Fact>]
    member _.``Concurrent writes to same aggregate via handler all succeed with retries``() =
        task {
            let aggregateId = Guid.NewGuid().ToString()

            let handler =
                AggregateHandler(
                    CounterAggregate.definition,
                    PostgresEventStore db.ConnectionString,
                    "counters",
                    maxRetries = 10
                )

            let tasks =
                [ for _ in 1 .. 5 ->
                    handler.ExecuteAsync(
                        { AggregateId = aggregateId
                          Commands = [| { Type = "Increment"; Payload = json {| by = 1 |} } |] },
                        CounterAggregate.deserializeCommand,
                        CounterAggregate.deserializeEvent
                    ) ]

            let! results = Task.WhenAll tasks
            results |> Array.iter (expectOk >> ignore)

            let store = PostgresEventStore db.ConnectionString :> IEventStore
            let! stored = store.LoadAsync $"counters/{aggregateId}"
            Assert.Equal(5, stored |> Seq.length)
        }

    [<Fact>]
    member _.``Concurrent writes to same stream without retries exactly one succeeds``() =
        task {
            let aggregateId = Guid.NewGuid().ToString()

            let handler =
                AggregateHandler(
                    CounterAggregate.definition,
                    PostgresEventStore db.ConnectionString,
                    "counters",
                    maxRetries = 0
                )

            let tasks =
                [ for _ in 1 .. 5 ->
                    handler.ExecuteAsync(
                        { AggregateId = aggregateId
                          Commands = [| { Type = "Increment"; Payload = json {| by = 1 |} } |] },
                        CounterAggregate.deserializeCommand,
                        CounterAggregate.deserializeEvent
                    ) ]

            let! results = Task.WhenAll tasks
            let successes = results |> Array.filter Result.isOk
            let failures  = results |> Array.filter Result.isError

            Assert.NotEmpty successes
            Assert.NotEmpty failures
            failures |> Array.iter (fun r -> Assert.Contains("Concurrency conflict", expectError r))

            let store = PostgresEventStore db.ConnectionString :> IEventStore
            let! stored = store.LoadAsync $"counters/{aggregateId}"
            Assert.Equal(successes.Length, stored |> Seq.length)
        }

    [<Fact>]
    member _.``Read immediately after write reflects appended events``() =
        task {
            let store = PostgresEventStore db.ConnectionString :> IEventStore
            let streamId = $"counters/{Guid.NewGuid()}"

            let! _ = store.AppendAsync(streamId, -1, [ ("CounterIncremented", """{"by":3}""") ], None, None)
            let! stored = store.LoadAsync streamId

            Assert.Single stored |> ignore
            Assert.Equal("CounterIncremented", stored.[0].EventType)
            Assert.Equal(0, stored.[0].Sequence)
        }

// ── Read Model Tests ─────────────────────────────────────────────────────────

type ReadModelTests() =

    let db = TestDatabase()

    [<Fact>]
    member _.``Concurrent commands each produce one read model entry``() =
        task {
            let testId = Guid.NewGuid().ToString()

            let processor =
                EventProcessor
                    [ EventReaction.on<CounterEvent> (fun e tx ->
                          task {
                              match e with
                              | CounterIncremented by ->
                                  let npgsqlTx = tx :?> NpgsqlTransaction

                                  do! npgsqlTx.Connection.ExecuteAsync(
                                          "INSERT INTO outbox (stream_id, event_type, payload, created_at) VALUES (@streamId, @eventType, @payload::jsonb, @createdAt)",
                                          {| streamId = $"{testId}/{Guid.NewGuid()}"
                                             eventType = "CounterIncremented"
                                             payload = $"""{{"by":{by}}}"""
                                             createdAt = DateTimeOffset.UtcNow |},
                                          npgsqlTx
                                      ) |> Ignore
                              | _ -> ()
                          }) ]

            let handler =
                AggregateHandler<CounterState, CounterEvent, CounterCommand>(
                    CounterAggregate.definition,
                    PostgresEventStore db.ConnectionString,
                    "counters",
                    processor
                )

            let tasks =
                [ for _ in  0 .. 20 - 1 ->
                    handler.ExecuteAsync(
                        { AggregateId = null
                          Commands =
                            [| { Type = "Increment"
                                 Payload = json {| by = 1 |} } |] },
                        CounterAggregate.deserializeCommand,
                        CounterAggregate.deserializeEvent
                    )]

            let! results = Task.WhenAll tasks
            results |> Array.iter (expectOk >> ignore)

            use conn = new NpgsqlConnection(db.ConnectionString)
            do! conn.OpenAsync()
            let! entryCount = conn.ExecuteScalarAsync<int>(
                "SELECT COUNT(*) FROM outbox WHERE stream_id LIKE @prefix",
                {| prefix = testId + "/%" |})
            Assert.Equal(20, entryCount)
        }
