# FSCommandFramework.Tests

Integration tests for FSCommandFramework. All tests require a running PostgreSQL database.

---

## Running tests

```sh
dotnet test tests/FSCommandFramework.Tests
```

Set the connection string in `appsettings.json`:

```json
{
  "ConnectionStrings": {
    "Postgres": "Host=localhost;Database=fscommandframework_test;Username=postgres;Password=..."
  }
}
```

The database must have the `events` and `outbox` tables. See the `Migrations/` directory in the sample project for the schema.

---

## Database lifecycle

`TestDatabase` implements `IAsyncLifetime` and truncates `events` and `outbox` at the start of each test class via `InitializeAsync`. Tests are grouped into xUnit collections (`[<Collection("Database")>]`) which share the fixture but each class gets a fresh truncation.

When adding a new test class that needs a clean database:

```fsharp
[<Collection("Database")>]
type MyTests() =

    let db = TestDatabase()

    interface IAsyncLifetime with
        member _.InitializeAsync() = (db :> IAsyncLifetime).InitializeAsync()
        member _.DisposeAsync() = task { () }
```

---

## Test aggregate

`CounterAggregate` is the shared test fixture — a simple counter with `Increment`, `Decrement`, and `Reset` commands. It lives in `CounterAggregate.fs` and is reused across all test collections. Do not add domain-specific logic to it; add a new aggregate type if a test needs different behavior.

---

## Helpers

`Helpers.json` serializes an anonymous record to `JsonElement` with camelCase naming — used to build `CommandEnvelope.Payload`:

```fsharp
{ Type = "Increment"; Payload = json {| by = 5 |} }
```

`expectOk` and `expectError` unwrap `Result` values and fail the test with a clear message if the wrong case is returned.

---

## Test collections

| Collection | File section | What it covers |
|---|---|---|
| `CounterTests` | basic command dispatch | increment, decrement, reset, batch, batch rollback |
| `EventProcessorTests` | reactions | outbox write in same tx, rollback with failed append |
| `EdgeCaseTests` | infrastructure guarantees | throwing reaction, zero events, unknown command, malformed payload, intra-batch conflict, optimistic concurrency, cross-aggregate atomic writes |
| `HttpLayerTests` | HTTP layer via `TestHost` | malformed JSON body (400), GET returns 404 when fold produces null |
| `ReadModelTests` | concurrent writes + reactions | 20 concurrent commands each produce one read model entry |

---

## Testing patterns

**Direct `AggregateHandler` (most tests)** — construct `AggregateHandler` directly with `PostgresEventStore`, call `ExecuteAsync`, inspect the result or load events to verify state. Pass `maxRetries = 0` to disable retry behaviour in concurrency tests:

```fsharp
let handler =
    AggregateHandler(
        CounterAggregate.definition,
        PostgresEventStore db.ConnectionString,
        "counters",
        maxRetries = 0)   // disable retries when testing concurrency conflict behaviour

let! result =
    handler.ExecuteAsync(
        { AggregateId = null
          Commands = [| { Type = "Increment"; Payload = json {| by = 5 |} } |] },
        CounterAggregate.deserializeCommand,
        CounterAggregate.deserializeEvent)

expectOk result |> ignore
```

**`TestHost` (HTTP layer tests)** — use `Microsoft.AspNetCore.TestHost` to spin up a minimal `WebApplication` with `MapAggregate` and send real HTTP requests:

```fsharp
let builder = WebApplication.CreateBuilder()
builder.WebHost.UseTestServer() |> ignore
let app = builder.Build()
app.MapAggregate("counters", handler, ...) |> ignore
do! app.StartAsync()
let client = app.GetTestClient()
let! response = client.PostAsync("/counters/commands", ...)
Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode)
do! app.StopAsync()
```

Use the `TestHost` path only when testing HTTP-level behavior (status codes, request parsing). Test domain and infrastructure logic directly through `AggregateHandler`.

**Custom `IEventStore`** — pass an inline implementation when testing HTTP behavior that shouldn't touch the database:

```fsharp
{ new IEventStore with
    member _.AppendAsync(_, _, _, _, _) = raise (NotImplementedException())
    member _.LoadAsync _ = raise (NotImplementedException()) }
```
