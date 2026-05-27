open System
open System.Text.Json
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Http
open Npgsql
open FSCommandFramework.Core
open FSCommandFramework.Http
open FSCommandFramework.Postgres
open FSCommandFramework.Sample.Orders

let builder = WebApplication.CreateBuilder(Environment.GetCommandLineArgs())

let connectionString =
    builder.Configuration.GetSection("ConnectionStrings").["Postgres"]
    |> Option.ofObj
    |> Option.defaultWith (fun () -> invalidOp "Connection string 'Postgres' is not configured.")

let app = builder.Build()

let eventStore = PostgresEventStore(connectionString)
let eventProcessor = EventProcessor(OrderSummariesReactions.all)

let handler =
    AggregateHandler<OrderState, OrderEvent, OrderCommand>(
        OrderAggregate.definition,
        eventStore,
        "orders",
        eventProcessor
    )

app.MapAggregate(
    "orders",
    handler,
    ReflectionDeserializer.forCommands<OrderCommand> None,
    ReflectionDeserializer.forEvents<OrderEvent> None
)
|> ignore

app.MapGet(
    "/orders",
    RequestDelegate(fun ctx ->
        task {
            use conn = new NpgsqlConnection(connectionString)
            do! conn.OpenAsync()

            use cmd = conn.CreateCommand()

            cmd.CommandText <-
                "SELECT order_id, customer_id, status, items, placed_at, updated_at FROM order_summaries ORDER BY placed_at DESC"

            let summaries = ResizeArray<obj>()
            use! reader = cmd.ExecuteReaderAsync()

            while! reader.ReadAsync() do
                summaries.Add
                    {| orderId = reader.GetString 0
                       customerId = reader.GetString 1
                       status = reader.GetString 2
                       items = JsonSerializer.Deserialize<string list>(reader.GetString 3)
                       placedAt = reader.GetFieldValue<DateTimeOffset> 4
                       updatedAt = reader.GetFieldValue<DateTimeOffset> 5 |}

            do! Results.Ok(summaries).ExecuteAsync ctx
        })
)
|> ignore

app.Run()
