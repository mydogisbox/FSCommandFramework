# F# style

_Current version: 1.1.10._

---

## Aggregate definition

An aggregate is defined by three types and two functions:

```fsharp
// State — the current snapshot of the aggregate
type OrderState =
    { Id: string; CustomerId: string; Status: string; Items: string list }

// Events — things that happened (discriminated union)
type OrderEvent =
    | OrderPlaced   of orderId: string * customerId: string * items: string list
    | OrderShipped  of orderId: string * trackingNumber: string
    | OrderCancelled of orderId: string * reason: string

// Commands — intentions (discriminated union)
type OrderCommand =
    | PlaceOrder  of customerId: string * items: string list
    | ShipOrder   of aggregateId: string * trackingNumber: string
    | CancelOrder of aggregateId: string * reason: string
```

`dispatch` validates the command against current state and returns the resulting events or an error:

```fsharp
let dispatch (state: OrderState option) (command: OrderCommand) : Result<OrderEvent list, string> =
    match command with
    | PlaceOrder(customerId, items) ->
        match state with
        | Some _ -> Error "Order has already been placed."
        | None   -> Ok [ OrderPlaced(Guid.NewGuid().ToString(), customerId, items) ]
    | ShipOrder(_, trackingNumber) ->
        match state with
        | None                             -> Error "Order does not exist."
        | Some s when s.Status = "shipped" -> Error $"Order '{s.Id}' has already been shipped."
        | Some s                           -> Ok [ OrderShipped(s.Id, trackingNumber) ]
    ...
```

`apply` folds an event into state — always returns the new state, never fails:

```fsharp
let apply (state: OrderState option) (event: OrderEvent) : OrderState =
    match event with
    | OrderPlaced(orderId, customerId, items) ->
        { Id = orderId; CustomerId = customerId; Status = "placed"; Items = items }
    | OrderShipped _ ->
        { state.Value with Status = "shipped" }
    | OrderCancelled _ ->
        { state.Value with Status = "cancelled" }
```

Wrap them in an `AggregateDefinition`:

```fsharp
let definition = { Dispatch = dispatch; Apply = apply }
```

---

## Event serialization

Events are stored using `FSharp.SystemTextJson` with `AdjacentTag` encoding — each event is written as `{ "Case": "EventName", "Fields": { ... } }`. The `ReflectionDeserializer` module handles round-tripping automatically.

Discriminated union cases with named fields serialize naturally:

```fsharp
| OrderPlaced of orderId: string * customerId: string * items: string list
// stored as: { "Case": "OrderPlaced", "Fields": { "orderId": "...", "customerId": "...", "items": [...] } }

| OrderCancelled of orderId: string * reason: string
// stored as: { "Case": "OrderCancelled", "Fields": { "orderId": "...", "reason": "..." } }
```

Cases with no fields serialize as `{ "Case": "EventName" }` with no `Fields` property.

Use named fields on all DU cases — positional tuples serialize as arrays and are harder to extend.

---

## Wiring up

```fsharp
let eventStore     = PostgresEventStore(connectionString)
let eventProcessor = EventProcessor(MyReactions.all)    // optional — omit if no reactions

let handler =
    AggregateHandler<OrderState, OrderEvent, OrderCommand>(
        OrderAggregate.definition,
        eventStore,
        "orders",            // aggregate name — used as stream prefix and HTTP route prefix
        eventProcessor,      // optional
        maxRetries = 3)      // optional — retries on concurrency conflict, default 3

app.MapAggregate(
    "orders",
    handler,
    ReflectionDeserializer.forCommands<OrderCommand> None,
    ReflectionDeserializer.forEvents<OrderEvent> None)
```

`MapAggregate` registers two endpoints:
- `POST /{name}/commands` — accepts a `CommandBatch`, dispatches each command in sequence
- `GET  /{name}/{aggregateId}` — loads and returns current state

---

## Command batch HTTP API

`POST /orders/commands` accepts a JSON batch:

```json
{
  "aggregateId": "",
  "commands": [
    { "type": "PlaceOrder", "payload": { "customerId": "cust-1", "items": ["widget", "gadget"] } }
  ]
}
```

- `aggregateId` — leave empty to auto-generate a new ID; supply an existing ID to append to that stream
- `commands` — array of `{ type, payload }` envelopes; dispatched in order, stopping on the first error
- `type` — the DU case name exactly as declared in F#

On success returns `200` with a list of `CommandSuccess`:

```json
[{ "index": 0, "aggregateId": "abc-123", "events": ["OrderPlaced"] }]
```

On validation failure returns `422` with `{ "error": "..." }`.

---

## Event reactions

Reactions run inside the same database transaction as the event append. Register them with `EventReaction.on<'Event>`:

```fsharp
module OrderReactions =

    let all: EventReaction list =
        [ EventReaction.on<OrderEvent> (fun event tx ->
              task {
                  let conn = (tx :?> NpgsqlTransaction).Connection
                  match event with
                  | OrderPlaced(orderId, customerId, _) ->
                      do! conn.ExecuteAsync(
                              "INSERT INTO order_summaries (order_id, customer_id, status) VALUES (@orderId, @customerId, 'placed')",
                              {| orderId = orderId; customerId = customerId |}, tx) |> ignore'
                  | OrderShipped(orderId, _) ->
                      do! conn.ExecuteAsync(
                              "UPDATE order_summaries SET status = 'shipped' WHERE order_id = @orderId",
                              {| orderId = orderId |}, tx) |> ignore'
                  | _ -> ()
              }) ]
```

`EventReaction.on<'Event>` matches on the exact event type. If multiple reaction registrations are provided, all matching ones run for each event.

Pass the `EventProcessor` to `AggregateHandler` — if omitted, no reactions run:

```fsharp
let eventProcessor = EventProcessor(OrderReactions.all)
let handler = AggregateHandler(definition, eventStore, "orders", eventProcessor)
```

---

## Custom deserializers

Pass `None` to `ReflectionDeserializer` to use the defaults (camelCase, case-insensitive, `AdjacentTag` union encoding). Pass `Some opts` to override:

```fsharp
let opts = JsonSerializerOptions(PropertyNamingPolicy = JsonNamingPolicy.CamelCase)
opts.Converters.Add(JsonFSharpConverter(JsonUnionEncoding.AdjacentTag ||| JsonUnionEncoding.NamedFields))

app.MapAggregate(
    "orders",
    handler,
    ReflectionDeserializer.forCommands<OrderCommand> (Some opts),
    ReflectionDeserializer.forEvents<OrderEvent> (Some opts))
```

---

## Concurrency

`PostgresEventStore` uses optimistic concurrency. The current sequence number of the stream is checked inside the transaction:

- If it matches `expectedSequence`, the new events are appended
- If it doesn't match, `AppendAsync` returns `Error "Concurrency conflict on stream '...': expected N, actual M."`

The `AggregateHandler` loads the stream, records the current sequence, then passes it to `AppendAsync`. On a conflict it reloads the stream, re-folds state, re-dispatches all commands, and retries the append — up to `maxRetries` times (default 3). If all retries are exhausted the conflict error surfaces as a `422` response.

---

## PostgreSQL schema

The `events` table must exist before the application starts:

```sql
CREATE TABLE events (
    stream_id   TEXT        NOT NULL,
    sequence    INTEGER     NOT NULL,
    event_type  TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (stream_id, sequence)
);
```

The `PostgresEventStore` reads and writes this table directly via Dapper. No migrations are managed by the framework — create and evolve the schema yourself.
