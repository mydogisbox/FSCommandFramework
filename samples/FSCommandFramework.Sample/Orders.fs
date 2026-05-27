namespace FSCommandFramework.Sample.Orders

open System
open System.Text.Json
open System.Threading.Tasks
open Dapper
open Npgsql
open FSCommandFramework.Core

// ── State ────────────────────────────────────────────────────────────────────

type OrderState =
    { Id: string
      CustomerId: string
      Status: string
      Items: string list }

// ── Events ───────────────────────────────────────────────────────────────────

type OrderEvent =
    | OrderPlaced of orderId: string * customerId: string * items: string list
    | OrderCancelled of orderId: string * reason: string
    | OrderShipped of orderId: string * trackingNumber: string

// ── Commands ─────────────────────────────────────────────────────────────────

type OrderCommand =
    | PlaceOrder of customerId: string * items: string list
    | CancelOrder of aggregateId: string * reason: string
    | ShipOrder of aggregateId: string * trackingNumber: string

// ── Aggregate ────────────────────────────────────────────────────────────────

module OrderAggregate =

    let private knownCustomers = Set.ofList [ "cust-1"; "cust-2"; "cust-3" ]

    let dispatch (state: OrderState option) (command: OrderCommand) : Result<OrderEvent list, string> =
        match command with
        | PlaceOrder(customerId, items) ->
            match state with
            | Some _ -> Error "Order has already been placed."
            | None ->
                if not (Set.contains customerId knownCustomers) then
                    Error $"Customer '{customerId}' does not exist."
                elif List.isEmpty items then
                    Error "An order must contain at least one item."
                else
                    Ok [ OrderPlaced(Guid.NewGuid().ToString(), customerId, items) ]

        | CancelOrder(_, reason) ->
            match state with
            | None -> Error "Order does not exist."
            | Some s when s.Status = "cancelled" ->
                    Error $"Order '{s.Id}' has already been cancelled."
            | Some s when s.Status = "shipped" ->
                    Error $"Order '{s.Id}' has already been shipped and cannot be cancelled."
            | Some s ->
                    Ok [ OrderCancelled(s.Id, reason) ]

        | ShipOrder(_, trackingNumber) ->
            match state with
            | None -> Error "Order does not exist."
            | Some s when s.Status = "cancelled" ->
                    Error $"Order '{s.Id}' has been cancelled and cannot be shipped."
            | Some s when s.Status = "shipped" ->
                    Error $"Order '{s.Id}' has already been shipped."
            | Some s ->
                    Ok [ OrderShipped(s.Id, trackingNumber) ]

    let apply (state: OrderState option) (event: OrderEvent) : OrderState =
        match event with
        | OrderPlaced(orderId, customerId, items) ->
            { Id = orderId
              CustomerId = customerId
              Status = "placed"
              Items = items }
        | OrderCancelled _ ->
            { state.Value with
                Status = "cancelled" }
        | OrderShipped _ -> { state.Value with Status = "shipped" }

    let definition = { Dispatch = dispatch; Apply = apply }

// ── Reactions ────────────────────────────────────────────────────────────────

module OrderSummariesReactions =

    let private jsonOptions =
        JsonSerializerOptions(PropertyNamingPolicy = JsonNamingPolicy.CamelCase)

    let private Ignore(t: Task<_>) = (t :> Task)

    let all: EventReaction list =
        [ EventReaction.on<OrderEvent> (fun e tx ->
              task {
                  let conn = (tx :?> NpgsqlTransaction).Connection

                  match e with
                  | OrderPlaced(orderId, customerId, items) ->
                      do! conn.ExecuteAsync(
                              "INSERT INTO order_summaries (order_id, customer_id, status, items, placed_at, updated_at) VALUES (@orderId, @customerId, 'placed', @items::jsonb, @now, @now) ON CONFLICT (order_id) DO NOTHING",
                              {| orderId = orderId
                                 customerId = customerId
                                 items = JsonSerializer.Serialize(items, jsonOptions)
                                 now = DateTimeOffset.UtcNow |},
                              tx
                            ) |> Ignore

                  | OrderCancelled(orderId, _) ->
                      do! conn.ExecuteAsync(
                              "UPDATE order_summaries SET status = 'cancelled', updated_at = @now WHERE order_id = @orderId",
                              {| orderId = orderId
                                 now = DateTimeOffset.UtcNow |},
                              tx
                          ) |> Ignore

                  | OrderShipped(orderId, _) ->
                      do! conn.ExecuteAsync(
                              "UPDATE order_summaries SET status = 'shipped', updated_at = @now WHERE order_id = @orderId",
                              {| orderId = orderId
                                 now = DateTimeOffset.UtcNow |},
                              tx
                          ) |> Ignore
              }) ]
