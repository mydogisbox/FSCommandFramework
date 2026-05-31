namespace FSCommandFramework.Http

open System.Text.Json
open System.Threading.Tasks
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Http
open Microsoft.AspNetCore.Routing
open FSCommandFramework.Core

[<AutoOpen>]
module WebApplicationExtensions =

    let private jsonOptions = JsonSerializerOptions(JsonSerializerDefaults.Web)

    let private writeJson (ctx: HttpContext) (value: obj) =
        ctx.Response.ContentType <- "application/json; charset=utf-8"
        ctx.Response.WriteAsync(JsonSerializer.Serialize(value, jsonOptions))

    type IEndpointRouteBuilder with

        member app.MapAggregate<'State, 'Event, 'Command>
            (
                name: string,
                handler: AggregateHandler<'State, 'Event, 'Command>,
                deserializeCommand: string -> JsonElement -> 'Command,
                deserializeEvent: string -> string -> 'Event
            ) =

            app.MapPost(
                $"/{name}/commands",
                RequestDelegate(fun ctx ->
                    task {
                        try
                            let! batch = ctx.Request.ReadFromJsonAsync<CommandBatch>()

                            if isNull (box batch) || isNull (box batch.Commands) || batch.Commands.Length = 0 then
                                do! Results.BadRequest("Batch must contain at least one command.").ExecuteAsync ctx
                            else
                                let! result = handler.ExecuteAsync(batch, deserializeCommand, deserializeEvent)

                                match result with
                                | Ok value -> do! writeJson ctx value
                                | Error error ->
                                    ctx.Response.StatusCode <- 422
                                    do! writeJson ctx { Error = error }
                        with :? JsonException ->
                            ctx.Response.StatusCode <- 400
                    })
            )
            |> ignore

            app.MapGet(
                $"/{name}/{{aggregateId}}",
                RequestDelegate(fun ctx ->
                    task {
                        let aggregateId = ctx.GetRouteValue "aggregateId" :?> string
                        let! stored = handler.EventStore.LoadAsync $"{name}/{aggregateId}"

                        if List.isEmpty stored then
                            do! Results.NotFound().ExecuteAsync ctx
                        else
                            let state =
                                Aggregate.fold
                                    (stored |> Seq.map (fun e -> deserializeEvent e.EventType e.Payload))
                                    handler.Apply

                            match state with
                            | Some s when not (isNull (box s)) -> do! writeJson ctx s
                            | _ -> do! Results.NotFound().ExecuteAsync ctx
                    })
            )
            |> ignore

            app
