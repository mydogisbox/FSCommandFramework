namespace FSCommandFramework.Postgres

open System
open System.Data
open Dapper
open Npgsql
open FSCommandFramework.Core

type private DateTimeOffsetHandler() =
    inherit SqlMapper.TypeHandler<DateTimeOffset>()

    override _.SetValue(parameter, value) = parameter.Value <- box value

    override _.Parse value =
        match value with
        | :? DateTimeOffset as dto -> dto
        | :? DateTime as dt -> DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc))
        | _ -> invalidOp $"Cannot convert {value.GetType().Name} to DateTimeOffset"

type PostgresEventStore(connectionString: string) =

    static do SqlMapper.AddTypeHandler(DateTimeOffsetHandler())

    interface IEventStore with

        member _.AppendAsync(streamId, expectedSequence, events, processor, domainEvents) =
            task {
                use conn = new NpgsqlConnection(connectionString)
                do! conn.OpenAsync()
                let! tx = conn.BeginTransactionAsync IsolationLevel.ReadCommitted

                try
                    let! currentSequence =
                        conn.ExecuteScalarAsync<int>(
                            "SELECT COALESCE(MAX(sequence), -1) FROM events WHERE stream_id = @streamId",
                            {| streamId = streamId |},
                            tx
                        )

                    if currentSequence <> expectedSequence then
                        return
                            Error
                                $"Concurrency conflict on stream '{streamId}': expected sequence {expectedSequence}, actual {currentSequence}."
                    else
                        let stored = ResizeArray<StoredEvent>()
                        let mutable nextSequence = expectedSequence + 1

                        for eventType, payload in events do
                            let occurredAt = DateTimeOffset.UtcNow

                            let! _ =
                                conn.ExecuteAsync(
                                    "INSERT INTO events (stream_id, sequence, event_type, payload, occurred_at) VALUES (@streamId, @sequence, @eventType, @payload::jsonb, @occurredAt)",
                                    {| streamId = streamId
                                       sequence = nextSequence
                                       eventType = eventType
                                       payload = payload
                                       occurredAt = occurredAt |},
                                    tx
                                )

                            stored.Add
                                { StreamId = streamId
                                  Sequence = nextSequence
                                  EventType = eventType
                                  Payload = payload
                                  OccurredAt = occurredAt }

                            nextSequence <- nextSequence + 1

                        match processor, domainEvents with
                        | Some proc, Some evts -> do! proc.ProcessAsync(evts, tx)
                        | _ -> ()

                        do! tx.CommitAsync()
                        return Ok(stored |> Seq.toList)
                with
                | :? PostgresException as pgEx when pgEx.SqlState = "23505" ->
                    do! tx.RollbackAsync()
                    return Error $"Concurrency conflict on stream '{streamId}': a concurrent write committed first."
                | ex ->
                    do! tx.RollbackAsync()
                    return raise ex
            }

        member _.LoadAsync streamId =
            task {
                use conn = new NpgsqlConnection(connectionString)
                do! conn.OpenAsync()

                let! events =
                    conn.QueryAsync<StoredEvent>(
                        "SELECT stream_id AS StreamId, sequence AS Sequence, event_type AS EventType, payload AS Payload, occurred_at AS OccurredAt FROM events WHERE stream_id = @streamId ORDER BY sequence",
                        {| streamId = streamId |}
                    )

                return events |> Seq.toList
            }
