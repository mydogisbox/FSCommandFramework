namespace FSCommandFramework.Postgres

open System
open System.Data
open Dapper
open Npgsql
open FSCommandFramework.Core

type PostgresDcbEventStore(connectionString: string) =

    static do SqlMapper.AddTypeHandler(DateTimeOffsetHandler())

    let buildTagJoins (tags: Tag list) =
        tags
        |> List.mapi (fun i _ ->
            $"INNER JOIN event_tags t{i} ON t{i}.position = e.position AND t{i}.key = @k{i} AND t{i}.value = @v{i}")
        |> String.concat "\n"

    let buildTagParams (tags: Tag list) (extra: (string * obj) list) =
        let dp = DynamicParameters()
        tags |> List.iteri (fun i t ->
            dp.Add($"k{i}", t.Key)
            dp.Add($"v{i}", t.Value))
        extra |> List.iter (fun (k, v) -> dp.Add(k, v))
        dp

    // Deterministic 64-bit key for pg_advisory_xact_lock, derived from condition tags.
    // Serialises only writers that share the same DCB boundary (same userId or formId),
    // leaving unrelated transactions completely uncontended.
    let advisoryLockKey (tags: Tag list) =
        let str =
            tags
            |> List.sortBy (fun t -> t.Key, t.Value)
            |> List.map (fun t -> $"{t.Key}={t.Value}")
            |> String.concat "|"
        use sha = System.Security.Cryptography.SHA256.Create()
        let bytes = sha.ComputeHash(System.Text.Encoding.UTF8.GetBytes(str))
        System.BitConverter.ToInt64(bytes, 0)

    interface IDcbEventStore with

        member _.ReadAsync(tags) =
            task {
                use conn = new NpgsqlConnection(connectionString)
                do! conn.OpenAsync()

                let joins = buildTagJoins tags
                let sql =
                    $"""SELECT e.position AS Position, e.event_type AS EventType, e.payload::text AS Payload, e.occurred_at AS OccurredAt
FROM events e
{joins}
ORDER BY e.position"""

                let dp = buildTagParams tags []
                let! rows = conn.QueryAsync<DcbStoredEvent>(sql, dp)
                let events = rows |> Seq.toList

                let marker =
                    match events with
                    | [] -> -1L
                    | _  -> events |> List.map (fun e -> e.Position) |> List.max

                return { Events = events; ConsistencyMarker = marker }
            }

        member _.AppendAsync(events, condition) =
            task {
                use conn = new NpgsqlConnection(connectionString)
                do! conn.OpenAsync()
                let! tx = conn.BeginTransactionAsync(IsolationLevel.ReadCommitted)

                try
                    // Acquire an advisory lock keyed on the condition's tags before reading.
                    // This serialises all writers sharing the same DCB boundary (e.g. same
                    // userId or formId) without contending with unrelated transactions,
                    // making the read-check-insert sequence atomic for that boundary.
                    match condition with
                    | Some cond ->
                        let lockKey = advisoryLockKey cond.FailIfEventsMatch
                        let! _ = conn.ExecuteAsync("SELECT pg_advisory_xact_lock(@k)", {| k = lockKey |}, tx)
                        ()
                    | None -> ()

                    let! hasConflict =
                        match condition with
                        | None -> task { return false }
                        | Some cond ->
                            task {
                                let marker = cond.After |> Option.defaultValue -1L
                                let joins  = buildTagJoins cond.FailIfEventsMatch
                                let checkSql =
                                    $"""SELECT 1 FROM events e
{joins}
WHERE e.position > @marker
LIMIT 1"""
                                let dp = buildTagParams cond.FailIfEventsMatch [("marker", box marker)]
                                let! conflict = conn.ExecuteScalarAsync<int>(checkSql, dp, tx)
                                return conflict > 0
                            }

                    if hasConflict then
                        do! tx.RollbackAsync()
                        return Error "Concurrency conflict: matching events were appended concurrently."
                    else
                        for (eventType, payload, tags) in events do
                            let occurredAt = DateTimeOffset.UtcNow
                            let! pos =
                                conn.ExecuteScalarAsync<int64>(
                                    "INSERT INTO events (event_type, payload, occurred_at) VALUES (@eventType, @payload::jsonb, @occurredAt) RETURNING position",
                                    {| eventType = eventType; payload = payload; occurredAt = occurredAt |},
                                    tx)
                            for t in tags do
                                let! _ =
                                    conn.ExecuteAsync(
                                        "INSERT INTO event_tags (position, key, value) VALUES (@pos, @key, @value)",
                                        {| pos = pos; key = t.Key; value = t.Value |},
                                        tx)
                                ()

                        do! tx.CommitAsync()
                        return Ok ()
                with ex ->
                    try do! tx.RollbackAsync() with _ -> ()
                    return raise ex
            }
