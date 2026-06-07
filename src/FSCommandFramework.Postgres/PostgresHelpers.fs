module FSCommandFramework.Postgres.PostgresHelpers

open System.Data
open Dapper
open Npgsql

/// Finds or creates a record using a double-checked advisory lock.
/// Fast path: run `query` without a lock. On miss: acquire a Postgres advisory lock,
/// re-check with the same query under the lock, and call `create` if still not found.
let findOrCreate
        (conn: NpgsqlConnection)
        (lockKey: int64)
        (query: string)
        (param: obj)
        (create: NpgsqlTransaction -> System.Threading.Tasks.Task<'a>)
        : System.Threading.Tasks.Task<'a> =
    task {
        if conn.State <> ConnectionState.Open then
            do! conn.OpenAsync()
        let! result = conn.QueryAsync<'a>(query, param)
        match result |> Seq.tryHead with
        | Some x -> return x
        | None ->
            use txn = conn.BeginTransaction()
            let! _ = conn.ExecuteAsync("SELECT pg_advisory_xact_lock(@lockKey)", {| lockKey = lockKey |}, txn)
            let! recheck = conn.QueryAsync<'a>(query, param, txn)
            match recheck |> Seq.tryHead with
            | Some x ->
                txn.Commit()
                return x
            | None ->
                let! result = create txn
                txn.Commit()
                return result
    }
