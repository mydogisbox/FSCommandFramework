#!/usr/bin/env dotnet-fsi

#r "nuget: Npgsql, 9.0.3"

open System
open Npgsql

let connectionString =
    let config =
        System.IO.File.ReadAllText(
            System.IO.Path.Combine(__SOURCE_DIRECTORY__, "../tests/FSCommandFramework.Tests/appsettings.json")
        )
        |> System.Text.Json.JsonDocument.Parse

    config
        .RootElement
        .GetProperty("ConnectionStrings")
        .GetProperty("Postgres")
        .GetString()

let conn = new NpgsqlConnection(connectionString)
conn.Open()

let cmd = conn.CreateCommand()
cmd.CommandText <- "SELECT id, stream_id, sequence, event_type, occurred_at, payload FROM events ORDER BY id"

let reader = cmd.ExecuteReader()

printfn "%-5s %-40s %-8s %-20s %-30s %s" "id" "stream_id" "seq" "event_type" "occurred_at" "payload"
printfn "%s" (String.replicate 140 "-")

while reader.Read() do
    printfn
        "%-5d %-40s %-8d %-20s %-30s %s"
        (reader.GetInt64(0))
        (reader.GetString(1))
        (reader.GetInt32(2))
        (reader.GetString(3))
        (reader
            .GetFieldValue<DateTimeOffset>(4)
            .ToString("u"))
        (reader.GetString(5))
