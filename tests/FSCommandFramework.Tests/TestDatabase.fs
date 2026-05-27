namespace FSCommandFramework.Tests

open Microsoft.Extensions.Configuration
open Npgsql
open Xunit

type TestDatabase() =

    let connectionString =
        ConfigurationBuilder().AddJsonFile("appsettings.json").Build().GetConnectionString "Postgres"

    member _.ConnectionString = connectionString

    interface IAsyncLifetime with
        member _.InitializeAsync() =
            task {
                use conn = new NpgsqlConnection(connectionString)
                do! conn.OpenAsync()
                use cmd = conn.CreateCommand()
                cmd.CommandText <- "TRUNCATE TABLE events, outbox;"
                let! _ = cmd.ExecuteNonQueryAsync()
                ()
            }

        member _.DisposeAsync() = task { () }

[<CollectionDefinition("Database")>]
type DatabaseCollection() =
    interface ICollectionFixture<TestDatabase>
