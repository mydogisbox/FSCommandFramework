namespace FSCommandFramework.Tests

open Microsoft.Extensions.Configuration

type TestDatabase() =

    let connectionString =
        ConfigurationBuilder().AddJsonFile("appsettings.json").Build().GetConnectionString "Postgres"

    member _.ConnectionString = connectionString
