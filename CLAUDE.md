# FSCommandFramework — Development Guide

---

## Running tests

Use `dotnet test` from the repo root, or run specific projects directly. Unit tests in `tests/FSCommandFramework.Tests` do not require a database.

---

## Publishing

Use `./publish-local.sh [patch|minor|major|x.y.z]` to bump the version, pack all three projects, and write `.nupkg` files to `./nupkgs`. Version is the single source of truth in `src/Directory.Build.props`.

---

## Architecture

```
FSCommandFramework.Core
├── AggregateDefinition<'State, 'Event, 'Command>  — pure record: Dispatch + Apply functions
├── Aggregate.fold                                  — replay events into state
├── StoredEvent                                     — raw event row: StreamId, Sequence, EventType, Payload, OccurredAt
├── IEventStore                                     — AppendAsync / LoadAsync
├── EventReaction                                   — EventType + Handle: run side effects in the same transaction
└── EventProcessor                                  — runs all matching reactions for each event

FSCommandFramework.Http
├── AggregateHandler<'State, 'Event, 'Command>  — loads stream, dispatches commands, appends events; retries on concurrency conflict (default 3)
├── CommandBatch / CommandEnvelope              — HTTP request model: aggregateId + commands[]
├── CommandSuccess / CommandFailure             — HTTP response models
├── ReflectionDeserializer                      — forCommands / forEvents using FSharp.SystemTextJson AdjacentTag
└── MapAggregate (IEndpointRouteBuilder)        — registers POST /{name}/commands and GET /{name}/{id}

FSCommandFramework.Postgres
└── PostgresEventStore : IEventStore            — append-only event store with optimistic concurrency via sequence check
```

Sample structure:

```
samples/FSCommandFramework.Sample/
├── Orders.fs       — OrderState, OrderEvent DU, OrderCommand DU, OrderAggregate, OrderSummariesReactions
└── Program.fs      — wiring: eventStore, eventProcessor, handler, MapAggregate
```

---

## Consumer docs

The published Claude guidance lives in `docs/claude/` and is copied into consuming projects on package restore via `buildTransitive` in the Core package. Edit those files when the public API or recommended patterns change.

- `docs/claude/claude.md` — consumer entrypoint: library overview and philosophy
- `docs/claude/fsharp-style.md` — F# API patterns (aggregate definition, reactions, wiring, HTTP API)
