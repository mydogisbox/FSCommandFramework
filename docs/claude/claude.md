# FSCommandFramework

FSCommandFramework is an F# event sourcing library for ASP.NET Core APIs. It provides the plumbing for command/event/state aggregates — dispatching commands, storing events, replaying state, and reacting to events in the same transaction — so you can focus on domain logic.

---

## Philosophy

**Aggregates are pure functions.**

`dispatch` and `apply` are plain F# functions with no dependencies on infrastructure. They can be tested without a database, a web server, or any framework involvement. The framework wires them to storage and HTTP; your domain code never needs to know about either.

**Events are the source of truth.**

State is always derived by replaying events. There is no "update" path — commands produce events, and events are appended to an append-only stream. The current state of an aggregate is reconstructed on every command by loading its event stream and folding `apply` over it.

**Reactions run in the same transaction.**

Side effects — read model projections, denormalized summaries, cross-aggregate notifications — are registered as `EventReaction` handlers and run inside the same database transaction as the event append. Either everything commits or nothing does.

---

## Style guide

- F#: [fsharp-style.md](fsharp-style.md)
