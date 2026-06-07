namespace FSCommandFramework.Core

open System
open System.Threading.Tasks

[<CLIMutable>]
type StoredEvent =
    { StreamId: string
      Sequence: int
      EventType: string
      Payload: string
      OccurredAt: DateTimeOffset }

type IEventStore =

    abstract member AppendAsync:
        streamId: string *
        expectedSequence: int *
        events: (string * string) seq *
        processor: EventProcessor option *
        domainEvents: obj seq option ->
            Task<Result<StoredEvent list, string>>

    abstract member LoadAsync: streamId: string -> Task<StoredEvent list>

// ── DCB (Dynamic Consistency Boundary) types ──────────────────────────────────

type Tag = { Key: string; Value: string }

[<CLIMutable>]
type DcbStoredEvent =
    { Position: int64
      EventType: string
      Payload: string
      OccurredAt: DateTimeOffset }

type ReadResult =
    { Events: DcbStoredEvent list
      ConsistencyMarker: int64 }

type AppendCondition =
    { FailIfEventsMatch: Tag list
      After: int64 option }

type IDcbEventStore =

    abstract member ReadAsync: tags: Tag list -> Task<ReadResult>

    abstract member AppendAsync:
        events: (string * string * Tag list) list *
        condition: AppendCondition option ->
            Task<Result<unit, string>>
