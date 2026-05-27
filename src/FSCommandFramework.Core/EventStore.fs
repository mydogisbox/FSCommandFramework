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
