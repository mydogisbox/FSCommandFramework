namespace FSCommandFramework.Http

open System.Text.Json
open System.Text.Json.Serialization

module ReflectionDeserializer =

    let private createOptions (userOptions: JsonSerializerOptions option) =
        let opts =
            match userOptions with
            | Some o -> JsonSerializerOptions o
            | None ->
                JsonSerializerOptions(
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                    PropertyNameCaseInsensitive = true
                )

        opts.Converters.Add(JsonFSharpConverter(JsonUnionEncoding.AdjacentTag ||| JsonUnionEncoding.NamedFields))

        opts

    let private wrap (typeName: string) (payload: string) =
        if payload = "{}" then
            $"""{{"Case":"{typeName}"}}"""
        else
            $"""{{"Case":"{typeName}","Fields":{payload}}}"""

    let forEvents<'Event> (options: JsonSerializerOptions option) : string -> string -> 'Event =
        let opts = createOptions options
        fun typeName payload -> JsonSerializer.Deserialize<'Event>(wrap typeName payload, opts)

    let forCommands<'Command> (options: JsonSerializerOptions option) : string -> JsonElement -> 'Command =
        let opts = createOptions options
        fun typeName element -> JsonSerializer.Deserialize<'Command>(wrap typeName (element.GetRawText()), opts)
