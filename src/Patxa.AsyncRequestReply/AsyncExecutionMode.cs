using System.Text.Json.Serialization;

namespace Patxa.AsyncRequestReply;

[JsonConverter(typeof(JsonStringEnumConverter<AsyncExecutionMode>))]
public enum AsyncExecutionMode
{
    resolve_now,
    wait_external
}
