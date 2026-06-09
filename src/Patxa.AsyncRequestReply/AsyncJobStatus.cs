using System.Text.Json.Serialization;

namespace Patxa.AsyncRequestReply;

[JsonConverter(typeof(JsonStringEnumConverter<AsyncJobStatus>))]
public enum AsyncJobStatus
{
    queued,
    processing,
    waiting_external,
    completed,
    failed,
    not_found
}
