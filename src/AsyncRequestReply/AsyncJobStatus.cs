using System.Text.Json.Serialization;

namespace AsyncRequestReply;

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
