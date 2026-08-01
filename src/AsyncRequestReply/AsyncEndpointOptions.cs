namespace AsyncRequestReply;

public sealed class AsyncEndpointOptions
{
    public string? PayloadPath { get; set; }

    public AsyncExecutionMode ExecutionMode { get; set; } = AsyncExecutionMode.resolve_now;

    public long MaxPayloadBytes { get; set; } = 1_048_576;
}
