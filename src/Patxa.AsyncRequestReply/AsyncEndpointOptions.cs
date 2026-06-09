namespace Patxa.AsyncRequestReply;

public sealed class AsyncEndpointOptions
{
    public string? PayloadPath { get; set; }

    public AsyncExecutionMode ExecutionMode { get; set; } = AsyncExecutionMode.resolve_now;
}
