namespace Patxa.AsyncRequestReply;

public interface IAsyncJobQueue
{
    ValueTask EnqueueAsync(string jobId, object? payload, AsyncExecutionMode executionMode, CancellationToken cancellationToken = default);
}
