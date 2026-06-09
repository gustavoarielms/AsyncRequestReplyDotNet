namespace Patxa.AsyncRequestReply;

public interface IAsyncJobProcessor
{
    Task<object?> ProcessAsync(string jobId, object? payload, CancellationToken cancellationToken = default);
}
