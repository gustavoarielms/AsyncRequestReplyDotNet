using System.Collections.Concurrent;

namespace AsyncRequestReply.Internal;

internal sealed class InMemoryAsyncStatusStore : IAsyncStatusStore
{
    private readonly ConcurrentDictionary<string, AsyncStatusResponse> statuses = new();

    public Task<AsyncStatusResponse?> GetAsync(string jobId, CancellationToken cancellationToken = default)
    {
        statuses.TryGetValue(jobId, out var status);
        return Task.FromResult(status);
    }

    public Task SetAsync(AsyncStatusResponse status, CancellationToken cancellationToken = default)
    {
        statuses[status.Id] = status;
        return Task.CompletedTask;
    }
}
