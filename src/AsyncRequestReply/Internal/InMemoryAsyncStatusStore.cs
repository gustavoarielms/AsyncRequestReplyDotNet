using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace AsyncRequestReply.Internal;

internal sealed class InMemoryAsyncStatusStore : IAsyncStatusStore
{
    private sealed record Entry(AsyncStatusResponse Status, DateTimeOffset? ExpiresAt);

    private readonly ConcurrentDictionary<string, Entry> statuses = new();
    private readonly object writeLock = new();
    private readonly AsyncRequestReplyOptions options;

    public InMemoryAsyncStatusStore(IOptions<AsyncRequestReplyOptions> options)
    {
        this.options = options.Value;
    }

    public Task<AsyncStatusResponse?> GetAsync(string jobId, CancellationToken cancellationToken = default)
    {
        if (!statuses.TryGetValue(jobId, out var entry))
        {
            return Task.FromResult<AsyncStatusResponse?>(null);
        }

        if (entry.ExpiresAt is { } expiresAt && expiresAt <= DateTimeOffset.UtcNow)
        {
            statuses.TryRemove(jobId, out _);
            return Task.FromResult<AsyncStatusResponse?>(null);
        }

        return Task.FromResult<AsyncStatusResponse?>(entry.Status);
    }

    public Task SetAsync(AsyncStatusResponse status, CancellationToken cancellationToken = default)
    {
        lock (writeLock)
        {
            var now = DateTimeOffset.UtcNow;
            RemoveExpired(now);

            if (!statuses.ContainsKey(status.Id) && statuses.Count >= options.StatusCapacity)
            {
                var oldest = statuses.MinBy(pair => pair.Value.Status.UpdatedAt);

                if (!oldest.Equals(default(KeyValuePair<string, Entry>)))
                {
                    statuses.TryRemove(oldest.Key, out _);
                }
            }

            statuses[status.Id] = new Entry(status, null);
        }

        return Task.CompletedTask;
    }

    public Task BeginRetentionAsync(
        string jobId,
        CancellationToken cancellationToken = default)
    {
        lock (writeLock)
        {
            if (statuses.TryGetValue(jobId, out var entry))
            {
                statuses[jobId] = entry with
                {
                    ExpiresAt = DateTimeOffset.UtcNow.Add(options.StatusTimeToLive)
                };
            }
        }

        return Task.CompletedTask;
    }

    public Task DeleteAsync(string jobId, CancellationToken cancellationToken = default)
    {
        statuses.TryRemove(jobId, out _);
        return Task.CompletedTask;
    }

    private void RemoveExpired(DateTimeOffset now)
    {
        foreach (var pair in statuses)
        {
            if (pair.Value.ExpiresAt is { } expiresAt && expiresAt <= now)
            {
                statuses.TryRemove(pair.Key, out _);
            }
        }
    }
}
