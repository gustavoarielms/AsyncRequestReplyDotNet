using System.Security.Cryptography;
using Microsoft.Extensions.Options;

namespace AsyncRequestReply.Internal;

internal sealed class InMemoryAsyncStatusStore : IAsyncStatusStore, IAsyncStatusTokenStore
{
    private sealed record Entry(
        AsyncStatusResponse? Status,
        byte[]? TokenHash,
        DateTimeOffset? ExpiresAt);

    private readonly Dictionary<string, Entry> entries = new();
    private readonly object writeLock = new();
    private readonly AsyncRequestReplyOptions options;

    public InMemoryAsyncStatusStore(IOptions<AsyncRequestReplyOptions> options)
    {
        this.options = options.Value;
    }

    public Task<AsyncStatusResponse?> GetAsync(string jobId, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (writeLock)
        {
            RemoveExpired(DateTimeOffset.UtcNow);

            return Task.FromResult(
                entries.TryGetValue(jobId, out var entry)
                    ? entry.Status
                    : null);
        }
    }

    public Task SetAsync(AsyncStatusResponse status, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (writeLock)
        {
            var now = DateTimeOffset.UtcNow;
            RemoveExpired(now);

            if (entries.TryGetValue(status.Id, out var entry))
            {
                entries[status.Id] = entry with { Status = status };
            }
            else
            {
                EnsureCapacity();
                entries[status.Id] = new Entry(status, null, null);
            }
        }

        return Task.CompletedTask;
    }

    public Task BeginRetentionAsync(
        string jobId,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (writeLock)
        {
            if (entries.TryGetValue(jobId, out var entry) && entry.ExpiresAt is null)
            {
                entries[jobId] = entry with
                {
                    ExpiresAt = DateTimeOffset.UtcNow.Add(options.StatusTimeToLive)
                };
            }
        }

        return Task.CompletedTask;
    }

    public Task DeleteAsync(string jobId, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (writeLock)
        {
            entries.Remove(jobId);
        }

        return Task.CompletedTask;
    }

    internal Task<bool> AdmitAsync(
        string jobId,
        string? accessToken,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (writeLock)
        {
            var now = DateTimeOffset.UtcNow;
            RemoveExpired(now);

            if (entries.ContainsKey(jobId))
            {
                return Task.FromResult(false);
            }

            EnsureCapacity();
            entries[jobId] = new Entry(
                StatusResponseFactory.Queued(jobId),
                accessToken is null ? null : StatusAccessToken.Hash(accessToken),
                null);

            return Task.FromResult(true);
        }
    }

    Task IAsyncStatusTokenStore.SetAsync(
        string jobId,
        string accessToken,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (writeLock)
        {
            var now = DateTimeOffset.UtcNow;
            RemoveExpired(now);
            var tokenHash = StatusAccessToken.Hash(accessToken);

            if (entries.TryGetValue(jobId, out var entry))
            {
                entries[jobId] = entry with { TokenHash = tokenHash };
            }
            else
            {
                EnsureCapacity();
                entries[jobId] = new Entry(null, tokenHash, null);
            }
        }

        return Task.CompletedTask;
    }

    Task<bool> IAsyncStatusTokenStore.ValidateAsync(
        string jobId,
        string accessToken,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (writeLock)
        {
            RemoveExpired(DateTimeOffset.UtcNow);

            return Task.FromResult(
                entries.TryGetValue(jobId, out var entry)
                && entry.TokenHash is not null
                && CryptographicOperations.FixedTimeEquals(
                    entry.TokenHash,
                    StatusAccessToken.Hash(accessToken)));
        }
    }

    Task IAsyncStatusTokenStore.DeleteAsync(
        string jobId,
        CancellationToken cancellationToken)
    {
        return DeleteAsync(jobId, cancellationToken);
    }

    Task IAsyncStatusTokenStore.BeginRetentionAsync(
        string jobId,
        CancellationToken cancellationToken)
    {
        return BeginRetentionAsync(jobId, cancellationToken);
    }

    private void EnsureCapacity()
    {
        if (entries.Count < options.StatusCapacity)
        {
            return;
        }

        var oldestRetained = entries
            .Where(pair => pair.Value.ExpiresAt is not null)
            .OrderBy(pair => pair.Value.ExpiresAt)
            .FirstOrDefault();

        if (oldestRetained.Equals(default(KeyValuePair<string, Entry>)))
        {
            throw new AsyncQueueUnavailableException(
                "The in-memory status store is full of active jobs.");
        }

        entries.Remove(oldestRetained.Key);
    }

    private void RemoveExpired(DateTimeOffset now)
    {
        foreach (var pair in entries.ToArray())
        {
            if (pair.Value.ExpiresAt is { } expiresAt && expiresAt <= now)
            {
                entries.Remove(pair.Key);
            }
        }
    }
}
