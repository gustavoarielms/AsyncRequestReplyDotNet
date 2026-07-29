using System.Collections.Concurrent;
using System.Security.Cryptography;
using Microsoft.Extensions.Options;

namespace AsyncRequestReply.Internal;

internal sealed class InMemoryAsyncStatusTokenStore : IAsyncStatusTokenStore
{
    private sealed record Entry(byte[] TokenHash, DateTimeOffset ExpiresAt);

    private readonly ConcurrentDictionary<string, Entry> tokens = new();
    private readonly object writeLock = new();
    private readonly AsyncRequestReplyOptions options;

    public InMemoryAsyncStatusTokenStore(IOptions<AsyncRequestReplyOptions> options)
    {
        this.options = options.Value;
    }

    public Task SetAsync(
        string jobId,
        string accessToken,
        CancellationToken cancellationToken = default)
    {
        lock (writeLock)
        {
            var now = DateTimeOffset.UtcNow;
            RemoveExpired(now);

            if (!tokens.ContainsKey(jobId) && tokens.Count >= options.StatusCapacity)
            {
                var oldest = tokens.MinBy(pair => pair.Value.ExpiresAt);

                if (!oldest.Equals(default(KeyValuePair<string, Entry>)))
                {
                    tokens.TryRemove(oldest.Key, out _);
                }
            }

            tokens[jobId] = new Entry(
                StatusAccessToken.Hash(accessToken),
                now.Add(options.StatusTimeToLive));
        }

        return Task.CompletedTask;
    }

    public Task<bool> ValidateAsync(
        string jobId,
        string accessToken,
        CancellationToken cancellationToken = default)
    {
        if (!tokens.TryGetValue(jobId, out var entry))
        {
            return Task.FromResult(false);
        }

        if (entry.ExpiresAt <= DateTimeOffset.UtcNow)
        {
            tokens.TryRemove(jobId, out _);
            return Task.FromResult(false);
        }

        return Task.FromResult(
            CryptographicOperations.FixedTimeEquals(
                entry.TokenHash,
                StatusAccessToken.Hash(accessToken)));
    }

    public Task DeleteAsync(string jobId, CancellationToken cancellationToken = default)
    {
        tokens.TryRemove(jobId, out _);
        return Task.CompletedTask;
    }

    public Task RefreshAsync(string jobId, CancellationToken cancellationToken = default)
    {
        if (tokens.TryGetValue(jobId, out var entry))
        {
            tokens[jobId] = entry with
            {
                ExpiresAt = DateTimeOffset.UtcNow.Add(options.StatusTimeToLive)
            };
        }

        return Task.CompletedTask;
    }

    private void RemoveExpired(DateTimeOffset now)
    {
        foreach (var pair in tokens)
        {
            if (pair.Value.ExpiresAt <= now)
            {
                tokens.TryRemove(pair.Key, out _);
            }
        }
    }
}
