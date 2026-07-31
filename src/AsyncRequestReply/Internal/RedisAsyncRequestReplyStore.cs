using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace AsyncRequestReply.Internal;

internal sealed class RedisAsyncRequestReplyStore(
    IConnectionMultiplexer connection,
    IOptions<RedisAsyncRequestReplyOptions> redisOptions,
    IOptions<AsyncRequestReplyOptions> requestReplyOptions,
    ILogger<RedisAsyncRequestReplyStore> logger) :
    IAsyncJobQueue,
    IAsyncJobQueueReader,
    IAsyncJobSubmissionStore,
    IAsyncStatusStore,
    IAsyncStatusTokenStore,
    IAsyncJobTerminalStore
{
    private const string JobField = "job";
    private const string EnqueueScript = """
        if redis.call('XLEN', KEYS[1]) >= tonumber(ARGV[1]) then
            return nil
        end
        return redis.call('XADD', KEYS[1], '*', 'job', ARGV[2])
        """;
    private const string SubmitScript = """
        if redis.call('EXISTS', KEYS[2]) == 1 then
            return 1
        end
        if redis.call('XLEN', KEYS[1]) >= tonumber(ARGV[1]) then
            return 0
        end
        redis.call('XADD', KEYS[1], '*', 'job', ARGV[2])
        redis.call('SET', KEYS[2], ARGV[3])
        if ARGV[4] == '1' then
            redis.call('SET', KEYS[3], ARGV[5])
        end
        return 1
        """;
    private const string CompleteScript = """
        local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[3], ARGV[3], 1)
        if #pending == 0 or pending[1][2] ~= ARGV[2] then
            return 0
        end
        redis.call('XACK', KEYS[1], ARGV[1], ARGV[3])
        redis.call('XDEL', KEYS[1], ARGV[3])
        return 1
        """;
    private const string CompleteTerminalScript = """
        local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[3], ARGV[3], 1)
        if #pending == 0 or pending[1][2] ~= ARGV[2] then
            return 0
        end
        redis.call('SET', KEYS[2], ARGV[4], 'PX', ARGV[5])
        redis.call('PEXPIRE', KEYS[3], ARGV[5])
        redis.call('XACK', KEYS[1], ARGV[1], ARGV[3])
        redis.call('XDEL', KEYS[1], ARGV[3])
        return 1
        """;
    private const string RenewScript = """
        local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[3], ARGV[3], 1)
        if #pending == 0 or pending[1][2] ~= ARGV[2] then
            return 0
        end
        redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[2], 0, ARGV[3], 'JUSTID')
        return 1
        """;

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() }
    };

    private readonly RedisAsyncRequestReplyOptions options = redisOptions.Value;
    private readonly TimeSpan enqueueTimeout = requestReplyOptions.Value.EnqueueTimeout;
    private readonly SemaphoreSlim groupLock = new(1, 1);
    private readonly string consumerName = CreateConsumerName(redisOptions.Value.ConsumerName);
    private bool groupCreated;
    private RedisValue autoClaimCursor = "0-0";

    public async ValueTask SubmitAsync(
        string jobId,
        object? payload,
        AsyncExecutionMode executionMode,
        string? accessToken,
        CancellationToken cancellationToken = default)
    {
        var job = new AsyncJobEnvelope(jobId, payload, executionMode);
        var jobJson = JsonSerializer.Serialize(job, JsonOptions);
        var statusJson = JsonSerializer.Serialize(StatusResponseFactory.Queued(jobId), JsonOptions);
        var tokenHash = accessToken is null
            ? Array.Empty<byte>()
            : StatusAccessToken.Hash(accessToken);
        var elapsed = Stopwatch.StartNew();
        Exception? lastTransientError = null;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var remaining = enqueueTimeout - elapsed.Elapsed;

            if (remaining <= TimeSpan.Zero)
            {
                throw new AsyncQueueUnavailableException(
                    "The Redis job stream is temporarily unavailable.",
                    lastTransientError ?? new TimeoutException(
                        "The Redis submission exceeded its enqueue timeout."));
            }

            Task<RedisResult>? submission = null;

            try
            {
                submission = Database.ScriptEvaluateAsync(
                    SubmitScript,
                    [options.StreamKey, StatusKey(jobId), AccessTokenKey(jobId)],
                    [
                        options.MaxQueueLength,
                        jobJson,
                        statusJson,
                        accessToken is null ? 0 : 1,
                        tokenHash
                    ]);
                var result = await submission.WaitAsync(remaining, cancellationToken);

                if ((long)result == 0)
                {
                    throw new AsyncQueueUnavailableException("The Redis job stream is full.");
                }

                return;
            }
            catch (TimeoutException ex)
            {
                _ = ObserveAmbiguousSubmissionAsync(submission!, jobId);
                throw new AsyncQueueUnavailableException(
                    "The Redis job stream is temporarily unavailable.",
                    ex);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                if (submission is not null)
                {
                    _ = ObserveAmbiguousSubmissionAsync(submission, jobId);
                }

                throw;
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                lastTransientError = ex;
                remaining = enqueueTimeout - elapsed.Elapsed;

                if (remaining <= TimeSpan.Zero)
                {
                    throw new AsyncQueueUnavailableException(
                        "The Redis job stream is temporarily unavailable.",
                        ex);
                }

                await Task.Delay(
                    remaining < TimeSpan.FromMilliseconds(100)
                        ? remaining
                        : TimeSpan.FromMilliseconds(100),
                    cancellationToken);
            }
        }
    }

    private static string CreateConsumerName(string? configuredName)
    {
        var prefix = string.IsNullOrWhiteSpace(configuredName)
            ? $"{Environment.MachineName}-{Environment.ProcessId}"
            : configuredName;

        return $"{prefix}-{Guid.NewGuid():N}";
    }

    public async ValueTask EnqueueAsync(
        string jobId,
        object? payload,
        AsyncExecutionMode executionMode,
        CancellationToken cancellationToken = default)
    {
        var job = new AsyncJobEnvelope(jobId, payload, executionMode);
        var json = JsonSerializer.Serialize(job, JsonOptions);
        RedisResult result;

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            result = await Database.ScriptEvaluateAsync(
                EnqueueScript,
                [options.StreamKey],
                [options.MaxQueueLength, json]);
        }
        catch (Exception ex) when (IsTransient(ex))
        {
            throw new AsyncQueueUnavailableException(
                "The Redis job stream is temporarily unavailable.",
                ex);
        }

        if (result.IsNull)
        {
            throw new AsyncQueueUnavailableException("The Redis job stream is full.");
        }
    }

    public async IAsyncEnumerable<AsyncJobDelivery> DequeueAllAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            StreamEntry? entry;

            try
            {
                await EnsureConsumerGroupAsync();
                entry = await ReadNextEntryAsync();
            }
            catch (RedisServerException ex) when (IsNoGroup(ex))
            {
                logger.LogWarning(
                    ex,
                    "Redis consumer group {ConsumerGroup} disappeared. Recreating it before continuing.",
                    options.ConsumerGroup);
                groupCreated = false;
                autoClaimCursor = "0-0";
                continue;
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                throw new AsyncQueueUnavailableException(
                    "The Redis job stream is temporarily unavailable.",
                    ex);
            }

            if (entry is null)
            {
                await Task.Delay(options.QueuePollInterval, cancellationToken);
                continue;
            }

            AsyncJobEnvelope? job = null;

            try
            {
                var value = entry.Value.Values.FirstOrDefault(item => item.Name == JobField).Value;
                job = value.HasValue
                    ? JsonSerializer.Deserialize<AsyncJobEnvelope>(value.ToString(), JsonOptions)
                    : null;
            }
            catch (JsonException ex)
            {
                logger.LogError(
                    ex,
                    "Discarding malformed async request-reply delivery {DeliveryId} from Redis.",
                    entry.Value.Id);
            }

            if (job is null)
            {
                try
                {
                    await CompleteDeliveryAsync(entry.Value.Id);
                }
                catch (Exception ex) when (IsTransient(ex))
                {
                    throw new AsyncQueueUnavailableException(
                        "The Redis job stream is temporarily unavailable.",
                        ex);
                }

                continue;
            }

            yield return new AsyncJobDelivery(entry.Value.Id.ToString(), job);
        }
    }

    public async ValueTask CompleteAsync(
        AsyncJobDelivery delivery,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await CompleteDeliveryAsync(delivery.DeliveryId);
    }

    public async ValueTask CompleteTerminalAsync(
        AsyncJobDelivery delivery,
        AsyncStatusResponse status,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var json = JsonSerializer.Serialize(status, JsonOptions);
        var retentionMilliseconds = Math.Max(
            1,
            (long)Math.Ceiling(options.StatusTimeToLive.TotalMilliseconds));
        var result = await Database.ScriptEvaluateAsync(
            CompleteTerminalScript,
            [options.StreamKey, StatusKey(status.Id), AccessTokenKey(status.Id)],
            [
                options.ConsumerGroup,
                consumerName,
                delivery.DeliveryId,
                json,
                retentionMilliseconds
            ]);

        if ((long)result == 0)
        {
            throw new AsyncDeliveryLeaseLostException(delivery.DeliveryId);
        }
    }

    public async ValueTask RenewAsync(
        AsyncJobDelivery delivery,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var result = await Database.ScriptEvaluateAsync(
            RenewScript,
            [options.StreamKey],
            [options.ConsumerGroup, consumerName, delivery.DeliveryId]);

        if ((long)result == 0)
        {
            throw new AsyncDeliveryLeaseLostException(delivery.DeliveryId);
        }
    }

    public ValueTask AbandonAsync(
        AsyncJobDelivery delivery,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.CompletedTask;
    }

    public async Task<AsyncStatusResponse?> GetAsync(string jobId, CancellationToken cancellationToken = default)
    {
        var value = await Database.StringGetAsync(StatusKey(jobId));

        return value.HasValue
            ? JsonSerializer.Deserialize<AsyncStatusResponse>(value.ToString(), JsonOptions)
            : null;
    }

    public async Task SetAsync(AsyncStatusResponse status, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var json = JsonSerializer.Serialize(status, JsonOptions);
        await Database.StringSetAsync(StatusKey(status.Id), json);
    }

    async Task IAsyncStatusStore.BeginRetentionAsync(
        string jobId,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await Database.KeyExpireAsync(StatusKey(jobId), options.StatusTimeToLive);
    }

    public async Task DeleteAsync(string jobId, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await Database.KeyDeleteAsync([StatusKey(jobId), AccessTokenKey(jobId)]);
    }

    async Task IAsyncStatusTokenStore.SetAsync(
        string jobId,
        string accessToken,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await Database.StringSetAsync(
            AccessTokenKey(jobId),
            StatusAccessToken.Hash(accessToken));
    }

    async Task<bool> IAsyncStatusTokenStore.ValidateAsync(
        string jobId,
        string accessToken,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var storedHash = await Database.StringGetAsync(AccessTokenKey(jobId));

        return storedHash.HasValue
            && System.Security.Cryptography.CryptographicOperations.FixedTimeEquals(
                (byte[])storedHash!,
                StatusAccessToken.Hash(accessToken));
    }

    async Task IAsyncStatusTokenStore.DeleteAsync(
        string jobId,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await Database.KeyDeleteAsync(AccessTokenKey(jobId));
    }

    async Task IAsyncStatusTokenStore.BeginRetentionAsync(
        string jobId,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await Database.KeyExpireAsync(AccessTokenKey(jobId), options.StatusTimeToLive);
    }

    private IDatabase Database => connection.GetDatabase(options.Database);

    private async Task<StreamEntry?> ReadNextEntryAsync()
    {
        while (true)
        {
            var claimed = await Database.StreamAutoClaimAsync(
                options.StreamKey,
                options.ConsumerGroup,
                consumerName,
                (long)options.ClaimIdleTime.TotalMilliseconds,
                autoClaimCursor,
                count: 1);
            autoClaimCursor = claimed.NextStartId;

            if (claimed.ClaimedEntries.Length > 0)
            {
                return claimed.ClaimedEntries[0];
            }

            if (autoClaimCursor == "0-0")
            {
                break;
            }
        }

        var entries = await Database.StreamReadGroupAsync(
            options.StreamKey,
            options.ConsumerGroup,
            consumerName,
            ">",
            count: 1);

        return entries.Length > 0 ? entries[0] : null;
    }

    private async Task EnsureConsumerGroupAsync()
    {
        if (groupCreated)
        {
            return;
        }

        await groupLock.WaitAsync();

        try
        {
            if (groupCreated)
            {
                return;
            }

            try
            {
                await Database.StreamCreateConsumerGroupAsync(
                    options.StreamKey,
                    options.ConsumerGroup,
                    "0-0",
                    createStream: true);
            }
            catch (RedisServerException ex) when (ex.Message.StartsWith("BUSYGROUP", StringComparison.Ordinal))
            {
            }

            groupCreated = true;
        }
        finally
        {
            groupLock.Release();
        }
    }

    private async Task CompleteDeliveryAsync(RedisValue deliveryId)
    {
        var result = await Database.ScriptEvaluateAsync(
            CompleteScript,
            [options.StreamKey],
            [options.ConsumerGroup, consumerName, deliveryId]);

        if ((long)result == 0)
        {
            throw new AsyncDeliveryLeaseLostException(deliveryId.ToString());
        }
    }

    private string StatusKey(string jobId)
    {
        return $"{options.StatusKeyPrefix}{jobId}";
    }

    private string AccessTokenKey(string jobId)
    {
        return $"{options.StatusKeyPrefix}access:{jobId}";
    }

    private static bool IsTransient(Exception exception)
    {
        return exception is RedisConnectionException or RedisTimeoutException;
    }

    private static bool IsNoGroup(RedisServerException exception)
    {
        return exception.Message.StartsWith("NOGROUP", StringComparison.Ordinal);
    }

    private async Task ObserveAmbiguousSubmissionAsync(
        Task<RedisResult> submission,
        string jobId)
    {
        try
        {
            await submission;
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Redis submission for job {JobId} failed after the caller stopped waiting for its result.",
                jobId);
        }
    }
}
