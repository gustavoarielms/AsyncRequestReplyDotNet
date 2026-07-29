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
    ILogger<RedisAsyncRequestReplyStore> logger) :
    IAsyncJobQueue,
    IAsyncJobQueueReader,
    IAsyncStatusStore,
    IAsyncStatusTokenStore
{
    private const string JobField = "job";
    private const string EnqueueScript = """
        if redis.call('XLEN', KEYS[1]) >= tonumber(ARGV[1]) then
            return nil
        end
        return redis.call('XADD', KEYS[1], '*', 'job', ARGV[2])
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
    private readonly SemaphoreSlim groupLock = new(1, 1);
    private readonly string consumerName = string.IsNullOrWhiteSpace(redisOptions.Value.ConsumerName)
        ? $"{Environment.MachineName}-{Environment.ProcessId}-{Guid.NewGuid():N}"
        : redisOptions.Value.ConsumerName;
    private bool groupCreated;

    public async ValueTask EnqueueAsync(
        string jobId,
        object? payload,
        AsyncExecutionMode executionMode,
        CancellationToken cancellationToken = default)
    {
        var job = new AsyncJobEnvelope(jobId, payload, executionMode);
        var json = JsonSerializer.Serialize(job, JsonOptions);
        var result = await Database.ScriptEvaluateAsync(
            EnqueueScript,
            [options.StreamKey],
            [options.MaxQueueLength, json]);

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
        var claimed = await Database.StreamAutoClaimAsync(
            options.StreamKey,
            options.ConsumerGroup,
            consumerName,
            (long)options.ClaimIdleTime.TotalMilliseconds,
            "0-0",
            count: 1);

        if (claimed.ClaimedEntries.Length > 0)
        {
            return claimed.ClaimedEntries[0];
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
}
