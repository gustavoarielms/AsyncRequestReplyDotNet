namespace AsyncRequestReply;

public sealed class RedisAsyncRequestReplyOptions
{
    public string Configuration { get; set; } = "localhost:6379";

    public int Database { get; set; } = -1;

    public string StreamKey { get; set; } = "{async-request-reply}:jobs";

    public string ConsumerGroup { get; set; } = "async-request-reply";

    public string? ConsumerName { get; set; }

    public string StatusKeyPrefix { get; set; } = "{async-request-reply}:status:";

    public TimeSpan QueuePollInterval { get; set; } = TimeSpan.FromMilliseconds(250);

    public TimeSpan ClaimIdleTime { get; set; } = TimeSpan.FromMinutes(1);

    public int MaxQueueLength { get; set; } = 10_000;

    public TimeSpan StatusTimeToLive { get; set; } = TimeSpan.FromHours(1);

    public bool AllowUnencryptedRemoteConnection { get; set; }
}
