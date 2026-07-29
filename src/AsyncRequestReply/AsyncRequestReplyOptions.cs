namespace AsyncRequestReply;

public sealed class AsyncRequestReplyOptions
{
    public string StatusBasePath { get; set; } = "/async-status";

    public bool AllowCapabilityStatusAccess { get; set; }

    public int QueueCapacity { get; set; } = 1_000;

    public TimeSpan EnqueueTimeout { get; set; } = TimeSpan.FromSeconds(2);

    public int StatusCapacity { get; set; } = 10_000;

    public TimeSpan StatusTimeToLive { get; set; } = TimeSpan.FromHours(1);

    public int WorkerConcurrency { get; set; } = 4;

    public TimeSpan WorkerRecoveryInterval { get; set; } = TimeSpan.FromSeconds(1);

    public TimeSpan DeliveryLeaseRenewalInterval { get; set; } = TimeSpan.FromSeconds(15);

    public TimeSpan ExternalResolutionInterval { get; set; } = TimeSpan.FromSeconds(1);

    public TimeSpan ExternalResolutionTimeout { get; set; } = TimeSpan.FromSeconds(10);

    public int ExternalResolutionMaxAttempts { get; set; } = 5;
}
