using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using AsyncRequestReply.Internal;

namespace AsyncRequestReply;

public static class AsyncRequestReplyServiceCollectionExtensions
{
    public static IServiceCollection AddAsyncRequestReply(
        this IServiceCollection services,
        Action<AsyncRequestReplyOptions>? configure = null)
    {
        var options = services.AddOptions<AsyncRequestReplyOptions>();

        if (configure is not null)
        {
            options.Configure(configure);
        }

        options
            .Validate(value => value.QueueCapacity > 0, "QueueCapacity must be greater than zero.")
            .Validate(value => value.EnqueueTimeout > TimeSpan.Zero, "EnqueueTimeout must be greater than zero.")
            .Validate(value => value.StatusCapacity > 0, "StatusCapacity must be greater than zero.")
            .Validate(value => value.StatusTimeToLive > TimeSpan.Zero, "StatusTimeToLive must be greater than zero.")
            .Validate(
                value => !value.AllowCapabilityStatusAccess
                    || value.SubmissionIdentitySecret is { } secret
                    && System.Text.Encoding.UTF8.GetByteCount(secret) >= 32,
                "SubmissionIdentitySecret must contain at least 32 UTF-8 bytes when capability status access is enabled.")
            .Validate(value => value.WorkerConcurrency > 0, "WorkerConcurrency must be greater than zero.")
            .Validate(
                value => value.WorkerRecoveryInterval > TimeSpan.Zero,
                "WorkerRecoveryInterval must be greater than zero.")
            .Validate(
                value => value.DeliveryLeaseRenewalInterval > TimeSpan.Zero,
                "DeliveryLeaseRenewalInterval must be greater than zero.")
            .Validate(
                value => value.ExternalResolutionInterval > TimeSpan.Zero,
                "ExternalResolutionInterval must be greater than zero.")
            .Validate(
                value => value.ExternalResolutionTimeout > TimeSpan.Zero,
                "ExternalResolutionTimeout must be greater than zero.")
            .Validate(
                value => value.ExternalResolutionMaxAttempts > 0,
                "ExternalResolutionMaxAttempts must be greater than zero.")
            .ValidateOnStart();

        services.TryAddSingleton<InMemoryAsyncJobQueue>();
        services.TryAddSingleton<IAsyncJobQueue>(sp => sp.GetRequiredService<InMemoryAsyncJobQueue>());
        services.TryAddSingleton<IAsyncJobQueueReader>(sp => sp.GetRequiredService<InMemoryAsyncJobQueue>());
        services.TryAddSingleton<IAsyncJobSubmissionStore>(sp => sp.GetRequiredService<InMemoryAsyncJobQueue>());
        services.TryAddSingleton<InMemoryAsyncStatusStore>();
        services.TryAddSingleton<IAsyncStatusStore>(sp => sp.GetRequiredService<InMemoryAsyncStatusStore>());
        services.TryAddSingleton<IAsyncStatusTokenStore>(sp => sp.GetRequiredService<InMemoryAsyncStatusStore>());
        services.AddHostedService<AsyncJobBackgroundService>();

        return services;
    }
}
