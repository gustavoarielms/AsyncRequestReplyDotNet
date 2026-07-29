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
        services.TryAddSingleton<IAsyncStatusStore, InMemoryAsyncStatusStore>();
        services.TryAddSingleton<IAsyncStatusTokenStore, InMemoryAsyncStatusTokenStore>();
        services.AddHostedService<AsyncJobBackgroundService>();

        return services;
    }
}
