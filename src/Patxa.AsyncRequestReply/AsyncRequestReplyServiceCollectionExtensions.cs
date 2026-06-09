using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Patxa.AsyncRequestReply.Internal;

namespace Patxa.AsyncRequestReply;

public static class AsyncRequestReplyServiceCollectionExtensions
{
    public static IServiceCollection AddAsyncRequestReply(
        this IServiceCollection services,
        Action<AsyncRequestReplyOptions>? configure = null)
    {
        if (configure is not null)
        {
            services.Configure(configure);
        }
        else
        {
            services.Configure<AsyncRequestReplyOptions>(_ => { });
        }

        services.TryAddSingleton<InMemoryAsyncJobQueue>();
        services.TryAddSingleton<IAsyncJobQueue>(sp => sp.GetRequiredService<InMemoryAsyncJobQueue>());
        services.TryAddSingleton<IAsyncStatusStore, InMemoryAsyncStatusStore>();
        services.AddHostedService<AsyncJobBackgroundService>();

        return services;
    }
}
