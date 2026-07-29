using AsyncRequestReply.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace AsyncRequestReply;

public static class RedisAsyncRequestReplyServiceCollectionExtensions
{
    public static IServiceCollection AddAsyncRequestReplyRedis(
        this IServiceCollection services,
        Action<RedisAsyncRequestReplyOptions>? configure = null)
    {
        var options = services.AddOptions<RedisAsyncRequestReplyOptions>();

        if (configure is not null)
        {
            options.Configure(configure);
        }

        options.ValidateOnStart();
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<RedisAsyncRequestReplyOptions>, RedisAsyncRequestReplyOptionsValidator>());

        services.TryAddSingleton<IConnectionMultiplexer>(sp =>
        {
            var redisOptions = sp.GetRequiredService<IOptions<RedisAsyncRequestReplyOptions>>().Value;
            return ConnectionMultiplexer.Connect(redisOptions.Configuration);
        });

        services.TryAddSingleton<RedisAsyncRequestReplyStore>();

        services.RemoveAll<IAsyncJobQueue>();
        services.RemoveAll<IAsyncJobQueueReader>();
        services.RemoveAll<IAsyncStatusStore>();
        services.RemoveAll<IAsyncStatusTokenStore>();

        services.AddSingleton<IAsyncJobQueue>(sp => sp.GetRequiredService<RedisAsyncRequestReplyStore>());
        services.AddSingleton<IAsyncJobQueueReader>(sp => sp.GetRequiredService<RedisAsyncRequestReplyStore>());
        services.AddSingleton<IAsyncStatusStore>(sp => sp.GetRequiredService<RedisAsyncRequestReplyStore>());
        services.AddSingleton<IAsyncStatusTokenStore>(sp => sp.GetRequiredService<RedisAsyncRequestReplyStore>());

        return services;
    }
}
