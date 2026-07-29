using System.Net;
using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace AsyncRequestReply.Tests;

public sealed class AsyncRequestReplyTests
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() }
    };

    [Fact]
    public async Task AsyncEndpoint_ReturnsAccepted()
    {
        await using var app = await TestApp.StartAsync();

        var response = await app.Client.PostAsJsonAsync(
            "/orders",
            new { data = new { name = "order-1" } },
            TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
    }

    [Fact]
    public async Task AsyncEndpoint_IncludesLocationHeader()
    {
        await using var app = await TestApp.StartAsync();

        var response = await app.Client.PostAsJsonAsync(
            "/orders",
            new { data = new { name = "order-1" } },
            TestContext.Current.CancellationToken);

        Assert.NotNull(response.Headers.Location);
        Assert.StartsWith("/async-status/status/", response.Headers.Location!.OriginalString);
    }

    [Fact]
    public async Task AsyncEndpoint_ReturnsAcceptedBody()
    {
        await using var app = await TestApp.StartAsync();

        var response = await app.Client.PostAsJsonAsync(
            "/orders",
            new { data = new { name = "order-1" } },
            TestContext.Current.CancellationToken);
        var body = await response.Content.ReadFromJsonAsync<AsyncAcceptedResponse>(
            JsonOptions,
            TestContext.Current.CancellationToken);

        Assert.NotNull(body);
        Assert.False(string.IsNullOrWhiteSpace(body!.Id));
        Assert.Equal(AsyncJobStatus.queued, body.Status);
        Assert.StartsWith($"/async-status/status/{body.Id}/", body.Location);
        Assert.NotEqual(body.Id, body.Location.Split('/').Last());
    }

    [Fact]
    public async Task AsyncEndpoint_StoresInitialQueuedStatus()
    {
        await using var app = await TestApp.StartAsync();

        var accepted = await PostOrderAsync(app.Client);

        var stored = await app.StatusStore.GetAsync(
            accepted.Id,
            TestContext.Current.CancellationToken);
        Assert.NotNull(stored);
        Assert.Equal(AsyncJobStatus.queued, stored!.Status);
    }

    [Fact]
    public async Task StatusEndpoint_ReturnsStoredJobStatus()
    {
        await using var app = await TestApp.StartAsync();
        var accepted = await PostOrderAsync(app.Client);

        var status = await app.Client.GetFromJsonAsync<AsyncStatusResponse>(
            accepted.Location,
            JsonOptions,
            TestContext.Current.CancellationToken);

        Assert.NotNull(status);
        Assert.Equal(accepted.Id, status!.Id);
        Assert.Equal(AsyncJobStatus.queued, status.Status);
    }

    [Fact]
    public async Task SuccessfulProcessor_ChangesStatusToCompleted()
    {
        var processor = new CapturingProcessor(_ => new { saved = true });
        await using var app = await TestApp.StartAsync(processor);

        var accepted = await PostOrderAsync(app.Client);
        var status = await PollStatusAsync(app.Client, accepted.Location, AsyncJobStatus.completed);

        Assert.Equal(AsyncJobStatus.completed, status.Status);
        Assert.NotNull(status.Result);
        Assert.Null(status.Error);
    }

    [Fact]
    public async Task FailingProcessor_ChangesStatusToFailed()
    {
        const string secret = "password=/private/path";
        var processor = new CapturingProcessor(_ => throw new InvalidOperationException(secret));
        await using var app = await TestApp.StartAsync(processor);

        var accepted = await PostOrderAsync(app.Client);
        var status = await PollStatusAsync(app.Client, accepted.Location, AsyncJobStatus.failed);

        Assert.Equal(AsyncJobStatus.failed, status.Status);
        Assert.NotNull(status.Error);
        Assert.DoesNotContain(secret, status.Error);
        Assert.Contains(accepted.Id, status.Error);
    }

    [Fact]
    public async Task PayloadOverConfiguredLimit_ReturnsPayloadTooLarge()
    {
        await using var app = await TestApp.StartAsync(
            configureEndpoint: options => options.MaxPayloadBytes = 16);

        var response = await app.Client.PostAsJsonAsync(
            "/orders",
            new { data = new { name = "payload-that-is-too-large" } },
            TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, response.StatusCode);
    }

    [Fact]
    public async Task MalformedJson_ReturnsBadRequest()
    {
        await using var app = await TestApp.StartAsync();
        using var content = new StringContent("{invalid", System.Text.Encoding.UTF8, "application/json");

        var response = await app.Client.PostAsync(
            "/orders",
            content,
            TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task QueueRejection_ReturnsServiceUnavailableAndDeletesQueuedStatus()
    {
        var store = new TrackingStatusStore();
        await using var app = await TestApp.StartAsync(configureServices: services =>
        {
            services.AddSingleton<IAsyncJobQueue, RejectingQueue>();
            services.AddSingleton<IAsyncStatusStore>(store);
        });

        var response = await app.Client.PostAsJsonAsync(
            "/orders",
            new { data = new { name = "order-1" } },
            TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.Equal("1", response.Headers.RetryAfter?.Delta?.TotalSeconds.ToString() ?? response.Headers.GetValues("Retry-After").Single());
        Assert.Empty(store.Statuses);
    }

    [Fact]
    public async Task ExpiredStatus_ReturnsNotFoundAndIsRemoved()
    {
        await using var app = await TestApp.StartAsync(
            configureOptions: options => options.StatusTimeToLive = TimeSpan.FromMilliseconds(50));
        var accepted = await PostOrderAsync(app.Client);

        await Task.Delay(150, TestContext.Current.CancellationToken);
        var response = await app.Client.GetAsync(
            accepted.Location,
            TestContext.Current.CancellationToken);
        var stored = await app.StatusStore.GetAsync(
            accepted.Id,
            TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.Null(stored);
    }

    [Fact]
    public async Task StatusCapacity_EvictsOldestEntry()
    {
        await using var app = await TestApp.StartAsync(
            configureOptions: options => options.StatusCapacity = 1);
        var now = DateTimeOffset.UtcNow;

        await app.StatusStore.SetAsync(new AsyncStatusResponse(
            "old",
            AsyncJobStatus.queued,
            null,
            null,
            now,
            now),
            TestContext.Current.CancellationToken);
        await app.StatusStore.SetAsync(new AsyncStatusResponse(
            "new",
            AsyncJobStatus.queued,
            null,
            null,
            now.AddSeconds(1),
            now.AddSeconds(1)),
            TestContext.Current.CancellationToken);

        Assert.Null(await app.StatusStore.GetAsync(
            "old",
            TestContext.Current.CancellationToken));
        Assert.NotNull(await app.StatusStore.GetAsync(
            "new",
            TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task AccessPolicyDenial_ReturnsNotFound()
    {
        await using var app = await TestApp.StartAsync(configureServices: services =>
        {
            services.AddSingleton<IAsyncStatusAccessPolicy, DenyStatusAccessPolicy>();
        });
        var accepted = await PostOrderAsync(app.Client);

        var response = await app.Client.GetAsync(
            accepted.Location,
            TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task InvalidCapabilityToken_ReturnsNotFound()
    {
        await using var app = await TestApp.StartAsync();
        var accepted = await PostOrderAsync(app.Client);
        var invalidLocation = string.Join(
            '/',
            accepted.Location.Split('/').SkipLast(1).Append("invalid-token"));

        var response = await app.Client.GetAsync(
            invalidLocation,
            TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task PolicyAccess_DoesNotExposeCapabilityToken()
    {
        await using var app = await TestApp.StartAsync(
            configureServices: services =>
                services.AddSingleton<IAsyncStatusAccessPolicy, AllowStatusAccessPolicy>(),
            configureOptions: options => options.AllowCapabilityStatusAccess = false);

        var accepted = await PostOrderAsync(app.Client);
        var response = await app.Client.GetAsync(
            accepted.Location,
            TestContext.Current.CancellationToken);

        Assert.Equal($"/async-status/status/{accepted.Id}", accepted.Location);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task StatusResponse_DisablesCaching()
    {
        await using var app = await TestApp.StartAsync();
        var accepted = await PostOrderAsync(app.Client);

        var response = await app.Client.GetAsync(
            accepted.Location,
            TestContext.Current.CancellationToken);

        Assert.Equal("no-store", response.Headers.CacheControl?.ToString());
    }

    [Fact]
    public async Task StatusGet_DoesNotInvokeExternalResolver()
    {
        var resolver = new CapturingResolver(_ => null);
        await using var app = await TestApp.StartAsync(configureServices: services =>
        {
            services.AddSingleton<IExternalStatusResolver>(resolver);
            services.AddSingleton<IAsyncStatusAccessPolicy, AllowStatusAccessPolicy>();
        });
        var now = DateTimeOffset.UtcNow;
        await app.StatusStore.SetAsync(new AsyncStatusResponse(
            "external-job",
            AsyncJobStatus.waiting_external,
            null,
            null,
            now,
            now),
            TestContext.Current.CancellationToken);

        for (var index = 0; index < 20; index++)
        {
            var response = await app.Client.GetAsync(
                "/async-status/status/external-job",
                TestContext.Current.CancellationToken);
            response.EnsureSuccessStatusCode();
        }

        Assert.Equal(0, resolver.Calls);
    }

    [Fact]
    public async Task ExternalResolver_RunsInBackground()
    {
        var resolver = new CapturingResolver(status => status with
        {
            Status = AsyncJobStatus.completed,
            Result = new { resolved = true },
            UpdatedAt = DateTimeOffset.UtcNow
        });
        await using var app = await TestApp.StartAsync(
            configureServices: services => services.AddSingleton<IExternalStatusResolver>(resolver),
            configureEndpoint: options => options.ExecutionMode = AsyncExecutionMode.wait_external);

        var accepted = await PostOrderAsync(app.Client);
        var status = await PollStatusAsync(app.Client, accepted.Location, AsyncJobStatus.completed);

        Assert.Equal(AsyncJobStatus.completed, status.Status);
        Assert.Equal(1, resolver.Calls);
    }

    [Fact]
    public void InvalidOptions_AreRejected()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddAsyncRequestReply(options => options.QueueCapacity = 0);
        using var provider = services.BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(
            () => provider.GetRequiredService<IOptions<AsyncRequestReplyOptions>>().Value);
    }

    [Fact]
    public async Task InMemoryQueue_RejectsWhenCapacityIsExhausted()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddAsyncRequestReply(options =>
        {
            options.QueueCapacity = 1;
            options.EnqueueTimeout = TimeSpan.FromMilliseconds(25);
        });
        await using var provider = services.BuildServiceProvider();
        var queue = provider.GetRequiredService<IAsyncJobQueue>();

        await queue.EnqueueAsync(
            "first",
            new { value = 1 },
            AsyncExecutionMode.resolve_now,
            TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<AsyncQueueUnavailableException>(
            () => queue.EnqueueAsync(
                "second",
                new { value = 2 },
                AsyncExecutionMode.resolve_now,
                TestContext.Current.CancellationToken).AsTask());
    }

    [Fact]
    public void RemoteRedisWithoutTls_IsRejected()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddAsyncRequestReply();
        services.AddAsyncRequestReplyRedis(options => options.Configuration = "redis.example.com:6379");
        using var provider = services.BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(
            () => provider.GetRequiredService<IOptions<RedisAsyncRequestReplyOptions>>().Value);
    }

    [Fact]
    [Trait("Category", "RedisIntegration")]
    public async Task RedisTransport_EnforcesCapacityRecoversPendingDeliveryAndExpiresState()
    {
        var configuration = Environment.GetEnvironmentVariable("ASYNC_REQUEST_REPLY_REDIS_CONNECTION");

        if (string.IsNullOrWhiteSpace(configuration))
        {
            Assert.Skip("Set ASYNC_REQUEST_REPLY_REDIS_CONNECTION to run the Redis integration test.");
        }

        var suffix = Guid.NewGuid().ToString("N");
        var streamKey = $"async-request-reply:test:{suffix}:jobs";
        var statusPrefix = $"async-request-reply:test:{suffix}:status:";
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddAsyncRequestReply(options =>
        {
            options.DeliveryLeaseRenewalInterval = TimeSpan.FromMilliseconds(10);
        });
        services.AddAsyncRequestReplyRedis(options =>
        {
            options.Configuration = configuration;
            options.StreamKey = streamKey;
            options.ConsumerGroup = $"group-{suffix}";
            options.ConsumerName = $"consumer-{suffix}";
            options.StatusKeyPrefix = statusPrefix;
            options.MaxQueueLength = 1;
            options.QueuePollInterval = TimeSpan.FromMilliseconds(10);
            options.ClaimIdleTime = TimeSpan.FromMilliseconds(50);
            options.StatusTimeToLive = TimeSpan.FromMilliseconds(75);
        });
        await using var provider = services.BuildServiceProvider();
        var queue = provider.GetRequiredService<IAsyncJobQueue>();
        var reader = provider.GetRequiredService<IAsyncJobQueueReader>();
        var statusStore = provider.GetRequiredService<IAsyncStatusStore>();
        var tokenStore = provider.GetRequiredService<IAsyncStatusTokenStore>();
        var connection = provider.GetRequiredService<StackExchange.Redis.IConnectionMultiplexer>();

        try
        {
            await queue.EnqueueAsync(
                "job-1",
                new { value = 1 },
                AsyncExecutionMode.resolve_now,
                TestContext.Current.CancellationToken);
            await Assert.ThrowsAsync<AsyncQueueUnavailableException>(
                () => queue.EnqueueAsync(
                    "job-2",
                    new { value = 2 },
                    AsyncExecutionMode.resolve_now,
                    TestContext.Current.CancellationToken).AsTask());

            var firstDelivery = await ReadOneAsync(reader);
            await Task.Delay(100, TestContext.Current.CancellationToken);
            var recoveredDelivery = await ReadOneAsync(reader);

            Assert.Equal(firstDelivery.DeliveryId, recoveredDelivery.DeliveryId);

            await reader.CompleteAsync(
                recoveredDelivery,
                TestContext.Current.CancellationToken);
            await queue.EnqueueAsync(
                "job-2",
                new { value = 2 },
                AsyncExecutionMode.resolve_now,
                TestContext.Current.CancellationToken);

            var now = DateTimeOffset.UtcNow;
            await statusStore.SetAsync(new AsyncStatusResponse(
                "job-1",
                AsyncJobStatus.queued,
                null,
                null,
                now,
                now),
                TestContext.Current.CancellationToken);
            await tokenStore.SetAsync(
                "job-1",
                "secret",
                TestContext.Current.CancellationToken);

            Assert.True(await tokenStore.ValidateAsync(
                "job-1",
                "secret",
                TestContext.Current.CancellationToken));

            await Task.Delay(150, TestContext.Current.CancellationToken);

            Assert.Null(await statusStore.GetAsync(
                "job-1",
                TestContext.Current.CancellationToken));
            Assert.False(await tokenStore.ValidateAsync(
                "job-1",
                "secret",
                TestContext.Current.CancellationToken));
        }
        finally
        {
            var database = connection.GetDatabase();
            await database.KeyDeleteAsync(
                [streamKey, $"{statusPrefix}job-1", $"{statusPrefix}access:job-1"]);
        }
    }

    [Fact]
    public async Task PayloadPath_ExtractsConfiguredBodyProperty()
    {
        var processor = new CapturingProcessor(payload => payload);
        await using var app = await TestApp.StartAsync(processor);

        await PostOrderAsync(app.Client, new { data = new { name = "nested" }, ignored = true });
        var payload = await processor.Payload.Task.WaitAsync(
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken);

        var json = Assert.IsType<JsonElement>(payload);
        Assert.Equal("nested", json.GetProperty("name").GetString());
        Assert.False(json.TryGetProperty("ignored", out _));
    }

    [Fact]
    public async Task BackgroundWorker_UsesConfiguredQueueReader()
    {
        var queue = new TestQueue();
        var processor = new CapturingProcessor(_ => new { saved = true });
        await using var app = await TestApp.StartAsync(processor, services =>
        {
            services.AddSingleton<IAsyncJobQueue>(queue);
            services.AddSingleton<IAsyncJobQueueReader>(queue);
        });

        var accepted = await PostOrderAsync(app.Client);
        var status = await PollStatusAsync(app.Client, accepted.Location, AsyncJobStatus.completed);

        Assert.Equal(AsyncJobStatus.completed, status.Status);
        Assert.True(queue.DequeueStarted);
    }

    [Fact]
    public void AddAsyncRequestReplyRedis_ReplacesDefaultTransportRegistrations()
    {
        var services = new ServiceCollection();

        services.AddAsyncRequestReply();
        services.AddAsyncRequestReplyRedis(options => options.Configuration = "localhost:6379");

        var redisStoreType = typeof(IAsyncJobQueue).Assembly.GetType("AsyncRequestReply.Internal.RedisAsyncRequestReplyStore");

        Assert.NotNull(redisStoreType);
        Assert.Contains(services, descriptor => descriptor.ServiceType == redisStoreType);
        Assert.Single(services, descriptor => descriptor.ServiceType == typeof(IAsyncJobQueue));
        Assert.Single(services, descriptor => descriptor.ServiceType == typeof(IAsyncJobQueueReader));
        Assert.Single(services, descriptor => descriptor.ServiceType == typeof(IAsyncStatusStore));
        Assert.Single(services, descriptor => descriptor.ServiceType == typeof(IAsyncStatusTokenStore));
    }

    private static async Task<AsyncAcceptedResponse> PostOrderAsync(HttpClient client, object? body = null)
    {
        var response = await client.PostAsJsonAsync("/orders", body ?? new { data = new { name = "order-1" } });
        response.EnsureSuccessStatusCode();

        var accepted = await response.Content.ReadFromJsonAsync<AsyncAcceptedResponse>(JsonOptions);
        Assert.NotNull(accepted);
        return accepted!;
    }

    private static async Task<AsyncStatusResponse> PollStatusAsync(HttpClient client, string location, AsyncJobStatus expectedStatus)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        while (!timeout.IsCancellationRequested)
        {
            var status = await client.GetFromJsonAsync<AsyncStatusResponse>(location, JsonOptions, timeout.Token);

            if (status?.Status == expectedStatus)
            {
                return status;
            }

            await Task.Delay(50, timeout.Token);
        }

        throw new TimeoutException($"Status did not become {expectedStatus}.");
    }

    private static async Task<AsyncJobDelivery> ReadOneAsync(IAsyncJobQueueReader reader)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await foreach (var delivery in reader.DequeueAllAsync(timeout.Token))
        {
            return delivery;
        }

        throw new TimeoutException("Redis did not produce a delivery.");
    }

    private sealed class CapturingProcessor(Func<object?, object?> process) : IAsyncJobProcessor
    {
        public TaskCompletionSource<object?> Payload { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<object?> ProcessAsync(string jobId, object? payload, CancellationToken cancellationToken = default)
        {
            Payload.TrySetResult(payload);
            return Task.FromResult(process(payload));
        }
    }

    private sealed class CapturingResolver(
        Func<AsyncStatusResponse, AsyncStatusResponse?> resolve) : IExternalStatusResolver
    {
        private int calls;

        public int Calls => Volatile.Read(ref calls);

        public Task<AsyncStatusResponse?> ResolveAsync(
            string jobId,
            AsyncStatusResponse currentStatus,
            CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref calls);
            return Task.FromResult(resolve(currentStatus));
        }
    }

    private sealed class TestQueue : IAsyncJobQueue, IAsyncJobQueueReader
    {
        private readonly Channel<AsyncJobEnvelope> channel = Channel.CreateUnbounded<AsyncJobEnvelope>();

        public bool DequeueStarted { get; private set; }

        public ValueTask EnqueueAsync(
            string jobId,
            object? payload,
            AsyncExecutionMode executionMode,
            CancellationToken cancellationToken = default)
        {
            return channel.Writer.WriteAsync(new AsyncJobEnvelope(jobId, payload, executionMode), cancellationToken);
        }

        public async IAsyncEnumerable<AsyncJobDelivery> DequeueAllAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            DequeueStarted = true;

            await foreach (var job in channel.Reader.ReadAllAsync(cancellationToken))
            {
                yield return new AsyncJobDelivery(job.Id, job);
            }
        }

        public ValueTask CompleteAsync(
            AsyncJobDelivery delivery,
            CancellationToken cancellationToken = default)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask RenewAsync(
            AsyncJobDelivery delivery,
            CancellationToken cancellationToken = default)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask AbandonAsync(
            AsyncJobDelivery delivery,
            CancellationToken cancellationToken = default)
        {
            return channel.Writer.WriteAsync(delivery.Job, cancellationToken);
        }
    }

    private sealed class RejectingQueue : IAsyncJobQueue
    {
        public ValueTask EnqueueAsync(
            string jobId,
            object? payload,
            AsyncExecutionMode executionMode,
            CancellationToken cancellationToken = default)
        {
            throw new AsyncQueueUnavailableException("full");
        }
    }

    private sealed class TrackingStatusStore : IAsyncStatusStore
    {
        public Dictionary<string, AsyncStatusResponse> Statuses { get; } = new();

        public Task<AsyncStatusResponse?> GetAsync(
            string jobId,
            CancellationToken cancellationToken = default)
        {
            Statuses.TryGetValue(jobId, out var status);
            return Task.FromResult(status);
        }

        public Task SetAsync(
            AsyncStatusResponse status,
            CancellationToken cancellationToken = default)
        {
            Statuses[status.Id] = status;
            return Task.CompletedTask;
        }

        public Task DeleteAsync(
            string jobId,
            CancellationToken cancellationToken = default)
        {
            Statuses.Remove(jobId);
            return Task.CompletedTask;
        }
    }

    private sealed class DenyStatusAccessPolicy : IAsyncStatusAccessPolicy
    {
        public ValueTask<bool> CanReadAsync(
            HttpContext httpContext,
            string jobId,
            CancellationToken cancellationToken = default)
        {
            return ValueTask.FromResult(false);
        }
    }

    private sealed class AllowStatusAccessPolicy : IAsyncStatusAccessPolicy
    {
        public ValueTask<bool> CanReadAsync(
            HttpContext httpContext,
            string jobId,
            CancellationToken cancellationToken = default)
        {
            return ValueTask.FromResult(true);
        }
    }

    private sealed class TestApp : IAsyncDisposable
    {
        private readonly WebApplication app;

        private TestApp(WebApplication app, HttpClient client, IAsyncStatusStore statusStore)
        {
            this.app = app;
            Client = client;
            StatusStore = statusStore;
        }

        public HttpClient Client { get; }

        public IAsyncStatusStore StatusStore { get; }

        public static async Task<TestApp> StartAsync(
            IAsyncJobProcessor? processor = null,
            Action<IServiceCollection>? configureServices = null,
            Action<AsyncRequestReplyOptions>? configureOptions = null,
            Action<AsyncEndpointOptions>? configureEndpoint = null)
        {
            var builder = WebApplication.CreateBuilder();
            builder.WebHost.UseUrls("http://127.0.0.1:0");
            configureServices?.Invoke(builder.Services);

            builder.Services.AddAsyncRequestReply(options =>
            {
                options.StatusBasePath = "/async-status";
                options.AllowCapabilityStatusAccess = true;
                options.ExternalResolutionInterval = TimeSpan.FromMilliseconds(10);
                configureOptions?.Invoke(options);
            });

            if (processor is not null)
            {
                builder.Services.AddSingleton(processor);
            }

            var app = builder.Build();
            app.MapAsyncRequestReplyStatusEndpoints();
            app.MapPost("/orders", () => Results.NoContent())
                .AsAsyncRequestReply(options =>
                {
                    options.PayloadPath = "data";
                    configureEndpoint?.Invoke(options);
                });

            await app.StartAsync();

            var address = app.Services.GetRequiredService<IServer>().Features
                .Get<IServerAddressesFeature>()!
                .Addresses
                .Single();

            return new TestApp(app, new HttpClient { BaseAddress = new Uri(address) }, app.Services.GetRequiredService<IAsyncStatusStore>());
        }

        public async ValueTask DisposeAsync()
        {
            Client.Dispose();
            await app.DisposeAsync();
        }
    }
}
