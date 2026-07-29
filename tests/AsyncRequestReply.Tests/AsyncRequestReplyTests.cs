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
    public async Task SubmissionRejection_ReturnsServiceUnavailable()
    {
        await using var app = await TestApp.StartAsync(configureServices: services =>
        {
            services.AddSingleton<IAsyncJobSubmissionStore, RejectingSubmissionStore>();
        });

        var response = await app.Client.PostAsJsonAsync(
            "/orders",
            new { data = new { name = "order-1" } },
            TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.Equal("1", response.Headers.RetryAfter?.Delta?.TotalSeconds.ToString() ?? response.Headers.GetValues("Retry-After").Single());
    }

    [Fact]
    public async Task TerminalStatus_ExpiresAndIsRemoved()
    {
        var processor = new CapturingProcessor(_ => new { saved = true });
        await using var app = await TestApp.StartAsync(
            processor,
            configureOptions: options => options.StatusTimeToLive = TimeSpan.FromMilliseconds(200));
        var accepted = await PostOrderAsync(app.Client);
        await PollStatusAsync(app.Client, accepted.Location, AsyncJobStatus.completed);

        await Task.Delay(350, TestContext.Current.CancellationToken);
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
    public async Task StatusCapacity_EvictsOnlyOldestRetainedStatusAndCapabilityPair()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddAsyncRequestReply(options => options.StatusCapacity = 2);
        await using var provider = services.BuildServiceProvider();
        var submissionStore = provider.GetRequiredService<IAsyncJobSubmissionStore>();
        var statusStore = provider.GetRequiredService<IAsyncStatusStore>();
        var tokenStore = provider.GetRequiredService<IAsyncStatusTokenStore>();

        await submissionStore.SubmitAsync(
            "terminal",
            new { value = 1 },
            AsyncExecutionMode.resolve_now,
            "terminal-token",
            TestContext.Current.CancellationToken);
        await statusStore.SetAsync(
            StatusResponseFactoryForTest("terminal", AsyncJobStatus.completed),
            TestContext.Current.CancellationToken);
        await statusStore.BeginRetentionAsync(
            "terminal",
            TestContext.Current.CancellationToken);
        await tokenStore.BeginRetentionAsync(
            "terminal",
            TestContext.Current.CancellationToken);
        await submissionStore.SubmitAsync(
            "active",
            new { value = 2 },
            AsyncExecutionMode.resolve_now,
            "active-token",
            TestContext.Current.CancellationToken);
        await submissionStore.SubmitAsync(
            "new",
            new { value = 3 },
            AsyncExecutionMode.resolve_now,
            "new-token",
            TestContext.Current.CancellationToken);

        Assert.Null(await statusStore.GetAsync(
            "terminal",
            TestContext.Current.CancellationToken));
        Assert.False(await tokenStore.ValidateAsync(
            "terminal",
            "terminal-token",
            TestContext.Current.CancellationToken));
        Assert.NotNull(await statusStore.GetAsync(
            "active",
            TestContext.Current.CancellationToken));
        Assert.True(await tokenStore.ValidateAsync(
            "active",
            "active-token",
            TestContext.Current.CancellationToken));
        Assert.NotNull(await statusStore.GetAsync(
            "new",
            TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task StatusCapacity_WithOnlyActiveJobsRejectsSubmissionWithoutEviction()
    {
        await using var app = await TestApp.StartAsync(
            configureOptions: options => options.StatusCapacity = 1);
        var accepted = await PostOrderAsync(app.Client);

        var rejected = await app.Client.PostAsJsonAsync(
            "/orders",
            new { data = new { name = "order-2" } },
            TestContext.Current.CancellationToken);
        var active = await app.Client.GetAsync(
            accepted.Location,
            TestContext.Current.CancellationToken);

        Assert.True(
            rejected.StatusCode == HttpStatusCode.ServiceUnavailable,
            $"Expected 503 but received {(int)rejected.StatusCode}: {await rejected.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)}");
        Assert.Equal(HttpStatusCode.OK, active.StatusCode);
        Assert.NotNull(await app.StatusStore.GetAsync(
            accepted.Id,
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
    public async Task ExternalResolver_WaitingExternalThenCompletedContinuesResolving()
    {
        var observedStatuses = new List<AsyncStatusResponse>();
        var resolver = new CapturingResolver(status =>
        {
            observedStatuses.Add(status);

            return status.Status == AsyncJobStatus.waiting_external && observedStatuses.Count == 1
                ? status with
                {
                    Result = new { attempt = 1 },
                    UpdatedAt = DateTimeOffset.UtcNow
                }
                : status with
                {
                    Status = AsyncJobStatus.completed,
                    Result = new { resolved = true },
                    UpdatedAt = DateTimeOffset.UtcNow
                };
        });
        await using var app = await TestApp.StartAsync(
            configureServices: services => services.AddSingleton<IExternalStatusResolver>(resolver),
            configureEndpoint: options => options.ExecutionMode = AsyncExecutionMode.wait_external);

        var accepted = await PostOrderAsync(app.Client);
        var status = await PollStatusAsync(app.Client, accepted.Location, AsyncJobStatus.completed);

        Assert.Equal(AsyncJobStatus.completed, status.Status);
        Assert.Equal(2, resolver.Calls);
        Assert.Equal(2, observedStatuses.Count);
        Assert.NotNull(observedStatuses[1].Result);
    }

    [Fact]
    public async Task ExternalResolver_PermanentWaitingExternalFailsAfterConfiguredAttempts()
    {
        var resolver = new CapturingResolver(status => status with
        {
            Status = AsyncJobStatus.waiting_external,
            UpdatedAt = DateTimeOffset.UtcNow
        });
        await using var app = await TestApp.StartAsync(
            configureServices: services => services.AddSingleton<IExternalStatusResolver>(resolver),
            configureOptions: options => options.ExternalResolutionMaxAttempts = 3,
            configureEndpoint: options => options.ExecutionMode = AsyncExecutionMode.wait_external);

        var accepted = await PostOrderAsync(app.Client);
        var status = await PollStatusAsync(app.Client, accepted.Location, AsyncJobStatus.failed);

        Assert.Equal(AsyncJobStatus.failed, status.Status);
        Assert.Equal(3, resolver.Calls);
    }

    [Fact]
    public async Task ExternalResolver_InvalidStatusesConsumeAttemptsAndFail()
    {
        var invalidStatuses = new[]
        {
            AsyncJobStatus.queued,
            AsyncJobStatus.processing,
            AsyncJobStatus.not_found
        };
        var resolverCallIndex = 0;
        var resolver = new CapturingResolver(status => status with
        {
            Status = invalidStatuses[Math.Min(invalidStatuses.Length - 1, resolverCallIndex++)],
            UpdatedAt = DateTimeOffset.UtcNow
        });
        await using var app = await TestApp.StartAsync(
            configureServices: services => services.AddSingleton<IExternalStatusResolver>(resolver),
            configureOptions: options => options.ExternalResolutionMaxAttempts = 3,
            configureEndpoint: options => options.ExecutionMode = AsyncExecutionMode.wait_external);

        var accepted = await PostOrderAsync(app.Client);
        var status = await PollStatusAsync(app.Client, accepted.Location, AsyncJobStatus.failed);

        Assert.Equal(AsyncJobStatus.failed, status.Status);
        Assert.Equal(3, resolver.Calls);
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
    public void RedisKeysWithoutMatchingHashTag_AreRejected()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddAsyncRequestReply();
        services.AddAsyncRequestReplyRedis(options =>
        {
            options.StreamKey = "{jobs}:stream";
            options.StatusKeyPrefix = "{statuses}:status:";
        });
        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => provider.GetRequiredService<IOptions<RedisAsyncRequestReplyOptions>>().Value);

        Assert.Contains("same non-empty hash tag", exception.Message);
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
        var streamKey = $"async-request-reply:{{{suffix}}}:jobs";
        var statusPrefix = $"async-request-reply:{{{suffix}}}:status:";
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
                AsyncJobStatus.processing,
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

            Assert.NotNull(await statusStore.GetAsync(
                "job-1",
                TestContext.Current.CancellationToken));
            Assert.True(await tokenStore.ValidateAsync(
                "job-1",
                "secret",
                TestContext.Current.CancellationToken));

            await statusStore.SetAsync(new AsyncStatusResponse(
                "job-1",
                AsyncJobStatus.completed,
                new { saved = true },
                null,
                now,
                DateTimeOffset.UtcNow),
                TestContext.Current.CancellationToken);
            await statusStore.BeginRetentionAsync(
                "job-1",
                TestContext.Current.CancellationToken);
            await tokenStore.BeginRetentionAsync(
                "job-1",
                TestContext.Current.CancellationToken);

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
    [Trait("Category", "RedisIntegration")]
    public async Task RedisSubmission_IsAtomicCapacitySafeAndIdempotentByJobId()
    {
        var configuration = Environment.GetEnvironmentVariable("ASYNC_REQUEST_REPLY_REDIS_CONNECTION");

        if (string.IsNullOrWhiteSpace(configuration))
        {
            Assert.Skip("Set ASYNC_REQUEST_REPLY_REDIS_CONNECTION to run the Redis integration test.");
        }

        var suffix = Guid.NewGuid().ToString("N");
        var streamKey = $"async-request-reply:{{{suffix}}}:jobs";
        var statusPrefix = $"async-request-reply:{{{suffix}}}:status:";
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddAsyncRequestReply();
        services.AddAsyncRequestReplyRedis(options =>
        {
            options.Configuration = configuration;
            options.StreamKey = streamKey;
            options.ConsumerGroup = $"group-{suffix}";
            options.StatusKeyPrefix = statusPrefix;
            options.MaxQueueLength = 1;
        });
        await using var provider = services.BuildServiceProvider();
        var submissionStore = provider.GetRequiredService<IAsyncJobSubmissionStore>();
        var statusStore = provider.GetRequiredService<IAsyncStatusStore>();
        var tokenStore = provider.GetRequiredService<IAsyncStatusTokenStore>();
        var connection = provider.GetRequiredService<StackExchange.Redis.IConnectionMultiplexer>();
        var database = connection.GetDatabase();

        try
        {
            await submissionStore.SubmitAsync(
                "job-1",
                new { value = 1 },
                AsyncExecutionMode.resolve_now,
                "secret-1",
                TestContext.Current.CancellationToken);
            await submissionStore.SubmitAsync(
                "job-1",
                new { value = 1 },
                AsyncExecutionMode.resolve_now,
                "secret-1",
                TestContext.Current.CancellationToken);

            Assert.Equal(1, await database.StreamLengthAsync(streamKey));
            Assert.NotNull(await statusStore.GetAsync(
                "job-1",
                TestContext.Current.CancellationToken));
            Assert.True(await tokenStore.ValidateAsync(
                "job-1",
                "secret-1",
                TestContext.Current.CancellationToken));

            await Assert.ThrowsAsync<AsyncQueueUnavailableException>(
                () => submissionStore.SubmitAsync(
                    "job-2",
                    new { value = 2 },
                    AsyncExecutionMode.resolve_now,
                    "secret-2",
                    TestContext.Current.CancellationToken).AsTask());

            Assert.Null(await statusStore.GetAsync(
                "job-2",
                TestContext.Current.CancellationToken));
            Assert.False(await tokenStore.ValidateAsync(
                "job-2",
                "secret-2",
                TestContext.Current.CancellationToken));
        }
        finally
        {
            await database.KeyDeleteAsync(
                [
                    streamKey,
                    $"{statusPrefix}job-1",
                    $"{statusPrefix}access:job-1",
                    $"{statusPrefix}job-2",
                    $"{statusPrefix}access:job-2"
                ]);
        }
    }

    [Fact]
    [Trait("Category", "RedisIntegration")]
    public async Task RedisPendingSweep_UsesAutoClaimCursorToRecoverEleventhEntry()
    {
        var configuration = Environment.GetEnvironmentVariable("ASYNC_REQUEST_REPLY_REDIS_CONNECTION");

        if (string.IsNullOrWhiteSpace(configuration))
        {
            Assert.Skip("Set ASYNC_REQUEST_REPLY_REDIS_CONNECTION to run the Redis integration test.");
        }

        var suffix = Guid.NewGuid().ToString("N");
        var streamKey = $"async-request-reply:{{{suffix}}}:jobs";
        var statusPrefix = $"async-request-reply:{{{suffix}}}:status:";
        var group = $"group-{suffix}";
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddAsyncRequestReply(options =>
        {
            options.DeliveryLeaseRenewalInterval = TimeSpan.FromMilliseconds(100);
        });
        services.AddAsyncRequestReplyRedis(options =>
        {
            options.Configuration = configuration;
            options.StreamKey = streamKey;
            options.ConsumerGroup = group;
            options.ConsumerName = $"recovering-{suffix}";
            options.StatusKeyPrefix = statusPrefix;
            options.MaxQueueLength = 20;
            options.ClaimIdleTime = TimeSpan.FromSeconds(1);
        });
        await using var provider = services.BuildServiceProvider();
        var queue = provider.GetRequiredService<IAsyncJobQueue>();
        var reader = provider.GetRequiredService<IAsyncJobQueueReader>();
        var connection = provider.GetRequiredService<StackExchange.Redis.IConnectionMultiplexer>();
        var database = connection.GetDatabase();

        try
        {
            for (var index = 1; index <= 11; index++)
            {
                await queue.EnqueueAsync(
                    $"job-{index}",
                    new { value = index },
                    AsyncExecutionMode.resolve_now,
                    TestContext.Current.CancellationToken);
            }

            await database.StreamCreateConsumerGroupAsync(
                streamKey,
                group,
                "0-0",
                createStream: false);
            var pending = await database.StreamReadGroupAsync(
                streamKey,
                group,
                $"original-{suffix}",
                ">",
                count: 11);
            Assert.Equal(11, pending.Length);

            await Task.Delay(
                TimeSpan.FromMilliseconds(1_100),
                TestContext.Current.CancellationToken);
            await database.StreamClaimIdsOnlyAsync(
                streamKey,
                group,
                $"original-{suffix}",
                0,
                pending.Take(10).Select(entry => entry.Id).ToArray());

            var recovered = await ReadOneAsync(reader);

            Assert.Equal(pending[10].Id.ToString(), recovered.DeliveryId);
            Assert.Equal("job-11", recovered.Job.Id);
        }
        finally
        {
            await database.KeyDeleteAsync(streamKey);
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
            services.AddSingleton<IAsyncJobSubmissionStore>(provider =>
                new TestSubmissionStore(
                    queue,
                    provider.GetRequiredService<IAsyncStatusStore>(),
                    provider.GetRequiredService<IAsyncStatusTokenStore>()));
        });

        var accepted = await PostOrderAsync(app.Client);
        var status = await PollStatusAsync(app.Client, accepted.Location, AsyncJobStatus.completed);

        Assert.Equal(AsyncJobStatus.completed, status.Status);
        Assert.True(queue.DequeueStarted);
    }

    [Fact]
    public async Task RecoveredTerminalDelivery_DoesNotRunProcessorAgain()
    {
        var queue = new RedeliveringQueue();
        var processor = new CountingProcessor();
        await using var app = await TestApp.StartAsync(processor, services =>
        {
            services.AddSingleton<IAsyncJobQueue>(queue);
            services.AddSingleton<IAsyncJobQueueReader>(queue);
            services.AddSingleton<IAsyncJobSubmissionStore>(provider =>
                new TestSubmissionStore(
                    queue,
                    provider.GetRequiredService<IAsyncStatusStore>(),
                    provider.GetRequiredService<IAsyncStatusTokenStore>()));
        });

        var accepted = await PostOrderAsync(app.Client);
        await queue.SecondCompletion.Task.WaitAsync(
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken);
        var status = await app.StatusStore.GetAsync(
            accepted.Id,
            TestContext.Current.CancellationToken);

        Assert.NotNull(status);
        Assert.Equal(AsyncJobStatus.completed, status!.Status);
        Assert.Equal(1, processor.Calls);
    }

    [Fact]
    public async Task BackgroundWorker_RetriesQueueReaderAfterUnavailableFailure()
    {
        var queue = new FlakyQueue();
        var processor = new CapturingProcessor(_ => new { saved = true });
        await using var app = await TestApp.StartAsync(
            processor,
            services =>
            {
                services.AddSingleton<IAsyncJobQueue>(queue);
                services.AddSingleton<IAsyncJobQueueReader>(queue);
                services.AddSingleton<IAsyncJobSubmissionStore>(provider =>
                    new TestSubmissionStore(
                        queue,
                        provider.GetRequiredService<IAsyncStatusStore>(),
                        provider.GetRequiredService<IAsyncStatusTokenStore>()));
            },
            options => options.WorkerRecoveryInterval = TimeSpan.FromMilliseconds(10));

        var accepted = await PostOrderAsync(app.Client);
        var status = await PollStatusAsync(app.Client, accepted.Location, AsyncJobStatus.completed);

        Assert.Equal(AsyncJobStatus.completed, status.Status);
        Assert.True(queue.DequeueAttempts >= 2);
    }

    [Fact]
    public async Task ActiveJob_KeepsStatusAndCapabilityUntilTerminalRetentionStarts()
    {
        var processor = new ControlledProcessor();
        await using var app = await TestApp.StartAsync(
            processor,
            configureOptions: options => options.StatusTimeToLive = TimeSpan.FromMilliseconds(200));

        var accepted = await PostOrderAsync(app.Client);
        await processor.Started.Task.WaitAsync(
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken);
        await Task.Delay(350, TestContext.Current.CancellationToken);

        var active = await app.Client.GetFromJsonAsync<AsyncStatusResponse>(
            accepted.Location,
            JsonOptions,
            TestContext.Current.CancellationToken);

        Assert.NotNull(active);
        Assert.Equal(AsyncJobStatus.processing, active!.Status);

        processor.Release.TrySetResult();
        var completed = await PollStatusAsync(
            app.Client,
            accepted.Location,
            AsyncJobStatus.completed);

        Assert.Equal(AsyncJobStatus.completed, completed.Status);

        await Task.Delay(350, TestContext.Current.CancellationToken);
        var expired = await app.Client.GetAsync(
            accepted.Location,
            TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.NotFound, expired.StatusCode);
    }

    [Fact]
    public async Task WaitingExternalWithoutResolver_StartsTerminalRetention()
    {
        await using var app = await TestApp.StartAsync(
            configureOptions: options => options.StatusTimeToLive = TimeSpan.FromMilliseconds(200),
            configureEndpoint: options => options.ExecutionMode = AsyncExecutionMode.wait_external);

        var accepted = await PostOrderAsync(app.Client);
        await PollStatusAsync(app.Client, accepted.Location, AsyncJobStatus.waiting_external);
        await Task.Delay(350, TestContext.Current.CancellationToken);

        var expired = await app.Client.GetAsync(
            accepted.Location,
            TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.NotFound, expired.StatusCode);
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
        Assert.Single(services, descriptor => descriptor.ServiceType == typeof(IAsyncJobSubmissionStore));
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

    private static AsyncStatusResponse StatusResponseFactoryForTest(
        string jobId,
        AsyncJobStatus status)
    {
        var now = DateTimeOffset.UtcNow;
        return new AsyncStatusResponse(jobId, status, null, null, now, now);
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

    private sealed class CountingProcessor : IAsyncJobProcessor
    {
        private int calls;

        public int Calls => Volatile.Read(ref calls);

        public Task<object?> ProcessAsync(
            string jobId,
            object? payload,
            CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref calls);
            return Task.FromResult<object?>(new { saved = true });
        }
    }

    private sealed class ControlledProcessor : IAsyncJobProcessor
    {
        public TaskCompletionSource Started { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task<object?> ProcessAsync(
            string jobId,
            object? payload,
            CancellationToken cancellationToken = default)
        {
            Started.TrySetResult();
            await Release.Task.WaitAsync(cancellationToken);
            return new { saved = true };
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

    private sealed class RedeliveringQueue : IAsyncJobQueue, IAsyncJobQueueReader
    {
        private readonly Channel<AsyncJobDelivery> channel = Channel.CreateUnbounded<AsyncJobDelivery>();
        private int completions;

        public TaskCompletionSource SecondCompletion { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ValueTask EnqueueAsync(
            string jobId,
            object? payload,
            AsyncExecutionMode executionMode,
            CancellationToken cancellationToken = default)
        {
            return channel.Writer.WriteAsync(
                new AsyncJobDelivery(
                    "delivery-1",
                    new AsyncJobEnvelope(jobId, payload, executionMode)),
                cancellationToken);
        }

        public async IAsyncEnumerable<AsyncJobDelivery> DequeueAllAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await foreach (var delivery in channel.Reader.ReadAllAsync(cancellationToken))
            {
                yield return delivery;
            }
        }

        public ValueTask CompleteAsync(
            AsyncJobDelivery delivery,
            CancellationToken cancellationToken = default)
        {
            if (Interlocked.Increment(ref completions) == 1)
            {
                return channel.Writer.WriteAsync(delivery, cancellationToken);
            }

            SecondCompletion.TrySetResult();
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
            return ValueTask.CompletedTask;
        }
    }

    private sealed class FlakyQueue : IAsyncJobQueue, IAsyncJobQueueReader
    {
        private readonly Channel<AsyncJobEnvelope> channel = Channel.CreateUnbounded<AsyncJobEnvelope>();
        private int dequeueAttempts;

        public int DequeueAttempts => Volatile.Read(ref dequeueAttempts);

        public ValueTask EnqueueAsync(
            string jobId,
            object? payload,
            AsyncExecutionMode executionMode,
            CancellationToken cancellationToken = default)
        {
            return channel.Writer.WriteAsync(
                new AsyncJobEnvelope(jobId, payload, executionMode),
                cancellationToken);
        }

        public async IAsyncEnumerable<AsyncJobDelivery> DequeueAllAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            if (Interlocked.Increment(ref dequeueAttempts) == 1)
            {
                throw new AsyncQueueUnavailableException("transient");
            }

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
            return ValueTask.CompletedTask;
        }
    }

    private sealed class TestSubmissionStore(
        IAsyncJobQueue queue,
        IAsyncStatusStore statusStore,
        IAsyncStatusTokenStore tokenStore) : IAsyncJobSubmissionStore
    {
        public async ValueTask SubmitAsync(
            string jobId,
            object? payload,
            AsyncExecutionMode executionMode,
            string? accessToken,
            CancellationToken cancellationToken = default)
        {
            await statusStore.SetAsync(
                StatusResponseFactoryForTest(jobId, AsyncJobStatus.queued),
                cancellationToken);

            if (accessToken is not null)
            {
                await tokenStore.SetAsync(jobId, accessToken, cancellationToken);
            }

            try
            {
                await queue.EnqueueAsync(jobId, payload, executionMode, cancellationToken);
            }
            catch
            {
                await statusStore.DeleteAsync(jobId, CancellationToken.None);
                await tokenStore.DeleteAsync(jobId, CancellationToken.None);
                throw;
            }
        }
    }

    private sealed class RejectingSubmissionStore : IAsyncJobSubmissionStore
    {
        public ValueTask SubmitAsync(
            string jobId,
            object? payload,
            AsyncExecutionMode executionMode,
            string? accessToken,
            CancellationToken cancellationToken = default)
        {
            throw new AsyncQueueUnavailableException("full");
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
