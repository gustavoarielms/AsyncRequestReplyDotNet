using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

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

        var response = await app.Client.PostAsJsonAsync("/orders", new { data = new { name = "order-1" } });

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
    }

    [Fact]
    public async Task AsyncEndpoint_IncludesLocationHeader()
    {
        await using var app = await TestApp.StartAsync();

        var response = await app.Client.PostAsJsonAsync("/orders", new { data = new { name = "order-1" } });

        Assert.NotNull(response.Headers.Location);
        Assert.StartsWith("/async-status/status/", response.Headers.Location!.OriginalString);
    }

    [Fact]
    public async Task AsyncEndpoint_ReturnsAcceptedBody()
    {
        await using var app = await TestApp.StartAsync();

        var response = await app.Client.PostAsJsonAsync("/orders", new { data = new { name = "order-1" } });
        var body = await response.Content.ReadFromJsonAsync<AsyncAcceptedResponse>(JsonOptions);

        Assert.NotNull(body);
        Assert.False(string.IsNullOrWhiteSpace(body!.Id));
        Assert.Equal(AsyncJobStatus.queued, body.Status);
        Assert.Equal($"/async-status/status/{body.Id}", body.Location);
    }

    [Fact]
    public async Task AsyncEndpoint_StoresInitialQueuedStatus()
    {
        await using var app = await TestApp.StartAsync();

        var accepted = await PostOrderAsync(app.Client);

        var stored = await app.StatusStore.GetAsync(accepted.Id);
        Assert.NotNull(stored);
        Assert.Equal(AsyncJobStatus.queued, stored!.Status);
    }

    [Fact]
    public async Task StatusEndpoint_ReturnsStoredJobStatus()
    {
        await using var app = await TestApp.StartAsync();
        var accepted = await PostOrderAsync(app.Client);

        var status = await app.Client.GetFromJsonAsync<AsyncStatusResponse>(accepted.Location, JsonOptions);

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
        var processor = new CapturingProcessor(_ => throw new InvalidOperationException("processor failed"));
        await using var app = await TestApp.StartAsync(processor);

        var accepted = await PostOrderAsync(app.Client);
        var status = await PollStatusAsync(app.Client, accepted.Location, AsyncJobStatus.failed);

        Assert.Equal(AsyncJobStatus.failed, status.Status);
        Assert.Equal("processor failed", status.Error);
    }

    [Fact]
    public async Task PayloadPath_ExtractsConfiguredBodyProperty()
    {
        var processor = new CapturingProcessor(payload => payload);
        await using var app = await TestApp.StartAsync(processor);

        await PostOrderAsync(app.Client, new { data = new { name = "nested" }, ignored = true });
        var payload = await processor.Payload.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var json = Assert.IsType<JsonElement>(payload);
        Assert.Equal("nested", json.GetProperty("name").GetString());
        Assert.False(json.TryGetProperty("ignored", out _));
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

    private sealed class CapturingProcessor(Func<object?, object?> process) : IAsyncJobProcessor
    {
        public TaskCompletionSource<object?> Payload { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<object?> ProcessAsync(string jobId, object? payload, CancellationToken cancellationToken = default)
        {
            Payload.TrySetResult(payload);
            return Task.FromResult(process(payload));
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

        public static async Task<TestApp> StartAsync(IAsyncJobProcessor? processor = null)
        {
            var builder = WebApplication.CreateBuilder();
            builder.WebHost.UseUrls("http://127.0.0.1:0");
            builder.Services.AddAsyncRequestReply(options =>
            {
                options.StatusBasePath = "/async-status";
                options.ExposeStatusEndpoint = true;
            });

            if (processor is not null)
            {
                builder.Services.AddSingleton(processor);
            }

            var app = builder.Build();
            app.MapAsyncRequestReplyStatusEndpoints();
            app.MapPost("/orders", () => Results.NoContent())
                .AsAsyncRequestReply(options => options.PayloadPath = "data");

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
