# AsyncRequestReply

`AsyncRequestReply` is a minimal ASP.NET Core library for the Asynchronous Request-Reply pattern.

It lets an HTTP endpoint accept work, enqueue it, return `202 Accepted` immediately, and expose a polling endpoint where clients can check the job status later. The library owns the HTTP pattern and status contract. The host application owns the real business processing.

By default, the package uses in-memory queue and status store implementations. Redis can be enabled with an opt-in service registration when the host application needs the queue and statuses outside process memory.

## Local package installation

Build the NuGet package locally:

```bash
dotnet pack src/AsyncRequestReply/AsyncRequestReply.csproj -c Release
```

Install it from the generated package folder:

```bash
dotnet add package AsyncRequestReply --source ./src/AsyncRequestReply/bin/Release
```

## ASP.NET Core configuration

```csharp
using AsyncRequestReply;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddAsyncRequestReply(options =>
{
    options.StatusBasePath = "/async-status";
    options.AllowCapabilityStatusAccess = true;
    options.QueueCapacity = 1_000;
    options.StatusTimeToLive = TimeSpan.FromHours(1);
    options.WorkerRecoveryInterval = TimeSpan.FromSeconds(1);
});

builder.Services.AddSingleton<IAsyncJobProcessor, OrderProcessor>();

var app = builder.Build();

app.MapAsyncRequestReplyStatusEndpoints();

app.MapPost("/orders", () => Results.NoContent())
    .AsAsyncRequestReply(options =>
    {
        options.PayloadPath = "data";
        options.MaxPayloadBytes = 1_048_576;
    });

app.Run();
```

If no `IAsyncJobProcessor` is registered, accepted jobs remain in `queued` until a host application provides processing.

## Redis transport

Call `AddAsyncRequestReplyRedis(...)` after `AddAsyncRequestReply(...)` to replace the in-memory queue and status store with Redis-backed implementations:

```csharp
builder.Services.AddAsyncRequestReply(options =>
{
    options.StatusBasePath = "/async-status";
    options.AllowCapabilityStatusAccess = true;
});

builder.Services.AddAsyncRequestReplyRedis(options =>
{
    options.Configuration = builder.Configuration.GetConnectionString("Redis") ?? "localhost:6379";
    options.StreamKey = "{async-request-reply}:jobs";
    options.ConsumerGroup = "orders";
    options.StatusKeyPrefix = "{async-request-reply}:status:";
    options.StatusTimeToLive = TimeSpan.FromHours(1);
    options.MaxQueueLength = 10_000;
});
```

Redis support uses Streams and consumer groups, requires Redis 6.2 or later, and acknowledges a delivery before deleting it. Pending deliveries whose lease expires can be claimed by another instance, and a missing consumer group is recreated automatically. Submission atomically checks stream capacity, adds the delivery, and creates its queued status and capability hash. Retrying the same `jobId` is idempotent and does not add a second stream entry. Transient Redis failures are retried with the same `jobId` until `EnqueueTimeout` is exhausted. If that timeout expires while the Redis command is still in flight, the endpoint returns `503`; retrying with the same required HTTP `Idempotency-Key` safely observes or reuses the ambiguous submission instead of adding another stream entry.

`ConsumerName` is an observability prefix. Each store instance appends a random suffix so two live workers cannot share the lease-fencing identity even when they use the same configured value.

`StreamKey` and `StatusKeyPrefix` must contain the same non-empty Redis hash tag, as in the defaults above. This keeps all keys used by the submission Lua script in one Redis Cluster hash slot.

Delivery remains at-least-once: a worker can receive the same job again after a lost ACK, expired lease, or ambiguous connection failure. Every `IAsyncJobProcessor` implementation must therefore be idempotent for a given `jobId`. Redis persists terminal state, starts status and capability retention, and acknowledges the delivery atomically only while the worker still owns its lease, preventing a stale consumer from overwriting the winner.

Remote Redis endpoints require TLS. `AllowUnencryptedRemoteConnection` is an explicit opt-out for a trusted private network; loopback development connections remain allowed without TLS.

Hosts that provide a distributed custom transport should register `IAsyncJobSubmissionStore`, `IAsyncJobQueue`, `IAsyncJobQueueReader`, `IAsyncStatusStore`, and `IAsyncStatusTokenStore`. `IAsyncJobSubmissionStore.SubmitAsync` is the HTTP acceptance boundary: it must initialize queued status and capability before making the delivery visible, and cleanly reject the entire submission if it cannot do so. Custom status stores keep active entries available until `BeginRetentionAsync` starts their terminal retention period.

## Async endpoint

The endpoint marked with `.AsAsyncRequestReply(...)` reads the request body, extracts the configured payload, stores the initial status, enqueues the job, and returns:

```json
{
  "id": "{jobId}",
  "status": "queued",
  "location": "/async-status/status/{jobId}/{accessToken}"
}
```

The HTTP response status is `202 Accepted` and the `Location` header contains the polling URL. The access token is derived separately from the job identifier and only its SHA-256 hash is stored.

Every submission requires exactly one HTTP `Idempotency-Key` containing 16 to 256 UTF-8 bytes. Generate it with a cryptographically secure random source (a UUID is sufficient) and reuse it after `503`, a connection loss, or client-side cancellation. The method and route scope the key: while its status is retained, retries return the same `jobId` and polling location, and the first accepted payload wins. Missing or invalid keys return `400 Bad Request`.

With `PayloadPath = "data"`, this request enqueues only the nested `data` object:

```json
{
  "data": {
    "name": "order-1"
  }
}
```

## Processor example

```csharp
using AsyncRequestReply;

public sealed class OrderProcessor : IAsyncJobProcessor
{
    public Task<object?> ProcessAsync(string jobId, object? payload, CancellationToken cancellationToken = default)
    {
        return Task.FromResult<object?>(new
        {
            message = "Order processed",
            jobId,
            payload
        });
    }
}
```

Successful processors move the job to `completed`. Exceptions move the job to `failed`, return a generic public error containing the job reference, and keep the complete exception only in protected application logs.

## Status endpoint

Register status polling with:

```csharp
app.MapAsyncRequestReplyStatusEndpoints();
```

Mapping the endpoint is explicit. Access is denied by default unless the host either enables `AllowCapabilityStatusAccess` or registers an `IAsyncStatusAccessPolicy`. A host using authenticated ownership checks can configure:

```csharp
builder.Services.AddSingleton<IAsyncStatusAccessPolicy, JobOwnerAccessPolicy>();

app.MapAsyncRequestReplyStatusEndpoints()
    .RequireAuthorization();
```

Policy denials return `404` so the endpoint does not disclose whether a job exists. A registered policy takes precedence over capability mode.

Default route:

```text
GET /async-status/status/{jobId}/{accessToken}
```

Response body:

```json
{
  "id": "{jobId}",
  "status": "queued",
  "result": null,
  "error": null,
  "createdAt": "2026-06-08T00:00:00+00:00",
  "updatedAt": "2026-06-08T00:00:00+00:00"
}
```

Supported statuses are `queued`, `processing`, `waiting_external`, `completed`, `failed`, and `not_found`.

Status responses include `Cache-Control: no-store`. The GET endpoint is read-only; an `IExternalStatusResolver` is invoked by the background worker with bounded attempts and per-attempt timeouts, never by polling requests. A resolver may return `waiting_external` to persist progress and continue resolving, `completed` or `failed` to finish, or `null` to keep the last status and retry. Returning `queued`, `processing`, or `not_found` is invalid and consumes an attempt. Exhausting the configured attempts stores `failed`, starts retention, and acknowledges the delivery.

## Limits and retention

The in-memory queue and status store are bounded. `AsyncRequestReplyOptions` controls queue capacity and enqueue timeout, status capacity and terminal-state TTL, worker concurrency and recovery, delivery lease renewal, and external-resolution retry behavior. Queue capacity remains reserved until a delivery completes, so a redelivery can always be returned to the channel. Active jobs do not expire while queued or processing. When `StatusCapacity` is reached, expired entries are removed first and then the oldest retained terminal pair (status plus capability) is evicted. If every entry is active, the new submission returns `503 Service Unavailable`; an accepted active job is never evicted. A full queue also returns `503` with `Retry-After`; an oversized endpoint payload returns `413 Payload Too Large`.

## Commands

```bash
dotnet build
dotnet test
dotnet pack src/AsyncRequestReply/AsyncRequestReply.csproj -c Release
dotnet list package --vulnerable --include-transitive
```

Redis integration tests verify queue capacity, atomic/idempotent submission, ambiguous submission retries, pending-delivery cursor and consumer-group recovery, lease-fenced terminal completion, acknowledgements, and real TTL behavior:

```bash
ASYNC_REQUEST_REPLY_REDIS_CONNECTION=localhost:6379 \
  dotnet test --filter Category=RedisIntegration
```

## NuGet sample app

This repository also includes a standalone sample that consumes the generated NuGet package instead of the local project:

```bash
dotnet pack src/AsyncRequestReply/AsyncRequestReply.csproj \
  --configuration Release \
  --output ./artifacts

dotnet restore \
  samples/AsyncRequestReply.NuGetSampleApi/AsyncRequestReply.NuGetSampleApi.csproj \
  --configfile samples/AsyncRequestReply.NuGetSampleApi/NuGet.config

dotnet run \
  --project samples/AsyncRequestReply.NuGetSampleApi \
  --no-restore
```

The app listens on `http://localhost:5088` and uses only the package's built-in in-memory queue and status store, so no Redis, database, or broker is required unless you opt into Redis.

Submit work:

```bash
curl -i http://localhost:5088/invoices \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: 550e8400-e29b-41d4-a716-446655440000" \
  -d '{"data":{"number":"INV-1001","customer":"Patxa","amount":1250.75}}'
```

Poll the returned `location`:

```bash
curl http://localhost:5088/async-status/status/{jobId}/{accessToken}
```
