# Patxa.AsyncRequestReply

`Patxa.AsyncRequestReply` is a minimal ASP.NET Core library for the Asynchronous Request-Reply pattern.

It lets an HTTP endpoint accept work, enqueue it, return `202 Accepted` immediately, and expose a polling endpoint where clients can check the job status later. The library owns the HTTP pattern and status contract. The host application owns the real business processing.

This first version uses in-memory queue and status store implementations. Redis, Hangfire, MassTransit, or any durable transport can be added later by replacing the public interfaces.

## Local package installation

Build the NuGet package locally:

```bash
dotnet pack src/Patxa.AsyncRequestReply/Patxa.AsyncRequestReply.csproj -c Release
```

Install it from the generated package folder:

```bash
dotnet add package Patxa.AsyncRequestReply --source ./src/Patxa.AsyncRequestReply/bin/Release
```

## ASP.NET Core configuration

```csharp
using Patxa.AsyncRequestReply;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddAsyncRequestReply(options =>
{
    options.StatusBasePath = "/async-status";
    options.ExposeStatusEndpoint = true;
});

builder.Services.AddSingleton<IAsyncJobProcessor, OrderProcessor>();

var app = builder.Build();

app.MapAsyncRequestReplyStatusEndpoints();

app.MapPost("/orders", () => Results.NoContent())
    .AsAsyncRequestReply(options => options.PayloadPath = "data");

app.Run();
```

If no `IAsyncJobProcessor` is registered, accepted jobs remain in `queued` until a host application provides processing.

## Async endpoint

The endpoint marked with `.AsAsyncRequestReply(...)` reads the request body, extracts the configured payload, stores the initial status, enqueues the job, and returns:

```json
{
  "id": "{jobId}",
  "status": "queued",
  "location": "/async-status/status/{jobId}"
}
```

The HTTP response status is `202 Accepted` and the `Location` header contains the polling URL.

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
using Patxa.AsyncRequestReply;

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

Successful processors move the job to `completed`. Exceptions move the job to `failed` and store the exception message in `error`.

## Status endpoint

Register status polling with:

```csharp
app.MapAsyncRequestReplyStatusEndpoints();
```

Default route:

```text
GET /async-status/status/{jobId}
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

## Commands

```bash
dotnet build
dotnet test
dotnet pack src/Patxa.AsyncRequestReply/Patxa.AsyncRequestReply.csproj -c Release
```
