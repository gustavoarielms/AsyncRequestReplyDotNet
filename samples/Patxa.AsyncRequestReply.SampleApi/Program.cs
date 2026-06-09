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

internal sealed class OrderProcessor : IAsyncJobProcessor
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
