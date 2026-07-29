using AsyncRequestReply;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddAsyncRequestReply(options =>
{
    options.StatusBasePath = "/async-status";
    options.ExposeStatusEndpoint = true;
});
builder.Services.AddSingleton<IAsyncJobProcessor, InvoiceProcessor>();

var app = builder.Build();

app.MapGet("/", () => Results.Ok(new
{
    service = "AsyncRequestReply NuGet sample",
    submit = "POST /invoices",
    status = "GET /async-status/status/{jobId}"
}));

app.MapAsyncRequestReplyStatusEndpoints();

app.MapPost("/invoices", () => Results.NoContent())
    .AsAsyncRequestReply(options => options.PayloadPath = "data");

app.Run();

internal sealed class InvoiceProcessor : IAsyncJobProcessor
{
    public async Task<object?> ProcessAsync(string jobId, object? payload, CancellationToken cancellationToken = default)
    {
        await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);

        return new
        {
            message = "Invoice processed",
            jobId,
            payload,
            processedAt = DateTimeOffset.UtcNow
        };
    }
}
