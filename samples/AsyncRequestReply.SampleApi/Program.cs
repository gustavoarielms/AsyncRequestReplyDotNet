using System.Security.Cryptography;
using AsyncRequestReply;

var builder = WebApplication.CreateBuilder(args);
var submissionIdentitySecret = builder.Configuration["AsyncRequestReply:SubmissionIdentitySecret"]
    ?? Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));

builder.Services.AddAsyncRequestReply(options =>
{
    options.StatusBasePath = "/async-status";
    options.AllowCapabilityStatusAccess = true;
    options.SubmissionIdentitySecret = submissionIdentitySecret;
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
