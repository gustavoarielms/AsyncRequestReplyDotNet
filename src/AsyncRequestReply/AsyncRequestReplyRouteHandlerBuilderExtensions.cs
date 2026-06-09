using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using AsyncRequestReply.Internal;

namespace AsyncRequestReply;

public static class AsyncRequestReplyRouteHandlerBuilderExtensions
{
    public static RouteHandlerBuilder AsAsyncRequestReply(
        this RouteHandlerBuilder builder,
        Action<AsyncEndpointOptions>? configure = null)
    {
        var endpointOptions = new AsyncEndpointOptions();
        configure?.Invoke(endpointOptions);

        builder.AddEndpointFilter(async (context, _) =>
        {
            var httpContext = context.HttpContext;
            var cancellationToken = httpContext.RequestAborted;
            var payload = await PayloadReader.ReadAsync(httpContext.Request, endpointOptions.PayloadPath, cancellationToken);

            if (payload is null)
            {
                return Results.BadRequest(new
                {
                    error = endpointOptions.PayloadPath is null
                        ? "Request body is required"
                        : $"Payload field \"{endpointOptions.PayloadPath}\" is required"
                });
            }

            var jobId = Guid.NewGuid().ToString("N");
            var statusStore = httpContext.RequestServices.GetRequiredService<IAsyncStatusStore>();
            var queue = httpContext.RequestServices.GetRequiredService<IAsyncJobQueue>();
            var requestReplyOptions = httpContext.RequestServices.GetRequiredService<IOptions<AsyncRequestReplyOptions>>();
            var location = StatusLocationBuilder.Build(requestReplyOptions, jobId);

            await statusStore.SetAsync(StatusResponseFactory.Queued(jobId), cancellationToken);
            await queue.EnqueueAsync(jobId, payload, endpointOptions.ExecutionMode, cancellationToken);

            var response = new AsyncAcceptedResponse(jobId, AsyncJobStatus.queued, location);
            return Results.Accepted(location, response);
        });

        return builder;
    }
}
