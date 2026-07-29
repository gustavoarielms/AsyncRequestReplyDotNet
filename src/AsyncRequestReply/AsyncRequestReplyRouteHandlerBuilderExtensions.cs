using System.Text.Json;
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

        if (endpointOptions.MaxPayloadBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(endpointOptions.MaxPayloadBytes),
                "MaxPayloadBytes must be greater than zero.");
        }

        builder.AddEndpointFilter(async (context, _) =>
        {
            var httpContext = context.HttpContext;
            var cancellationToken = httpContext.RequestAborted;
            object? payload;

            try
            {
                payload = await PayloadReader.ReadAsync(
                    httpContext.Request,
                    endpointOptions.PayloadPath,
                    endpointOptions.MaxPayloadBytes,
                    cancellationToken);
            }
            catch (PayloadTooLargeException)
            {
                return Results.Json(
                    new { error = "Request payload is too large." },
                    statusCode: StatusCodes.Status413PayloadTooLarge);
            }
            catch (JsonException)
            {
                return Results.BadRequest(new { error = "Request body must contain valid JSON." });
            }

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
            var submissionStore = httpContext.RequestServices.GetRequiredService<IAsyncJobSubmissionStore>();
            var requestReplyOptions = httpContext.RequestServices.GetRequiredService<IOptions<AsyncRequestReplyOptions>>();
            var accessToken = requestReplyOptions.Value.AllowCapabilityStatusAccess
                ? StatusAccessToken.Create()
                : null;
            var location = StatusLocationBuilder.Build(requestReplyOptions, jobId, accessToken);

            try
            {
                await submissionStore.SubmitAsync(
                    jobId,
                    payload,
                    endpointOptions.ExecutionMode,
                    accessToken,
                    cancellationToken);
            }
            catch (AsyncQueueUnavailableException)
            {
                httpContext.Response.Headers.RetryAfter = "1";

                return Results.Json(
                    new { error = "The async job queue is temporarily unavailable." },
                    statusCode: StatusCodes.Status503ServiceUnavailable);
            }

            var response = new AsyncAcceptedResponse(jobId, AsyncJobStatus.queued, location);
            return Results.Accepted(location, response);
        });

        return builder;
    }
}
