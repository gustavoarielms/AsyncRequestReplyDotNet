using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using AsyncRequestReply.Internal;

namespace AsyncRequestReply;

public static class AsyncRequestReplyRouteHandlerBuilderExtensions
{
    private const int MinimumIdempotencyKeyBytes = 16;
    private const int MaximumIdempotencyKeyBytes = 256;

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
            var idempotencyKey = ReadIdempotencyKey(httpContext.Request);

            if (idempotencyKey is null)
            {
                return Results.BadRequest(new
                {
                    error = $"Idempotency-Key must contain between {MinimumIdempotencyKeyBytes} and {MaximumIdempotencyKeyBytes} UTF-8 bytes."
                });
            }

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

            var requestReplyOptions = httpContext.RequestServices.GetRequiredService<IOptions<AsyncRequestReplyOptions>>();
            var scope = CreateSubmissionScope(httpContext.Request, idempotencyKey);
            var jobId = CreateJobId(scope);
            var submissionStore = httpContext.RequestServices.GetRequiredService<IAsyncJobSubmissionStore>();
            var accessToken = requestReplyOptions.Value.AllowCapabilityStatusAccess
                ? CreateAccessToken(scope, requestReplyOptions.Value.SubmissionIdentitySecret!)
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

    private static string? ReadIdempotencyKey(HttpRequest request)
    {
        var values = request.Headers["Idempotency-Key"];

        if (values.Count != 1)
        {
            return null;
        }

        var value = values[0];

        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var byteCount = Encoding.UTF8.GetByteCount(value);
        return byteCount is >= MinimumIdempotencyKeyBytes and <= MaximumIdempotencyKeyBytes
            ? value
            : null;
    }

    private static string CreateSubmissionScope(
        HttpRequest request,
        string idempotencyKey)
    {
        return $"{request.Method}\n{request.PathBase}{request.Path}\n{idempotencyKey}";
    }

    private static string CreateJobId(string scope)
    {
        var jobHash = SHA256.HashData(Encoding.UTF8.GetBytes($"async-request-reply:job\n{scope}"));
        return Convert.ToHexString(jobHash.AsSpan(0, 16)).ToLowerInvariant();
    }

    private static string CreateAccessToken(string scope, string secret)
    {
        var accessHash = HMACSHA256.HashData(
            Encoding.UTF8.GetBytes(secret),
            Encoding.UTF8.GetBytes($"async-request-reply:access\n{scope}"));
        return WebEncoders.Base64UrlEncode(accessHash);
    }
}
