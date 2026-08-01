using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using AsyncRequestReply.Internal;

namespace AsyncRequestReply;

public static class AsyncRequestReplyEndpointRouteBuilderExtensions
{
    public static RouteHandlerBuilder MapAsyncRequestReplyStatusEndpoints(this IEndpointRouteBuilder endpoints)
    {
        var options = endpoints.ServiceProvider.GetRequiredService<IOptions<AsyncRequestReplyOptions>>().Value;
        var basePath = NormalizeBasePath(options.StatusBasePath);

        return endpoints.MapGet($"{basePath}/status/{{jobId}}/{{accessToken?}}", async (
            HttpContext httpContext,
            string jobId,
            string? accessToken,
            IAsyncStatusStore statusStore,
            IAsyncStatusTokenStore tokenStore,
            CancellationToken cancellationToken) =>
        {
            httpContext.Response.Headers.CacheControl = "no-store";

            var accessPolicy = httpContext.RequestServices.GetService<IAsyncStatusAccessPolicy>();
            var canRead = accessPolicy is not null
                ? await accessPolicy.CanReadAsync(httpContext, jobId, cancellationToken)
                : options.AllowCapabilityStatusAccess
                    && !string.IsNullOrWhiteSpace(accessToken)
                    && await tokenStore.ValidateAsync(jobId, accessToken, cancellationToken);

            if (!canRead)
            {
                return Results.NotFound(StatusResponseFactory.NotFound(jobId));
            }

            var status = await statusStore.GetAsync(jobId, cancellationToken);

            if (status is null)
            {
                return Results.NotFound(StatusResponseFactory.NotFound(jobId));
            }

            return Results.Ok(status);
        });
    }

    private static string NormalizeBasePath(string basePath)
    {
        if (string.IsNullOrWhiteSpace(basePath))
        {
            return "/async-status";
        }

        var normalized = basePath.StartsWith('/') ? basePath : $"/{basePath}";
        return normalized.TrimEnd('/');
    }
}
