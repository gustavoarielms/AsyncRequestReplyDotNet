using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using AsyncRequestReply.Internal;

namespace AsyncRequestReply;

public static class AsyncRequestReplyEndpointRouteBuilderExtensions
{
    public static IEndpointRouteBuilder MapAsyncRequestReplyStatusEndpoints(this IEndpointRouteBuilder endpoints)
    {
        var options = endpoints.ServiceProvider.GetRequiredService<IOptions<AsyncRequestReplyOptions>>().Value;

        if (!options.ExposeStatusEndpoint)
        {
            return endpoints;
        }

        var basePath = NormalizeBasePath(options.StatusBasePath);

        endpoints.MapGet($"{basePath}/status/{{jobId}}", async (
            string jobId,
            IAsyncStatusStore statusStore,
            IEnumerable<IExternalStatusResolver> resolvers,
            CancellationToken cancellationToken) =>
        {
            var status = await statusStore.GetAsync(jobId, cancellationToken);

            if (status is null)
            {
                return Results.NotFound(StatusResponseFactory.NotFound(jobId));
            }

            if (status.Status == AsyncJobStatus.waiting_external)
            {
                var resolver = resolvers.FirstOrDefault();
                var resolved = resolver is null
                    ? null
                    : await resolver.ResolveAsync(jobId, status, cancellationToken);

                if (resolved is not null)
                {
                    await statusStore.SetAsync(resolved, cancellationToken);
                    status = resolved;
                }
            }

            return Results.Ok(status);
        });

        return endpoints;
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
