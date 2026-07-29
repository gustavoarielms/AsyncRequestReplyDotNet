using Microsoft.Extensions.Options;

namespace AsyncRequestReply.Internal;

internal static class StatusLocationBuilder
{
    public static string Build(
        IOptions<AsyncRequestReplyOptions> options,
        string jobId,
        string? accessToken)
    {
        var basePath = NormalizeBasePath(options.Value.StatusBasePath);
        var statusPath = $"{basePath}/status/{jobId}";

        return string.IsNullOrWhiteSpace(accessToken)
            ? statusPath
            : $"{statusPath}/{accessToken}";
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
