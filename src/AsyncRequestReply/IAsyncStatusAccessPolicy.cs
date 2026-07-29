using Microsoft.AspNetCore.Http;

namespace AsyncRequestReply;

public interface IAsyncStatusAccessPolicy
{
    ValueTask<bool> CanReadAsync(
        HttpContext httpContext,
        string jobId,
        CancellationToken cancellationToken = default);
}
