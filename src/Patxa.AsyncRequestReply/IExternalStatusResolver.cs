namespace Patxa.AsyncRequestReply;

public interface IExternalStatusResolver
{
    Task<AsyncStatusResponse?> ResolveAsync(string jobId, AsyncStatusResponse currentStatus, CancellationToken cancellationToken = default);
}
