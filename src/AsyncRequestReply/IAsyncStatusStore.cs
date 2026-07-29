namespace AsyncRequestReply;

public interface IAsyncStatusStore
{
    Task<AsyncStatusResponse?> GetAsync(string jobId, CancellationToken cancellationToken = default);

    Task SetAsync(AsyncStatusResponse status, CancellationToken cancellationToken = default);

    Task DeleteAsync(string jobId, CancellationToken cancellationToken = default);
}
