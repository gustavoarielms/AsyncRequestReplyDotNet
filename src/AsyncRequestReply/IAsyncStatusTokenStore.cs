namespace AsyncRequestReply;

public interface IAsyncStatusTokenStore
{
    Task SetAsync(
        string jobId,
        string accessToken,
        CancellationToken cancellationToken = default);

    Task<bool> ValidateAsync(
        string jobId,
        string accessToken,
        CancellationToken cancellationToken = default);

    Task RefreshAsync(string jobId, CancellationToken cancellationToken = default);

    Task DeleteAsync(string jobId, CancellationToken cancellationToken = default);
}
