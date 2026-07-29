namespace AsyncRequestReply;

public interface IAsyncJobSubmissionStore
{
    ValueTask SubmitAsync(
        string jobId,
        object? payload,
        AsyncExecutionMode executionMode,
        string? accessToken,
        CancellationToken cancellationToken = default);
}
