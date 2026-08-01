namespace AsyncRequestReply.Internal;

internal interface IAsyncJobTerminalStore
{
    ValueTask CompleteTerminalAsync(
        AsyncJobDelivery delivery,
        AsyncStatusResponse status,
        CancellationToken cancellationToken = default);
}
