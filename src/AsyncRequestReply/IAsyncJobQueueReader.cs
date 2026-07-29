namespace AsyncRequestReply;

public interface IAsyncJobQueueReader
{
    IAsyncEnumerable<AsyncJobDelivery> DequeueAllAsync(CancellationToken cancellationToken = default);

    ValueTask CompleteAsync(AsyncJobDelivery delivery, CancellationToken cancellationToken = default);

    ValueTask RenewAsync(AsyncJobDelivery delivery, CancellationToken cancellationToken = default);

    ValueTask AbandonAsync(AsyncJobDelivery delivery, CancellationToken cancellationToken = default);
}
