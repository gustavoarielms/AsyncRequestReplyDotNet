using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Options;

namespace AsyncRequestReply.Internal;

internal sealed class InMemoryAsyncJobQueue : IAsyncJobQueue, IAsyncJobQueueReader
{
    private readonly Channel<AsyncJobEnvelope> channel;
    private readonly TimeSpan enqueueTimeout;

    public InMemoryAsyncJobQueue(IOptions<AsyncRequestReplyOptions> options)
    {
        enqueueTimeout = options.Value.EnqueueTimeout;
        channel = Channel.CreateBounded<AsyncJobEnvelope>(new BoundedChannelOptions(options.Value.QueueCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false
        });
    }

    public async ValueTask EnqueueAsync(
        string jobId,
        object? payload,
        AsyncExecutionMode executionMode,
        CancellationToken cancellationToken = default)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(enqueueTimeout);

        try
        {
            await channel.Writer.WriteAsync(
                new AsyncJobEnvelope(jobId, payload, executionMode),
                timeout.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new AsyncQueueUnavailableException("The in-memory job queue is full.");
        }
    }

    public async IAsyncEnumerable<AsyncJobDelivery> DequeueAllAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var job in channel.Reader.ReadAllAsync(cancellationToken))
        {
            yield return new AsyncJobDelivery(job.Id, job);
        }
    }

    public ValueTask CompleteAsync(
        AsyncJobDelivery delivery,
        CancellationToken cancellationToken = default)
    {
        return ValueTask.CompletedTask;
    }

    public ValueTask RenewAsync(
        AsyncJobDelivery delivery,
        CancellationToken cancellationToken = default)
    {
        return ValueTask.CompletedTask;
    }

    public ValueTask AbandonAsync(
        AsyncJobDelivery delivery,
        CancellationToken cancellationToken = default)
    {
        if (!channel.Writer.TryWrite(delivery.Job))
        {
            throw new AsyncQueueUnavailableException("The in-memory job queue is full.");
        }

        return ValueTask.CompletedTask;
    }
}
