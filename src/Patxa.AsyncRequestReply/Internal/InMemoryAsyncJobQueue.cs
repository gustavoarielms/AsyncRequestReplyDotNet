using System.Threading.Channels;

namespace Patxa.AsyncRequestReply.Internal;

internal sealed class InMemoryAsyncJobQueue : IAsyncJobQueue
{
    private readonly Channel<AsyncJob> channel = Channel.CreateUnbounded<AsyncJob>();

    public ValueTask EnqueueAsync(string jobId, object? payload, AsyncExecutionMode executionMode, CancellationToken cancellationToken = default)
    {
        return channel.Writer.WriteAsync(new AsyncJob(jobId, payload, executionMode), cancellationToken);
    }

    internal IAsyncEnumerable<AsyncJob> DequeueAllAsync(CancellationToken cancellationToken)
    {
        return channel.Reader.ReadAllAsync(cancellationToken);
    }
}
