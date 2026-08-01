using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Options;

namespace AsyncRequestReply.Internal;

internal sealed class InMemoryAsyncJobQueue :
    IAsyncJobQueue,
    IAsyncJobQueueReader,
    IAsyncJobSubmissionStore
{
    private readonly Channel<AsyncJobEnvelope> channel;
    private readonly TimeSpan enqueueTimeout;
    private readonly SemaphoreSlim admissionSlots;
    private readonly InMemoryAsyncStatusStore statusStore;
    private readonly Dictionary<string, Task> submissions = new();
    private readonly object submissionLock = new();

    public InMemoryAsyncJobQueue(
        IOptions<AsyncRequestReplyOptions> options,
        InMemoryAsyncStatusStore statusStore)
    {
        this.statusStore = statusStore;
        enqueueTimeout = options.Value.EnqueueTimeout;
        admissionSlots = new SemaphoreSlim(options.Value.QueueCapacity, options.Value.QueueCapacity);
        channel = Channel.CreateBounded<AsyncJobEnvelope>(new BoundedChannelOptions(options.Value.QueueCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false
        });
    }

    public async ValueTask SubmitAsync(
        string jobId,
        object? payload,
        AsyncExecutionMode executionMode,
        string? accessToken,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Task submission;
        TaskCompletionSource? owner = null;

        lock (submissionLock)
        {
            if (!submissions.TryGetValue(jobId, out submission!))
            {
                owner = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                submission = owner.Task;
                submissions.Add(jobId, submission);
            }
        }

        if (owner is not null)
        {
            _ = RunSubmissionAsync(
                jobId,
                payload,
                executionMode,
                accessToken,
                submission,
                owner);
        }

        await submission.WaitAsync(cancellationToken);
    }

    private async Task RunSubmissionAsync(
        string jobId,
        object? payload,
        AsyncExecutionMode executionMode,
        string? accessToken,
        Task submission,
        TaskCompletionSource completion)
    {
        Exception? error = null;

        try
        {
            var admitted = await statusStore.AdmitAsync(jobId, accessToken, CancellationToken.None);

            if (admitted)
            {
                try
                {
                    await EnqueueAsync(jobId, payload, executionMode, CancellationToken.None);
                }
                catch
                {
                    await statusStore.DeleteAsync(jobId, CancellationToken.None);
                    throw;
                }
            }
        }
        catch (Exception ex)
        {
            error = ex;
        }

        lock (submissionLock)
        {
            if (submissions.TryGetValue(jobId, out var current)
                && ReferenceEquals(current, submission))
            {
                submissions.Remove(jobId);
            }
        }

        if (error is null)
        {
            completion.TrySetResult();
        }
        else
        {
            completion.TrySetException(error);
            _ = completion.Task.Exception;
        }
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
            await admissionSlots.WaitAsync(timeout.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new AsyncQueueUnavailableException("The in-memory job queue is full.");
        }

        try
        {
            if (!channel.Writer.TryWrite(new AsyncJobEnvelope(jobId, payload, executionMode)))
            {
                throw new InvalidOperationException("The in-memory job queue admission invariant was violated.");
            }
        }
        catch
        {
            admissionSlots.Release();
            throw;
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
        cancellationToken.ThrowIfCancellationRequested();
        admissionSlots.Release();
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
        cancellationToken.ThrowIfCancellationRequested();

        if (!channel.Writer.TryWrite(delivery.Job))
        {
            throw new InvalidOperationException("The in-memory job queue admission invariant was violated.");
        }

        return ValueTask.CompletedTask;
    }
}
