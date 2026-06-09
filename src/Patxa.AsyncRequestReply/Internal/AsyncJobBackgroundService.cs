using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Patxa.AsyncRequestReply.Internal;

internal sealed class AsyncJobBackgroundService(
    InMemoryAsyncJobQueue queue,
    IAsyncStatusStore statusStore,
    IEnumerable<IAsyncJobProcessor> processors,
    ILogger<AsyncJobBackgroundService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var job in queue.DequeueAllAsync(stoppingToken))
        {
            var processor = processors.FirstOrDefault();

            if (processor is null)
            {
                continue;
            }

            await ProcessJobAsync(job, processor, stoppingToken);
        }
    }

    private async Task ProcessJobAsync(AsyncJob job, IAsyncJobProcessor processor, CancellationToken cancellationToken)
    {
        var current = await statusStore.GetAsync(job.Id, cancellationToken);
        var createdAt = current?.CreatedAt ?? SystemClock.UtcNow();

        if (job.ExecutionMode == AsyncExecutionMode.wait_external)
        {
            await statusStore.SetAsync(new AsyncStatusResponse(
                job.Id,
                AsyncJobStatus.waiting_external,
                null,
                null,
                createdAt,
                SystemClock.UtcNow()), cancellationToken);
            return;
        }

        await statusStore.SetAsync(new AsyncStatusResponse(
            job.Id,
            AsyncJobStatus.processing,
            null,
            null,
            createdAt,
            SystemClock.UtcNow()), cancellationToken);

        try
        {
            var result = await processor.ProcessAsync(job.Id, job.Payload, cancellationToken);
            await statusStore.SetAsync(new AsyncStatusResponse(
                job.Id,
                AsyncJobStatus.completed,
                result,
                null,
                createdAt,
                SystemClock.UtcNow()), cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Async request-reply job {JobId} failed.", job.Id);
            await statusStore.SetAsync(new AsyncStatusResponse(
                job.Id,
                AsyncJobStatus.failed,
                null,
                ex.Message,
                createdAt,
                SystemClock.UtcNow()), cancellationToken);
        }
    }
}
