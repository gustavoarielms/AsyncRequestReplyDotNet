using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AsyncRequestReply.Internal;

internal sealed class AsyncJobBackgroundService(
    IAsyncJobQueueReader queue,
    IAsyncStatusStore statusStore,
    IAsyncStatusTokenStore tokenStore,
    IEnumerable<IAsyncJobProcessor> processors,
    IEnumerable<IExternalStatusResolver> externalResolvers,
    IOptions<AsyncRequestReplyOptions> requestReplyOptions,
    ILogger<AsyncJobBackgroundService> logger) : BackgroundService
{
    private readonly AsyncRequestReplyOptions options = requestReplyOptions.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunQueueAsync(stoppingToken);
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (AsyncQueueUnavailableException ex)
            {
                logger.LogWarning(
                    ex,
                    "Async request-reply queue reader is unavailable. Retrying in {RetryInterval}.",
                    options.WorkerRecoveryInterval);
                await Task.Delay(options.WorkerRecoveryInterval, stoppingToken);
            }
        }
    }

    private async Task RunQueueAsync(CancellationToken stoppingToken)
    {
        var parallelOptions = new ParallelOptions
        {
            CancellationToken = stoppingToken,
            MaxDegreeOfParallelism = options.WorkerConcurrency
        };

        await Parallel.ForEachAsync(
            queue.DequeueAllAsync(stoppingToken),
            parallelOptions,
            async (delivery, cancellationToken) =>
            {
                using var deliveryCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                var heartbeat = RenewLeaseAsync(delivery, deliveryCancellation);

                try
                {
                    var completed = await ProcessDeliveryAsync(delivery, deliveryCancellation.Token);

                    if (completed)
                    {
                        await queue.CompleteAsync(delivery, deliveryCancellation.Token);
                    }
                }
                catch (OperationCanceledException) when (deliveryCancellation.IsCancellationRequested)
                {
                    await TryAbandonAsync(delivery);
                }
                catch (Exception ex)
                {
                    logger.LogError(
                        ex,
                        "Unexpected failure while handling async request-reply delivery {DeliveryId} for job {JobId}.",
                        delivery.DeliveryId,
                        delivery.Job.Id);
                    await TryAbandonAsync(delivery);
                }
                finally
                {
                    deliveryCancellation.Cancel();

                    try
                    {
                        await heartbeat;
                    }
                    catch (OperationCanceledException) when (deliveryCancellation.IsCancellationRequested)
                    {
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(
                            ex,
                            "Delivery lease renewal stopped for delivery {DeliveryId} and job {JobId}.",
                            delivery.DeliveryId,
                            delivery.Job.Id);
                    }
                }
            });
    }

    private async Task<bool> ProcessDeliveryAsync(
        AsyncJobDelivery delivery,
        CancellationToken cancellationToken)
    {
        var job = delivery.Job;
        var current = await statusStore.GetAsync(job.Id, cancellationToken);
        var createdAt = current?.CreatedAt ?? SystemClock.UtcNow();

        if (current is not null && IsTerminal(current.Status))
        {
            await BeginRetentionAsync(job.Id, cancellationToken);
            return true;
        }

        if (job.ExecutionMode == AsyncExecutionMode.wait_external)
        {
            return await ResolveExternalAsync(job, createdAt, cancellationToken);
        }

        var processor = processors.FirstOrDefault();

        if (processor is null)
        {
            await queue.AbandonAsync(delivery, cancellationToken);
            await Task.Delay(options.ExternalResolutionInterval, cancellationToken);
            return false;
        }

        await SetStatusAsync(new AsyncStatusResponse(
            job.Id,
            AsyncJobStatus.processing,
            null,
            null,
            createdAt,
            SystemClock.UtcNow()), cancellationToken);

        try
        {
            var result = await processor.ProcessAsync(job.Id, job.Payload, cancellationToken);
            await SetStatusAsync(new AsyncStatusResponse(
                job.Id,
                AsyncJobStatus.completed,
                result,
                null,
                createdAt,
                SystemClock.UtcNow()), cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Async request-reply job {JobId} failed.", job.Id);
            await SetStatusAsync(new AsyncStatusResponse(
                job.Id,
                AsyncJobStatus.failed,
                null,
                $"Job processing failed. Reference: {job.Id}.",
                createdAt,
                SystemClock.UtcNow()), cancellationToken);
        }

        return true;
    }

    private async Task<bool> ResolveExternalAsync(
        AsyncJobEnvelope job,
        DateTimeOffset createdAt,
        CancellationToken cancellationToken)
    {
        var currentStatus = new AsyncStatusResponse(
            job.Id,
            AsyncJobStatus.waiting_external,
            null,
            null,
            createdAt,
            SystemClock.UtcNow());
        await SetStatusAsync(currentStatus, cancellationToken);

        var resolver = externalResolvers.FirstOrDefault();

        if (resolver is null)
        {
            await BeginRetentionAsync(job.Id, cancellationToken);
            return true;
        }

        for (var attempt = 1; attempt <= options.ExternalResolutionMaxAttempts; attempt++)
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(options.ExternalResolutionTimeout);

            try
            {
                var resolved = await resolver.ResolveAsync(job.Id, currentStatus, timeout.Token);

                if (resolved is not null)
                {
                    if (!string.Equals(resolved.Id, job.Id, StringComparison.Ordinal))
                    {
                        throw new InvalidOperationException(
                            "The external status resolver returned a different job identifier.");
                    }

                    switch (resolved.Status)
                    {
                        case AsyncJobStatus.completed:
                        case AsyncJobStatus.failed:
                            await SetStatusAsync(resolved, cancellationToken);
                            return true;
                        case AsyncJobStatus.waiting_external:
                            await SetStatusAsync(resolved, cancellationToken);
                            currentStatus = resolved;
                            break;
                        case AsyncJobStatus.queued:
                        case AsyncJobStatus.processing:
                        case AsyncJobStatus.not_found:
                            throw new InvalidOperationException(
                                $"The external status resolver returned invalid status {resolved.Status}.");
                        default:
                            throw new InvalidOperationException(
                                $"The external status resolver returned unknown status {resolved.Status}.");
                    }
                }
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning(
                    "External status resolution attempt {Attempt} timed out for job {JobId}.",
                    attempt,
                    job.Id);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                logger.LogWarning(
                    ex,
                    "External status resolution attempt {Attempt} failed for job {JobId}.",
                    attempt,
                    job.Id);
            }

            if (attempt < options.ExternalResolutionMaxAttempts)
            {
                await Task.Delay(options.ExternalResolutionInterval, cancellationToken);
            }
        }

        await SetStatusAsync(new AsyncStatusResponse(
            job.Id,
            AsyncJobStatus.failed,
            null,
            $"External status resolution failed. Reference: {job.Id}.",
            createdAt,
            SystemClock.UtcNow()), cancellationToken);

        return true;
    }

    private async Task SetStatusAsync(
        AsyncStatusResponse status,
        CancellationToken cancellationToken)
    {
        await statusStore.SetAsync(status, cancellationToken);

        if (IsTerminal(status.Status))
        {
            await BeginRetentionAsync(status.Id, cancellationToken);
        }
    }

    private async Task BeginRetentionAsync(
        string jobId,
        CancellationToken cancellationToken)
    {
        await statusStore.BeginRetentionAsync(jobId, cancellationToken);
        await tokenStore.BeginRetentionAsync(jobId, cancellationToken);
    }

    private async Task RenewLeaseAsync(
        AsyncJobDelivery delivery,
        CancellationTokenSource deliveryCancellation)
    {
        try
        {
            using var timer = new PeriodicTimer(options.DeliveryLeaseRenewalInterval);

            while (await timer.WaitForNextTickAsync(deliveryCancellation.Token))
            {
                await queue.RenewAsync(delivery, deliveryCancellation.Token);
            }
        }
        catch (OperationCanceledException) when (deliveryCancellation.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            deliveryCancellation.Cancel();
            throw;
        }
    }

    private async Task TryAbandonAsync(AsyncJobDelivery delivery)
    {
        try
        {
            await queue.AbandonAsync(delivery, CancellationToken.None);
        }
        catch (Exception ex)
        {
            logger.LogError(
                ex,
                "Failed to abandon async request-reply delivery {DeliveryId} for job {JobId}.",
                delivery.DeliveryId,
                delivery.Job.Id);
        }
    }

    private static bool IsTerminal(AsyncJobStatus status)
    {
        return status is AsyncJobStatus.completed or AsyncJobStatus.failed;
    }
}
