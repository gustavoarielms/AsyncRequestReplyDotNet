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
                    var result = await ProcessDeliveryAsync(delivery, deliveryCancellation.Token);

                    if (result.Complete)
                    {
                        await CompleteDeliveryAsync(delivery, result, deliveryCancellation.Token);
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

    private async Task<DeliveryResult> ProcessDeliveryAsync(
        AsyncJobDelivery delivery,
        CancellationToken cancellationToken)
    {
        var job = delivery.Job;
        var current = await statusStore.GetAsync(job.Id, cancellationToken);
        var createdAt = current?.CreatedAt ?? SystemClock.UtcNow();

        if (current is not null && IsTerminal(current.Status))
        {
            return DeliveryResult.Terminal(current);
        }

        if (job.ExecutionMode == AsyncExecutionMode.wait_external)
        {
            return await ResolveExternalAsync(job, current, createdAt, cancellationToken);
        }

        var processor = processors.FirstOrDefault();

        if (processor is null)
        {
            await queue.AbandonAsync(delivery, cancellationToken);
            await Task.Delay(options.ExternalResolutionInterval, cancellationToken);
            return DeliveryResult.Incomplete;
        }

        await SetStatusAsync(new AsyncStatusResponse(
            job.Id,
            AsyncJobStatus.processing,
            null,
            null,
            createdAt,
            SystemClock.UtcNow()), cancellationToken);

        object? result;

        try
        {
            result = await processor.ProcessAsync(job.Id, job.Payload, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Async request-reply job {JobId} failed.", job.Id);
            return DeliveryResult.Terminal(new AsyncStatusResponse(
                job.Id,
                AsyncJobStatus.failed,
                null,
                $"Job processing failed. Reference: {job.Id}.",
                createdAt,
                SystemClock.UtcNow()));
        }

        return DeliveryResult.Terminal(new AsyncStatusResponse(
            job.Id,
            AsyncJobStatus.completed,
            result,
            null,
            createdAt,
            SystemClock.UtcNow()));
    }

    private async Task<DeliveryResult> ResolveExternalAsync(
        AsyncJobEnvelope job,
        AsyncStatusResponse? persistedStatus,
        DateTimeOffset createdAt,
        CancellationToken cancellationToken)
    {
        AsyncStatusResponse currentStatus;

        if (persistedStatus?.Status == AsyncJobStatus.waiting_external)
        {
            currentStatus = persistedStatus;
        }
        else
        {
            currentStatus = new AsyncStatusResponse(
                job.Id,
                AsyncJobStatus.waiting_external,
                null,
                null,
                createdAt,
                SystemClock.UtcNow());
            await SetStatusAsync(currentStatus, cancellationToken);
        }

        var resolver = externalResolvers.FirstOrDefault();

        if (resolver is null)
        {
            await BeginRetentionAsync(job.Id, cancellationToken);
            return DeliveryResult.CompleteWithoutTerminal;
        }

        for (var attempt = 1; attempt <= options.ExternalResolutionMaxAttempts; attempt++)
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(options.ExternalResolutionTimeout);
            AsyncStatusResponse? resolved = null;

            try
            {
                resolved = await resolver.ResolveAsync(job.Id, currentStatus, timeout.Token);

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
                        case AsyncJobStatus.waiting_external:
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
                resolved = null;
            }

            if (resolved is not null)
            {
                if (IsTerminal(resolved.Status))
                {
                    return DeliveryResult.Terminal(resolved);
                }

                await SetStatusAsync(resolved, cancellationToken);
                currentStatus = resolved;
            }

            if (attempt < options.ExternalResolutionMaxAttempts)
            {
                await Task.Delay(options.ExternalResolutionInterval, cancellationToken);
            }
        }

        return DeliveryResult.Terminal(new AsyncStatusResponse(
            job.Id,
            AsyncJobStatus.failed,
            null,
            $"External status resolution failed. Reference: {job.Id}.",
            createdAt,
            SystemClock.UtcNow()));
    }

    private async Task CompleteDeliveryAsync(
        AsyncJobDelivery delivery,
        DeliveryResult result,
        CancellationToken cancellationToken)
    {
        if (result.TerminalStatus is not null)
        {
            if (queue is IAsyncJobTerminalStore terminalStore)
            {
                await terminalStore.CompleteTerminalAsync(
                    delivery,
                    result.TerminalStatus,
                    cancellationToken);
                return;
            }

            await SetStatusAsync(result.TerminalStatus, cancellationToken);
        }

        await queue.CompleteAsync(delivery, cancellationToken);
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

    private readonly record struct DeliveryResult(
        bool Complete,
        AsyncStatusResponse? TerminalStatus)
    {
        public static DeliveryResult Incomplete => new(false, null);

        public static DeliveryResult CompleteWithoutTerminal => new(true, null);

        public static DeliveryResult Terminal(AsyncStatusResponse status) => new(true, status);
    }
}
