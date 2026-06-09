namespace AsyncRequestReply.Internal;

internal static class StatusResponseFactory
{
    public static AsyncStatusResponse Queued(string jobId)
    {
        var now = SystemClock.UtcNow();
        return new AsyncStatusResponse(jobId, AsyncJobStatus.queued, null, null, now, now);
    }

    public static AsyncStatusResponse NotFound(string jobId)
    {
        var now = SystemClock.UtcNow();
        return new AsyncStatusResponse(jobId, AsyncJobStatus.not_found, null, null, now, now);
    }
}
