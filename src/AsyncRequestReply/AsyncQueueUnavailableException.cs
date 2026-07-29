namespace AsyncRequestReply;

public sealed class AsyncQueueUnavailableException : Exception
{
    public AsyncQueueUnavailableException(string message)
        : base(message)
    {
    }
}
