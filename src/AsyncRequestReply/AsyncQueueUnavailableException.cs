namespace AsyncRequestReply;

public sealed class AsyncQueueUnavailableException : Exception
{
    public AsyncQueueUnavailableException(string message)
        : base(message)
    {
    }

    public AsyncQueueUnavailableException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
