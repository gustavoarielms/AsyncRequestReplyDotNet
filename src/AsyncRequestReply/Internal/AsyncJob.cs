namespace AsyncRequestReply.Internal;

internal sealed record AsyncJob(
    string Id,
    object? Payload,
    AsyncExecutionMode ExecutionMode);
