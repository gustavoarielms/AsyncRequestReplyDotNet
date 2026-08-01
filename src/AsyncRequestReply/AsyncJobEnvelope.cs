namespace AsyncRequestReply;

public sealed record AsyncJobEnvelope(
    string Id,
    object? Payload,
    AsyncExecutionMode ExecutionMode);
