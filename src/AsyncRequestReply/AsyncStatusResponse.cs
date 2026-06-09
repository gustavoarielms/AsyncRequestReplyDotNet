namespace AsyncRequestReply;

public sealed record AsyncStatusResponse(
    string Id,
    AsyncJobStatus Status,
    object? Result,
    string? Error,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt);
