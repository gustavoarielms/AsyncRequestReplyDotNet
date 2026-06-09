namespace AsyncRequestReply;

public sealed record AsyncAcceptedResponse(
    string Id,
    AsyncJobStatus Status,
    string Location);
