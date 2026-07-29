namespace AsyncRequestReply;

public sealed record AsyncJobDelivery(
    string DeliveryId,
    AsyncJobEnvelope Job);
