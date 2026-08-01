namespace AsyncRequestReply.Internal;

internal sealed class AsyncDeliveryLeaseLostException : Exception
{
    public AsyncDeliveryLeaseLostException(string deliveryId)
        : base($"Delivery lease was lost for delivery {deliveryId}.")
    {
    }
}
