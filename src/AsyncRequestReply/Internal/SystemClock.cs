namespace AsyncRequestReply.Internal;

internal static class SystemClock
{
    public static DateTimeOffset UtcNow() => DateTimeOffset.UtcNow;
}
