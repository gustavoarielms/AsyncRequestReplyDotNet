namespace Patxa.AsyncRequestReply;

public sealed class AsyncRequestReplyOptions
{
    public string StatusBasePath { get; set; } = "/async-status";

    public bool ExposeStatusEndpoint { get; set; } = true;
}
