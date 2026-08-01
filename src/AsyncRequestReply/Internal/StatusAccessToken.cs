using System.Security.Cryptography;

namespace AsyncRequestReply.Internal;

internal static class StatusAccessToken
{
    public static byte[] Hash(string token)
    {
        return SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(token));
    }
}
