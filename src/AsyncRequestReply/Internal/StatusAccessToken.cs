using System.Security.Cryptography;
using Microsoft.AspNetCore.WebUtilities;

namespace AsyncRequestReply.Internal;

internal static class StatusAccessToken
{
    public static string Create()
    {
        return WebEncoders.Base64UrlEncode(RandomNumberGenerator.GetBytes(32));
    }

    public static byte[] Hash(string token)
    {
        return SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(token));
    }
}
