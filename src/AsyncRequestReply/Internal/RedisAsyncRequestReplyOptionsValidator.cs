using System.Net;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace AsyncRequestReply.Internal;

internal sealed class RedisAsyncRequestReplyOptionsValidator(
    IOptions<AsyncRequestReplyOptions> requestReplyOptions) : IValidateOptions<RedisAsyncRequestReplyOptions>
{
    public ValidateOptionsResult Validate(string? name, RedisAsyncRequestReplyOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.Configuration))
        {
            return ValidateOptionsResult.Fail("Redis Configuration is required.");
        }

        if (string.IsNullOrWhiteSpace(options.StreamKey))
        {
            return ValidateOptionsResult.Fail("Redis StreamKey is required.");
        }

        if (string.IsNullOrWhiteSpace(options.ConsumerGroup))
        {
            return ValidateOptionsResult.Fail("Redis ConsumerGroup is required.");
        }

        if (string.IsNullOrWhiteSpace(options.StatusKeyPrefix))
        {
            return ValidateOptionsResult.Fail("Redis StatusKeyPrefix is required.");
        }

        var streamHashTag = GetHashTag(options.StreamKey);
        var statusHashTag = GetHashTag(options.StatusKeyPrefix);

        if (streamHashTag is null
            || statusHashTag is null
            || !string.Equals(streamHashTag, statusHashTag, StringComparison.Ordinal))
        {
            return ValidateOptionsResult.Fail(
                "Redis StreamKey and StatusKeyPrefix must contain the same non-empty hash tag.");
        }

        if (options.QueuePollInterval <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail("Redis QueuePollInterval must be greater than zero.");
        }

        if (options.ClaimIdleTime <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail("Redis ClaimIdleTime must be greater than zero.");
        }

        if (options.ClaimIdleTime <= requestReplyOptions.Value.DeliveryLeaseRenewalInterval)
        {
            return ValidateOptionsResult.Fail(
                "Redis ClaimIdleTime must be greater than DeliveryLeaseRenewalInterval.");
        }

        if (options.MaxQueueLength <= 0)
        {
            return ValidateOptionsResult.Fail("Redis MaxQueueLength must be greater than zero.");
        }

        if (options.StatusTimeToLive <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail("Redis StatusTimeToLive must be greater than zero.");
        }

        try
        {
            var configuration = ConfigurationOptions.Parse(options.Configuration);

            if (!configuration.Ssl
                && !options.AllowUnencryptedRemoteConnection
                && configuration.EndPoints.Any(endpoint => !IsLoopback(endpoint)))
            {
                return ValidateOptionsResult.Fail(
                    "TLS is required for remote Redis endpoints. Set ssl=true or explicitly opt into AllowUnencryptedRemoteConnection.");
            }
        }
        catch (Exception ex)
        {
            return ValidateOptionsResult.Fail($"Redis Configuration is invalid: {ex.Message}");
        }

        return ValidateOptionsResult.Success;
    }

    private static string? GetHashTag(string key)
    {
        var start = key.IndexOf('{');

        if (start < 0)
        {
            return null;
        }

        var end = key.IndexOf('}', start + 1);

        return end > start + 1
            ? key[(start + 1)..end]
            : null;
    }

    private static bool IsLoopback(EndPoint endpoint)
    {
        return endpoint switch
        {
            IPEndPoint ip => IPAddress.IsLoopback(ip.Address),
            DnsEndPoint dns => string.Equals(dns.Host, "localhost", StringComparison.OrdinalIgnoreCase)
                || string.Equals(dns.Host, "localhost.localdomain", StringComparison.OrdinalIgnoreCase)
                || IPAddress.TryParse(dns.Host, out var address) && IPAddress.IsLoopback(address),
            _ => false
        };
    }
}
