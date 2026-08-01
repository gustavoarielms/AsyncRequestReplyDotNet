using System.Buffers;
using System.Text.Json;
using Microsoft.AspNetCore.Http;

namespace AsyncRequestReply.Internal;

internal static class PayloadReader
{
    public static async Task<object?> ReadAsync(
        HttpRequest request,
        string? payloadPath,
        long maxPayloadBytes,
        CancellationToken cancellationToken)
    {
        if (request.ContentLength is > 0 && request.ContentLength > maxPayloadBytes)
        {
            throw new PayloadTooLargeException();
        }

        if (request.Body is null || request.ContentLength == 0)
        {
            return null;
        }

        await using var payload = new MemoryStream();
        var buffer = ArrayPool<byte>.Shared.Rent(81_920);

        try
        {
            while (true)
            {
                var read = await request.Body.ReadAsync(buffer, cancellationToken);

                if (read == 0)
                {
                    break;
                }

                if (payload.Length + read > maxPayloadBytes)
                {
                    throw new PayloadTooLargeException();
                }

                await payload.WriteAsync(buffer.AsMemory(0, read), cancellationToken);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        if (payload.Length == 0)
        {
            return null;
        }

        payload.Position = 0;

        using var document = await JsonDocument.ParseAsync(payload, cancellationToken: cancellationToken);
        var element = document.RootElement.Clone();

        if (string.IsNullOrWhiteSpace(payloadPath))
        {
            return element;
        }

        foreach (var segment in payloadPath.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (element.ValueKind != JsonValueKind.Object || !element.TryGetProperty(segment, out var next))
            {
                return null;
            }

            element = next.Clone();
        }

        return element;
    }
}
