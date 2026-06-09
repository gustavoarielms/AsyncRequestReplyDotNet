using System.Text.Json;
using Microsoft.AspNetCore.Http;

namespace AsyncRequestReply.Internal;

internal static class PayloadReader
{
    public static async Task<object?> ReadAsync(HttpRequest request, string? payloadPath, CancellationToken cancellationToken)
    {
        if (request.Body is null)
        {
            return null;
        }

        using var document = await JsonDocument.ParseAsync(request.Body, cancellationToken: cancellationToken);
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
