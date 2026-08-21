using System.Diagnostics;
using System.Text.Json;
using NATS.Client.Core;
using Prism.Api.Metrics;
using Prism.Api.Models;
using Prism.Api.Store;

namespace Prism.Api.Ingest;

public static class NatsConsumer
{
    public static async Task RunAsync(AppConfig cfg, IStore store, JsonSerializerOptions json, CancellationToken ct)
    {
        try
        {
            var opts = NatsOpts.Default with
            {
                Url = cfg.NatsUrl,
                Name = "prism-csharp-api",
                ConnectTimeout = TimeSpan.FromSeconds(3)
            };
            await using var nats = new NatsConnection(opts);
            await nats.ConnectAsync();
            Console.WriteLine($"nats subscribed subject={cfg.NatsSubject} queue=csharp");
            await foreach (var msg in nats.SubscribeAsync<string>(cfg.NatsSubject, "csharp", cancellationToken: ct))
            {
                await Handle(store, json, msg.Data);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // shutdown
        }
        catch (Exception ex)
        {
            Console.WriteLine($"nats unavailable, HTTP-only mode: {ex.Message}");
        }
    }

    private static async Task Handle(IStore store, JsonSerializerOptions json, string? data)
    {
        if (string.IsNullOrWhiteSpace(data))
        {
            return;
        }

        List<WriteSample>? items;
        try
        {
            using var doc = JsonDocument.Parse(data);
            items = doc.RootElement.ValueKind switch
            {
                JsonValueKind.Array => JsonSerializer.Deserialize<List<WriteSample>>(data, json),
                JsonValueKind.Object when doc.RootElement.TryGetProperty("samples", out _) =>
                    JsonSerializer.Deserialize<SamplesWrap>(data, json)?.Samples,
                JsonValueKind.Object =>
                [
                    JsonSerializer.Deserialize<WriteSample>(data, json)!
                ],
                _ => null
            };
            items = items?.Where(s => s is not null).ToList();
            if (items is null || items.Count == 0)
            {
                return;
            }
        }
        catch (JsonException ex)
        {
            PrismMetrics.ObserveBackend(store.Name, "write", "nats", 0, TimeSpan.Zero, ex);
            Console.WriteLine($"nats decode error: {ex.Message}");
            return;
        }

        var now = DateTimeOffset.UtcNow;
        var samples = items.Select(s => s.Normalize(now)).ToList();
        var start = Stopwatch.StartNew();
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await store.WriteAsync(samples, timeout.Token);
            PrismMetrics.ObserveBackend(store.Name, "write", "nats", samples.Count, start.Elapsed, null);
        }
        catch (Exception ex)
        {
            PrismMetrics.ObserveBackend(store.Name, "write", "nats", samples.Count, start.Elapsed, ex);
            Console.WriteLine($"nats write error: {ex.Message}");
        }
    }
}
