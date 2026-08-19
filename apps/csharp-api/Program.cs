using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Diagnostics;
using Prism.Api;
using Prism.Api.Ingest;
using Prism.Api.Metrics;
using Prism.Api.Models;
using Prism.Api.Store;
using Prometheus;

var cfg = AppConfig.Load();
IStore store;
try
{
    store = StoreFactory.Create(cfg);
}
catch (Exception ex)
{
    Console.WriteLine($"storage init deferred ({cfg.Storage}): {ex.Message}");
    store = new FailedStore(cfg.Storage, ex.Message);
}

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls(AppConfig.ToListenUrl(cfg.HttpAddr));
builder.Services.Configure<RouteHandlerOptions>(o => o.ThrowOnBadRequest = true);
builder.Services.ConfigureHttpJsonOptions(o => ConfigureJson(o.SerializerOptions));
builder.Logging.ClearProviders();
builder.Logging.AddConsole();

var app = builder.Build();
var json = new JsonSerializerOptions(JsonSerializerDefaults.Web);
ConfigureJson(json);

app.Use(async (ctx, next) =>
{
    var path = ctx.Request.Path.Value ?? "";
    var skip = path is "/metrics" or "/healthz" or "/readyz";
    var start = Stopwatch.StartNew();
    try
    {
        await next();
    }
    finally
    {
        if (!skip)
        {
            var route = path.Trim('/').Replace('/', '_');
            if (string.IsNullOrEmpty(route))
            {
                route = "root";
            }

            PrismMetrics.ObserveApi(store.Name, route, ctx.Request.Method, ctx.Response.StatusCode, start.Elapsed);
        }
    }
});

app.UseExceptionHandler(err =>
{
    err.Run(async ctx =>
    {
        var ex = ctx.Features.Get<IExceptionHandlerFeature>()?.Error;
        if (ex is BadHttpRequestException or JsonException)
        {
            await ApiErrors.Write(ctx, 400, ApiErrors.InvalidRequest, "invalid json", json);
            return;
        }

        await ApiErrors.Write(ctx, 500, ApiErrors.StorageError, ex?.Message ?? "error", json);
    });
});

app.MapGet("/healthz", () => Results.Text("ok"));
app.MapGet("/readyz", async (CancellationToken ct) =>
{
    try
    {
        await store.PingAsync(ct);
        return Results.Text("ready");
    }
    catch (Exception ex)
    {
        return ApiErrors.Unavailable(ex.Message);
    }
});
app.MapMetrics();

app.MapGet("/v1/meta", () => new Meta
{
    Backend = "csharp",
    Storage = store.Name,
    Storages = AppConfig.Storages,
    Contract = Contract.Version,
    Ops = Contract.Ops
});

app.MapGet("/v1/tags", async (CancellationToken ct) =>
{
    try
    {
        var tags = await store.ListTagsAsync(ct);
        return Results.Json(new Prism.Api.Models.TagList { Tags = tags.ToList() });
    }
    catch (Exception ex)
    {
        return ApiErrors.Storage(ex.Message);
    }
});

app.MapPost("/v1/tags", async (TagWriteRequest? req, CancellationToken ct) =>
{
    if (req?.Tags is null || req.Tags.Count == 0)
    {
        return ApiErrors.Invalid("tags is required");
    }

    try
    {
        await store.UpsertTagsAsync(req.Tags, ct);
        return Results.Json(new TagWriteResponse { Upserted = req.Tags.Count });
    }
    catch (Exception ex)
    {
        return ApiErrors.Storage(ex.Message);
    }
});

app.MapPost("/v1/write", async (WriteRequest? req, CancellationToken ct) =>
{
    if (req?.Samples is null || req.Samples.Count == 0)
    {
        return ApiErrors.Invalid("samples is required");
    }

    var now = DateTimeOffset.UtcNow;
    var samples = req.Samples.Select(s => s.Normalize(now)).ToList();
    var start = Stopwatch.StartNew();
    try
    {
        await store.WriteAsync(samples, ct);
        PrismMetrics.ObserveBackend(store.Name, "write", "http", samples.Count, start.Elapsed, null);
        return Results.Json(new WriteResponse { Written = samples.Count });
    }
    catch (Exception ex)
    {
        PrismMetrics.ObserveBackend(store.Name, "write", "http", samples.Count, start.Elapsed, ex);
        return ApiErrors.Storage(ex.Message);
    }
});

app.MapPost("/v1/read", (ReadRequest? req, CancellationToken ct) => ServeRead(store, req, ct));
app.MapPost("/v1/locf", (ReadRequest? req, CancellationToken ct) =>
{
    if (req is not null)
    {
        req.Mode = "locf";
    }

    return ServeRead(store, req, ct);
});
app.MapPost("/v1/range", (ReadRequest? req, CancellationToken ct) =>
{
    if (req is not null)
    {
        req.Mode = "range";
    }

    return ServeRead(store, req, ct);
});

_ = NatsConsumer.RunAsync(cfg, store, json, app.Lifetime.ApplicationStopping);
app.Lifetime.ApplicationStopped.Register(() => store.DisposeAsync().AsTask().GetAwaiter().GetResult());

Console.WriteLine($"csharp-api listening on {cfg.HttpAddr} storage={store.Name}");
app.Run();

static void ConfigureJson(JsonSerializerOptions o)
{
    o.PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower;
    o.PropertyNameCaseInsensitive = true;
    o.DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull;
}

static async Task<IResult> ServeRead(IStore store, ReadRequest? req, CancellationToken ct)
{
    if (req is null)
    {
        return ApiErrors.Invalid("invalid json");
    }

    if (!ReadLogic.ValidMode(req.Mode))
    {
        return ApiErrors.Invalid("mode must be locf, range, sample or twavg");
    }

    if (req.TagIds.Count == 0)
    {
        return ApiErrors.Invalid("tag_ids is required");
    }

    IReadOnlyList<Sample> raw;
    var start = Stopwatch.StartNew();
    try
    {
        if (req.Mode == "locf")
        {
            if (req.At is null || req.At == default)
            {
                return ApiErrors.Invalid("at is required");
            }

            raw = await store.LocfAsync(req.TagIds, req.At.Value.ToUniversalTime(), ct);
        }
        else
        {
            if (req.From is null || req.From == default || req.To is null || req.To == default)
            {
                return ApiErrors.Invalid("from and to are required");
            }

            raw = await store.RangeAsync(req.TagIds, req.From.Value.ToUniversalTime(), req.To.Value.ToUniversalTime(), ct);
        }
    }
    catch (Exception ex)
    {
        PrismMetrics.ObserveBackend(store.Name, req.Mode, "http", 0, start.Elapsed, ex);
        return ApiErrors.Storage(ex.Message);
    }

    PrismMetrics.ObserveBackend(store.Name, req.Mode, "http", raw.Count, start.Elapsed, null);
    return Results.Json(ReadLogic.Assemble(req.Mode, req, raw));
}
