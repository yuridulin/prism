using System.Text.Json;
using Prism.Api.Models;

namespace Prism.Api;

public static class ApiErrors
{
    public const string InvalidRequest = "invalid_request";
    public const string NotFound = "not_found";
    public const string StorageUnavailable = "storage_unavailable";
    public const string StorageError = "storage_error";

    public static IResult Invalid(string message) => Json(400, InvalidRequest, message);

    public static IResult Unavailable(string message) => Json(503, StorageUnavailable, message);

    public static IResult Storage(string message) => Json(500, StorageError, message);

    public static IResult Json(int status, string code, string message) =>
        Results.Json(new ErrorBody { Error = new ErrorDetail { Code = code, Message = message } }, statusCode: status);

    public static async Task Write(HttpContext ctx, int status, string code, string message, JsonSerializerOptions json)
    {
        if (ctx.Response.HasStarted)
        {
            return;
        }

        ctx.Response.StatusCode = status;
        ctx.Response.ContentType = "application/json";
        await ctx.Response.WriteAsJsonAsync(new ErrorBody { Error = new ErrorDetail { Code = code, Message = message } }, json);
    }
}
