from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.models import ErrorBody, ErrorDetail


def error_body(code: str, message: str) -> dict:
    return ErrorBody(error=ErrorDetail(code=code, message=message)).model_dump()


def code_for_status(status: int) -> str:
    if status == 400:
        return "invalid_request"
    if status == 404:
        return "not_found"
    if status == 503:
        return "storage_unavailable"
    return "storage_error"


async def http_error_handler(_: Request, exc: StarletteHTTPException) -> JSONResponse:
    message = exc.detail if isinstance(exc.detail, str) else str(exc.detail)
    return JSONResponse(status_code=exc.status_code, content=error_body(code_for_status(exc.status_code), message))


async def validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
    return JSONResponse(status_code=400, content=error_body("invalid_request", str(exc)))
