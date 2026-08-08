"""Shared retry configuration for the sync and async M-Pesa HTTP clients.

Kept in one place so MpesaHttpClient and MpesaAsyncHttpClient retry the
same way and expose the same `max_retries` behavior.
"""

from typing import Any

import httpx
from tenacity import RetryCallState, retry_if_exception_type

from mpesakit.errors import MpesaApiException, MpesaError

DEFAULT_MAX_RETRIES = 3


def retry_enabled(enabled: bool):
    """Factory function to conditionally enable retries.

    Args:
        enabled (bool): Whether to enable retry logic.

    Returns:
        A retry condition function.
    """
    base_retry = retry_if_exception_type(
        httpx.TimeoutException
    ) | retry_if_exception_type(httpx.ConnectError)

    def _retry(retry_state: RetryCallState) -> bool:
        if not enabled:
            return False
        return base_retry(retry_state)

    return _retry


def stop_after_instance_max_retries(retry_state: RetryCallState) -> bool:
    """Stop condition reading `max_retries` off the bound client instance.

    Decorating a bound method with `@retry(...)` fixes the stop condition at
    class-definition time, so a plain `stop_after_attempt(n)` can't see a
    per-instance value. Reading `retry_state.args[0]` (i.e. `self`) lets each
    MpesaHttpClient/MpesaAsyncHttpClient instance configure its own attempt
    count via `max_retries`.
    """
    instance = retry_state.args[0]
    return retry_state.attempt_number >= instance.max_retries


def handle_retry_exception(retry_state: RetryCallState) -> Any:
    """Custom hook to handle exceptions after all retries fail.

    It raises a custom MpesaApiException with the appropriate error code.
    """
    if retry_state.outcome:
        exception = retry_state.outcome.exception()

        if isinstance(exception, httpx.TimeoutException):
            raise MpesaApiException(
                MpesaError(error_code="REQUEST_TIMEOUT", error_message=str(exception))
            ) from exception
        elif isinstance(exception, httpx.ConnectError):
            raise MpesaApiException(
                MpesaError(error_code="CONNECTION_ERROR", error_message=str(exception))
            ) from exception

        raise MpesaApiException(
            MpesaError(error_code="REQUEST_FAILED", error_message=str(exception))
        ) from exception

    raise MpesaApiException(
        MpesaError(
            error_code="REQUEST_FAILED",
            error_message="An unknown retry error occurred.",
        )
    )
