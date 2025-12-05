# mpesakit/retry.py

"""
HOW IT WORKS:
____________

Decorator Factory: retryable_request() is reusable and configurable.

Logging: Logs each retry attempt.

Post request helper: post_request() wraps requests.post for automatic retries.
"""
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import requests
import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def retryable_request(
        stop_attempts: int = 3,
        wait_min: int = 1,
        wait_max: int = 10,
        exception_type=Exception
):
    """
    Decorator factory that returns a retry decorator using tenacity.

    Usage:
        @retryable_request()
        def your_func(...):
            ...
    """
    return retry(
        stop=stop_after_attempt(stop_attempts),
        wait=wait_exponential(multiplier=1, min=wait_min, max=wait_max),
        retry=retry_if_exception_type(exception_type),
        before=lambda retry_state: logger.info(
            f"Retrying {retry_state.fn.__name__} after attempt {retry_state.attempt_number}"
        ),
        after=lambda retry_state: logger.warning(
            f"Finished attempt {retry_state.attempt_number} for {retry_state.fn.__name__}"
        ),
    )


# Example wrapper for requests.post
@retryable_request(exception_type=requests.exceptions.RequestException)
def post_request(url: str, payload: dict, headers: dict = None):
    """Send POST request with retry logic."""
    headers = headers or {}
    response = requests.post(url, json=payload, headers=headers, timeout=10)
    response.raise_for_status()
    return response.json()
