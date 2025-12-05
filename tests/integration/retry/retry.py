# tests/test_retry.py
import pytest
from mpesakit.retry.retry import retryable_request
attempt_counter = {"count": 0}


@retryable_request(stop_attempts=5, wait_min=0, wait_max=0, exception_type=ValueError)
def sometimes_fails():
"""Fails deterministically to test retry logic."""
    attempt_counter["count"] += 1
    if attempt_counter["count"] < 5:
        raise ValueError("Planned failure")
    return "Success"
