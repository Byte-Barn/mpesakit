"""Centralized and shareable fixtures and configs for integration tests."""

import os
import pytest
from dotenv import load_dotenv
from pathlib import Path

# Set the project base directory
# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session", autouse=True)
def load_env_file():
    """Loads a .env file once at the start of the entire test session."""
    env_file = Path(BASE_DIR, ".envs", "sandbox.env")
    load_dotenv(dotenv_path=env_file, override=True)


@pytest.fixture(scope="session", autouse=True)
def validate_live_environment():
    """Global check to ensure required credentials are configured.

    Runs once for ALL integration tests. Fails fast if credentials are missing.
    """
    consumer_key = os.getenv("MPESA_CONSUMER_KEY")
    consumer_secret = os.getenv("MPESA_CONSUMER_SECRET")
    if not consumer_key or not consumer_secret:
        pytest.fail(
            """
            Caution: Authentication is REQUIRED.

            Live tests must run against the real sandbox API.
            Please set MPESA_CONSUMER_KEY and MPESA_CONSUMER_SECRET in your
            environment or .env file.
            """
        )


@pytest.fixture(scope="session")
def mpesa_http_client():
    """Shared single instance client for all services.

    Created once per test session.
    """
    from mpesakit.http_client.mpesa_http_client import MpesaHttpClient

    env = os.getenv("MPESA_ENVIRONMENT", "sandbox")
    return MpesaHttpClient(env=env)


@pytest.fixture(scope="session")
def token_manager(mpesa_http_client):
    """Shared single instance token manager.

    Fetches an OAuth token ONCE and caches it for the entire test session.
    """
    from mpesakit.auth import TokenManager

    return TokenManager(
        http_client=mpesa_http_client,
        consumer_key=os.getenv("MPESA_CONSUMER_KEY", ""),
        consumer_secret=os.getenv("MPESA_CONSUMER_SECRET", ""),
    )
