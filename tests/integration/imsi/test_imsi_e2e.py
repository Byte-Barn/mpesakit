"""End-to-End Tests for M-Pesa IMSI / SIM Swap Query Requests (Sync & Async V1, V2, V3)."""

import os
import pytest
import pytest_asyncio
from dotenv import load_dotenv

from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.http_client import MpesaAsyncHttpClient, MpesaHttpClient
from mpesakit.imsi import (
    IMSIV1Response,
    IMSIV2Response,
    IMSIV3Response,
)
from mpesakit.services.imsi import AsyncIMSIService, IMSIService

load_dotenv()
pytestmark = pytest.mark.live


@pytest.fixture
def imsi_service():
    """Initialize the synchronous M-Pesa IMSI facade service with authentication."""
    http_client = MpesaHttpClient(env=os.getenv("MPESA_ENV", "sandbox"))
    token_manager = TokenManager(
        consumer_key=os.getenv("MPESA_CONSUMER_KEY"),
        consumer_secret=os.getenv("MPESA_CONSUMER_SECRET"),
        http_client=http_client,
    )
    return IMSIService(http_client=http_client, token_manager=token_manager)


@pytest_asyncio.fixture(loop_scope="session")
async def async_imsi_service():
    """Initialize the asynchronous M-Pesa IMSI facade service with authentication."""
    http_client = MpesaAsyncHttpClient(env=os.getenv("MPESA_ENV", "sandbox"))
    token_manager = AsyncTokenManager(
        consumer_key=os.getenv("MPESA_CONSUMER_KEY"),
        consumer_secret=os.getenv("MPESA_CONSUMER_SECRET"),
        http_client=http_client,
    )
    return AsyncIMSIService(http_client=http_client, token_manager=token_manager)


def test_imsi_check_ati_v1_e2e(imsi_service):
    """End-to-end test for synchronous IMSI Check ATI V1 Request."""
    customer_number = os.getenv("MPESA_RECIPIENT_PHONE", "254712345678")
    response = imsi_service.query_v1(customer_number=customer_number)

    assert isinstance(response, IMSIV1Response)
    assert response.requestRefID is not None
    assert response.responseCode in ("200", "0")
    assert response.lastSwapDate is not None
    assert isinstance(response.is_recently_swapped, bool)


def test_imsi_check_ati_v2_e2e(imsi_service):
    """End-to-end test for synchronous IMSI Check ATI V2 Request."""
    customer_number = os.getenv("MPESA_RECIPIENT_PHONE", "254712345678")
    response = imsi_service.query_v2(customer_number=customer_number)

    assert isinstance(response, IMSIV2Response)
    assert response.requestRefID is not None
    assert response.responseCode in ("200", "0")
    assert response.msisdnRegistrationDate is None or isinstance(response.msisdnRegistrationDate, str)


@pytest.mark.xfail(
    reason="Safaricom Daraja V3 ATI endpoint exists in documentation but returns 404 in Sandbox environment.",
    strict=False,
)
def test_imsi_check_ati_v3_e2e(imsi_service):
    """End-to-end test for synchronous IMSI Check ATI V3 Request."""
    customer_number = os.getenv("MPESA_RECIPIENT_PHONE", "254712345678")
    response = imsi_service.query_v3(customer_number=customer_number)

    assert isinstance(response, IMSIV3Response)
    assert response.requestRefID is not None
    assert response.imsi is not None


@pytest.mark.asyncio(loop_scope="session")
async def test_async_imsi_check_ati_v1_e2e(async_imsi_service):
    """End-to-end test for asynchronous IMSI Check ATI V1 Request."""
    customer_number = os.getenv("MPESA_RECIPIENT_PHONE", "254712345678")
    response = await async_imsi_service.query_v1(customer_number=customer_number)

    assert isinstance(response, IMSIV1Response)
    assert response.requestRefID is not None
    assert response.responseCode in ("200", "0")
    assert response.lastSwapDate is not None
    assert isinstance(response.is_recently_swapped, bool)


@pytest.mark.asyncio(loop_scope="session")
async def test_async_imsi_check_ati_v2_e2e(async_imsi_service):
    """End-to-end test for asynchronous IMSI Check ATI V2 Request."""
    customer_number = os.getenv("MPESA_RECIPIENT_PHONE", "254712345678")
    response = await async_imsi_service.query_v2(customer_number=customer_number)

    assert isinstance(response, IMSIV2Response)
    assert response.requestRefID is not None
    assert response.responseCode in ("200", "0")
    assert response.msisdnRegistrationDate is None or isinstance(response.msisdnRegistrationDate, str)


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.xfail(
    reason="Safaricom Daraja V3 ATI endpoint exists in documentation but returns 404 in Sandbox environment.",
    strict=False,
)
async def test_async_imsi_check_ati_v3_e2e(async_imsi_service):
    """End-to-end test for asynchronous IMSI Check ATI V3 Request."""
    customer_number = os.getenv("MPESA_RECIPIENT_PHONE", "254712345678")
    response = await async_imsi_service.query_v3(customer_number=customer_number)

    assert isinstance(response, IMSIV3Response)
    assert response.requestRefID is not None
    assert response.imsi is not None
