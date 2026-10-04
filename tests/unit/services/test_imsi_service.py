"""Unit tests for the IMSIService class in mpesakit.services.imsi module."""

import pytest
from mpesakit.imsi import IMSIV1Response, IMSIV2Response, IMSIV3Response
from mpesakit.services.imsi import AsyncIMSIService, IMSIService


@pytest.fixture
def imsi_service(mock_http_client, mock_token_manager):
    """Creates an IMSIService instance for testing."""
    return IMSIService(
        http_client=mock_http_client,
        token_manager=mock_token_manager,
    )


@pytest.fixture
def async_imsi_service(mock_async_http_client, mock_async_token_manager):
    """Creates an AsyncIMSIService instance for testing."""
    return AsyncIMSIService(
        http_client=mock_async_http_client,
        token_manager=mock_async_token_manager,
    )


def test_imsi_service_initializes_correctly(mock_http_client, mock_token_manager):
    """Test IMSIService initializes with correct arguments."""
    service = IMSIService(
        http_client=mock_http_client,
        token_manager=mock_token_manager,
    )
    assert service.http_client is mock_http_client
    assert service.token_manager is mock_token_manager


def test_query_v1_facade_success(imsi_service, mock_http_client):
    """Test successful facade call for V1 checkATI query."""
    response_data = {
        "requestRefID": "FACADE_V1_123",
        "responseCode": "200",
        "responseDesc": "Success",
        "lastSwapDate": "01-01-1900 00:00",
    }
    mock_http_client.post.return_value = response_data

    resp = imsi_service.query_v1(customer_number="0722000000")

    assert isinstance(resp, IMSIV1Response)
    assert resp.requestRefID == "FACADE_V1_123"
    assert resp.responseCode == "200"
    assert resp.is_recently_swapped is False


def test_query_v1_facade_filters_kwargs(imsi_service, mock_http_client):
    """Test that async V1 facade call successfully queries and returns IMSIV1Response."""
    response_data = {
        "requestRefID": "FACADE_V1_456",
        "responseCode": "200",
        "responseDesc": "Success",
        "lastSwapDate": "01-01-1900 00:00",
    }
    mock_http_client.post.return_value = response_data

    resp = imsi_service.query_v1(
        customer_number="0722000000",

    )

    assert isinstance(resp, IMSIV1Response)
    assert resp.requestRefID == "FACADE_V1_456"


def test_query_v2_facade_success(imsi_service, mock_http_client):
    """Test successful facade call for V2 checkATI query."""
    response_data = {
        "requestRefID": "FACADE_V2_123",
        "responseCode": "200",
        "msisdnRegistrationDate": "12-04-2019 15:30",
    }
    mock_http_client.post.return_value = response_data

    resp = imsi_service.query_v2(customer_number="0722000000")

    assert isinstance(resp, IMSIV2Response)
    assert resp.requestRefID == "FACADE_V2_123"
    assert resp.msisdnRegistrationDate == "12-04-2019 15:30"


def test_query_v3_facade_success(imsi_service, mock_http_client):
    """Test successful facade call for V3 checkATI query."""
    response_data = {
        "requestRefID": "FACADE_V3_123",
        "imsi": "hashed_imsi_val",
    }
    mock_http_client.post.return_value = response_data

    resp = imsi_service.query_v3(customer_number="0722000000")

    assert isinstance(resp, IMSIV3Response)
    assert resp.requestRefID == "FACADE_V3_123"
    assert resp.imsi == "hashed_imsi_val"
    assert getattr(resp,"is_recently_swapped",None) is None


def test_async_imsi_service_initializes_correctly(
    mock_async_http_client, mock_async_token_manager
):
    """Test AsyncIMSIService initializes with correct arguments."""
    service = AsyncIMSIService(
        http_client=mock_async_http_client,
        token_manager=mock_async_token_manager,
    )
    assert service.http_client is mock_async_http_client
    assert service.token_manager is mock_async_token_manager


@pytest.mark.asyncio
async def test_async_query_v1_facade_success(
    async_imsi_service, mock_async_http_client
):
    """Test successful async facade call for V1 checkATI query."""
    response_data = {
        "requestRefID": "ASYNC_FACADE_V1_123",
        "responseCode": "200",
        "responseDesc": "Success",
        "lastSwapDate": "01-01-1900 00:00",
    }
    mock_async_http_client.post.return_value = response_data

    resp = await async_imsi_service.query_v1(customer_number="0722000000")

    assert isinstance(resp, IMSIV1Response)
    assert resp.requestRefID == "ASYNC_FACADE_V1_123"
    assert resp.is_recently_swapped is False


@pytest.mark.asyncio
async def test_async_query_v1_facade_filters_kwargs(
    async_imsi_service, mock_async_http_client
):
    """Test that async V1 facade call successfully queries and returns IMSIV1Response."""
    response_data = {
        "requestRefID": "ASYNC_FACADE_V1_456",
        "responseCode": "200",
        "responseDesc": "Success",
        "lastSwapDate": "01-01-1900 00:00",
    }
    mock_async_http_client.post.return_value = response_data

    resp = await async_imsi_service.query_v1(
        customer_number="0722000000",
    )

    assert isinstance(resp, IMSIV1Response)
    assert resp.requestRefID == "ASYNC_FACADE_V1_456"


@pytest.mark.asyncio
async def test_async_query_v2_facade_success(
    async_imsi_service, mock_async_http_client
):
    """Test successful async facade call for V2 checkATI query."""
    response_data = {
        "requestRefID": "ASYNC_FACADE_V2_123",
        "responseCode": "200",
        "msisdnRegistrationDate": "01-01-2021 12:00",
    }
    mock_async_http_client.post.return_value = response_data

    resp = await async_imsi_service.query_v2(customer_number="0722000000")

    assert isinstance(resp, IMSIV2Response)
    assert resp.requestRefID == "ASYNC_FACADE_V2_123"


@pytest.mark.asyncio
async def test_async_query_v3_facade_success(
    async_imsi_service, mock_async_http_client
):
    """Test successful async facade call for V3 checkATI query."""
    response_data = {
        "requestRefID": "ASYNC_FACADE_V3_123",
        "imsi": "async_hashed_imsi_val",
        "lastSwapDate": "05-05-2025 10:15",
    }
    mock_async_http_client.post.return_value = response_data

    resp = await async_imsi_service.query_v3(customer_number="0722000000")

    assert isinstance(resp, IMSIV3Response)
    assert resp.requestRefID == "ASYNC_FACADE_V3_123"
    assert resp.imsi == "async_hashed_imsi_val"
    assert getattr(resp,"is_recently_swapped",None) is None
