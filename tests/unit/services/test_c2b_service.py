"""Unit tests for the C2BService class in mpesakit.services.c2b module."""

import pytest
from mpesakit.services.c2b import C2BService, AsyncC2BService
from mpesakit.c2b import C2BRegisterUrlResponse, C2BResponseType


@pytest.fixture
def c2b_service(mock_http_client, mock_token_manager):
    """Creates a C2BService instance for testing."""
    return C2BService(
        http_client=mock_http_client,
        token_manager=mock_token_manager,
    )


@pytest.fixture
def async_c2b_service(mock_async_http_client, mock_async_token_manager):
    """Creates an AsyncC2BService instance for testing."""
    return AsyncC2BService(
        http_client=mock_async_http_client,
        token_manager=mock_async_token_manager,
    )


def test_register_url_success(c2b_service, mock_http_client):
    """Test successful registration of C2B URLs."""
    response_data = {
        "OriginatorConversationID": "12345",
        "ConversationID": "AG_20230601_123456789",
        "ResponseCode": "0",
        "ResponseDescription": "URLs registered successfully.",
    }
    mock_http_client.post.return_value = response_data

    resp = c2b_service.register_url(
        short_code=600999,
        response_type=C2BResponseType.COMPLETED,
        confirmation_url="https://example.com/confirm",
        validation_url="https://example.com/validate",
    )
    assert isinstance(resp, C2BRegisterUrlResponse)
    assert resp.ResponseCode == "0"
    assert resp.ResponseDescription == "URLs registered successfully."


def test_register_url_filters_kwargs(c2b_service, mock_http_client):
    """Test that extra kwargs are filtered out in the request."""
    response_data = {
        "OriginatorConversationID": "67890",
        "ConversationID": "AG_20230601_987654321",
        "ResponseCode": "0",
        "ResponseDescription": "URLs registered successfully.",
    }
    mock_http_client.post.return_value = response_data

    resp = c2b_service.register_url(
        short_code=600999,
        response_type=C2BResponseType.COMPLETED,
        confirmation_url="https://example.com/confirm",
        validation_url="https://example.com/validate",
        ExtraField="should_be_filtered",
    )
    assert isinstance(resp, C2BRegisterUrlResponse)
    assert resp.ResponseCode == "0"
    assert resp.ResponseDescription == "URLs registered successfully."


def test_c2b_service_initializes_correctly(mock_http_client, mock_token_manager):
    """Test C2BService initializes with correct arguments."""
    service = C2BService(
        http_client=mock_http_client,
        token_manager=mock_token_manager,
    )
    assert service.http_client is mock_http_client
    assert service.token_manager is mock_token_manager


@pytest.mark.asyncio
async def test_async_register_url_success(async_c2b_service, mock_async_http_client):
    """Test successful async registration of C2B URLs."""
    response_data = {
        "OriginatorConversationID": "12345",
        "ConversationID": "AG_20230601_123456789",
        "ResponseCode": "0",
        "ResponseDescription": "URLs registered successfully.",
    }
    mock_async_http_client.post.return_value = response_data

    resp = await async_c2b_service.register_url(
        short_code=600999,
        response_type=C2BResponseType.COMPLETED,
        confirmation_url="https://example.com/confirm",
        validation_url="https://example.com/validate",
    )
    assert isinstance(resp, C2BRegisterUrlResponse)
    assert resp.ResponseCode == "0"
    assert resp.ResponseDescription == "URLs registered successfully."


@pytest.mark.asyncio
async def test_async_register_url_filters_kwargs(
    async_c2b_service, mock_async_http_client
):
    """Test that async register_url filters out unexpected kwargs."""
    response_data = {
        "OriginatorConversationID": "67890",
        "ConversationID": "AG_20230601_987654321",
        "ResponseCode": "0",
        "ResponseDescription": "URLs registered successfully.",
    }
    mock_async_http_client.post.return_value = response_data

    resp = await async_c2b_service.register_url(
        short_code=600999,
        response_type=C2BResponseType.COMPLETED,
        confirmation_url="https://example.com/confirm",
        validation_url="https://example.com/validate",
        ExtraField="should_be_filtered",
    )
    assert isinstance(resp, C2BRegisterUrlResponse)
    assert resp.ResponseCode == "0"
    assert resp.ResponseDescription == "URLs registered successfully."


def test_async_c2b_service_initializes_correctly(
    mock_async_http_client, mock_async_token_manager
):
    """Test AsyncC2BService initializes with correct arguments."""
    service = AsyncC2BService(
        http_client=mock_async_http_client,
        token_manager=mock_async_token_manager,
    )
    assert service.http_client is mock_async_http_client
    assert service.token_manager is mock_async_token_manager
    assert service.c2b.http_client is mock_async_http_client
    assert service.c2b.token_manager is mock_async_token_manager
