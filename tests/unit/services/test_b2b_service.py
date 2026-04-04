"""Unit tests for B2BService class."""

import pytest
from unittest.mock import MagicMock

from mpesakit.services.b2b import B2BService, AsyncB2BService
from mpesakit.business_buy_goods import (
    BusinessBuyGoodsResponse,
)
from mpesakit.business_paybill import (
    BusinessPayBillResponse,
)
from mpesakit.b2b_express_checkout import (
    B2BExpressCheckoutResponse,
)
from mpesakit.auth import TokenManager, AsyncTokenManager
from mpesakit.http_client import HttpClient, AsyncHttpClient
from unittest.mock import AsyncMock


@pytest.fixture
def mock_token_manager():
    """Mock TokenManager to return a fixed token."""
    mock = MagicMock(spec=TokenManager)
    mock.get_token.return_value = "test_token"
    return mock


@pytest.fixture
def mock_http_client():
    """Mock HttpClient to simulate HTTP requests."""
    return MagicMock(spec=HttpClient)

@pytest.fixture
def mock_async_token_manager():
    """Mock AsyncTokenManager to return a fixed token."""
    mock = AsyncMock(spec=AsyncTokenManager)
    mock.get_token.return_value = "async_test_token"
    return mock


@pytest.fixture
def mock_async_http_client():
    """Mock AsyncHttpClient to simulate async HTTP requests."""
    return AsyncMock(spec=AsyncHttpClient)

@pytest.fixture
def b2b_service(mock_http_client, mock_token_manager):
    """Fixture to create a B2BService instance with mocked dependencies."""
    # Use real SDK classes with mocked dependencies
    return B2BService(
        http_client=mock_http_client,
        token_manager=mock_token_manager,
    )


def test_express_checkout_calls_ussd_push(b2b_service, mock_http_client):
    """Test that express_checkout calls the B2BExpressCheckout service."""
    response_data = {"code": "0", "status": "USSD Initiated Successfully"}
    mock_http_client.post.return_value = response_data

    resp = b2b_service.express_checkout(
        primary_short_code="123456",
        receiver_short_code="654321",
        amount=100,
        payment_ref="Invoice123",
        callback_url="http://example.com/result",
        partner_name="VendorName",
        request_ref_id="550e8400-e29b-41d4-a716-446655440000",
    )
    assert isinstance(resp, B2BExpressCheckoutResponse)
    assert resp.code == "0"
    assert resp.status == "USSD Initiated Successfully"


def test_paybill_calls_paybill(b2b_service, mock_http_client):
    """Test that paybill calls the BusinessPayBill service."""
    response_data = {
        "OriginatorConversationID": "5118-111210482-1",
        "ConversationID": "AG_20230420_2010759fd5662ef6d054",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_http_client.post.return_value = response_data

    resp = b2b_service.paybill(
        initiator="apiuser",
        security_credential="secure",
        amount=200,
        party_a=111111,
        party_b=222222,
        account_reference="Ref001",
        requester="254700000000",
        remarks="Test",
        queue_timeout_url="http://timeout.url",
        result_url="http://result.url",
    )
    assert isinstance(resp, BusinessPayBillResponse)
    assert resp.is_successful() is True
    assert resp.ResponseDescription == "Accept the service request successfully."


def test_buygoods_calls_buy_goods(b2b_service, mock_http_client):
    """Test that buygoods calls the BusinessBuyGoods service."""
    response_data = {
        "OriginatorConversationID": "5118-111210482-1",
        "ConversationID": "AG_20230420_2010759fd5662ef6d054",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_http_client.post.return_value = response_data

    resp = b2b_service.buygoods(
        initiator="apiuser",
        security_credential="secure",
        amount=300,
        party_a=333333,
        party_b=444444,
        account_reference="Ref002",
        requester="254711111111",
        remarks="BuyGoods",
        queue_timeout_url="http://timeout.url",
        result_url="http://result.url",
        occassion="Occasion",
    )
    assert isinstance(resp, BusinessBuyGoodsResponse)
    assert resp.is_successful() is True
    assert resp.ResponseDescription == "Accept the service request successfully."


def test_express_checkout_filters_kwargs(b2b_service, mock_http_client):
    """Test that express_checkout filters out unexpected kwargs."""
    response_data = {"code": "0", "status": "USSD Initiated Successfully"}
    mock_http_client.post.return_value = response_data

    # Pass extra fields, only valid ones should be used
    resp = b2b_service.express_checkout(
        primary_short_code="123456",
        receiver_short_code="654321",
        amount=100,
        payment_ref="Invoice123",
        callback_url="http://example.com/result",
        partner_name="VendorName",
        request_ref_id="550e8400-e29b-41d4-a716-446655440000",
        unexpected_field="should be ignored",
    )
    assert isinstance(resp, B2BExpressCheckoutResponse)
    assert resp.is_successful() is True
    assert resp.status == "USSD Initiated Successfully"


def test_b2b_service_initializes_services_correctly(
    mock_http_client, mock_token_manager
):
    """Test B2BService initializes its dependencies with correct arguments."""
    service = B2BService(
        http_client=mock_http_client,
        token_manager=mock_token_manager,
    )
    assert service.http_client is mock_http_client
    assert service.token_manager is mock_token_manager
    # If B2BService initializes sub-services, check them too
    if hasattr(service, "express_checkout_service"):
        assert service.express_checkout_service.http_client is mock_http_client
        assert service.express_checkout_service.token_manager is mock_token_manager
    if hasattr(service, "paybill_service"):
        assert service.paybill_service.http_client is mock_http_client
        assert service.paybill_service.token_manager is mock_token_manager
    if hasattr(service, "buygoods_service"):
        assert service.buygoods_service.http_client is mock_http_client
        assert service.buygoods_service.token_manager is mock_token_manager

@pytest.fixture
def async_b2b_service(mock_async_http_client, mock_async_token_manager):
    """Fixture to create an AsyncB2BService instance with mocked dependencies."""
    return AsyncB2BService(
        http_client=mock_async_http_client,
        token_manager=mock_async_token_manager
    )


@pytest.mark.asyncio
async def test_async_express_checkout_calls_ussd_push(
    async_b2b_service, mock_async_http_client
):
    """Test that async express_checkout calls the B2BExpressCheckout service."""
    response_data = {"code": "0", "status": "USSD Initiated Successfully"}
    mock_async_http_client.post.return_value = response_data

    resp = await async_b2b_service.express_checkout(
        primary_short_code="123456",
        receiver_short_code="654321",
        amount=100,
        payment_ref="Invoice123",
        callback_url="http://example.com/result",
        partner_name="VendorName",
        request_ref_id="550e8400-e29b-41d4-a716-446655440000",
    )

    assert isinstance(resp, B2BExpressCheckoutResponse)
    assert resp.code == "0"
    assert resp.status == "USSD Initiated Successfully"


@pytest.mark.asyncio
async def test_async_paybill_calls_paybill(async_b2b_service, mock_async_http_client):
    """Test that async paybill calls the BusinessPayBill service."""
    response_data = {
        "OriginatorConversationID": "5118-111210482-1",
        "ConversationID": "AG_20230420_2010759fd5662ef6d054",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_async_http_client.post.return_value = response_data

    resp = await async_b2b_service.paybill(
        initiator="apiuser",
        security_credential="secure",
        amount=200,
        party_a=111111,
        party_b=222222,
        account_reference="Ref001",
        requester="254700000000",
        remarks="Test",
        queue_timeout_url="http://timeout.url",
        result_url="http://result.url",
    )

    assert isinstance(resp, BusinessPayBillResponse)
    assert resp.is_successful() is True
    assert resp.ResponseDescription == "Accept the service request successfully."


@pytest.mark.asyncio
async def test_async_buygoods_calls_buy_goods(
    async_b2b_service, mock_async_http_client
):
    """Test that async buygoods calls the BusinessBuyGoods service."""
    response_data = {
        "OriginatorConversationID": "5118-111210482-1",
        "ConversationID": "AG_20230420_2010759fd5662ef6d054",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_async_http_client.post.return_value = response_data

    resp = await async_b2b_service.buygoods(
        initiator="apiuser",
        security_credential="secure",
        amount=300,
        party_a=333333,
        party_b=444444,
        account_reference="Ref002",
        requester="254711111111",
        remarks="BuyGoods",
        queue_timeout_url="http://timeout.url",
        result_url="http://result.url",
        occassion="Occasion",
    )

    assert isinstance(resp, BusinessBuyGoodsResponse)
    assert resp.is_successful() is True
    assert resp.ResponseDescription == "Accept the service request successfully."


@pytest.mark.asyncio
async def test_async_express_checkout_filters_kwargs(
    async_b2b_service, mock_async_http_client
):
    """Test that async express_checkout filters out unexpected kwargs."""
    response_data = {"code": "0", "status": "USSD Initiated Successfully"}
    mock_async_http_client.post.return_value = response_data

    resp = await async_b2b_service.express_checkout(
        primary_short_code="123456",
        receiver_short_code="654321",
        amount=100,
        payment_ref="Invoice123",
        callback_url="http://example.com/result",
        partner_name="VendorName",
        request_ref_id="550e8400-e29b-41d4-a716-446655440000",
        unexpected_field="should be ignored",
    )

    assert isinstance(resp, B2BExpressCheckoutResponse)
    assert resp.is_successful() is True
    assert resp.status == "USSD Initiated Successfully"


def test_async_b2b_service_initializes_services_correctly(
    mock_async_http_client, mock_async_token_manager
):
    """Test AsyncB2BService initializes dependencies with correct arguments."""
    service = AsyncB2BService(
        http_client=mock_async_http_client,
        token_manager=mock_async_token_manager,
    )

    assert service.http_client is mock_async_http_client
    assert service.token_manager is mock_async_token_manager

    express_checkout_delegate = getattr(service, "_express_checkout", None)
    paybill_delegate = getattr(service, "_business_paybill", None)
    buygoods_delegate = getattr(service, "_business_buygoods", None)

    assert express_checkout_delegate is not None
    assert express_checkout_delegate.http_client is mock_async_http_client
    assert express_checkout_delegate.token_manager is mock_async_token_manager

    assert paybill_delegate is not None
    assert paybill_delegate.http_client is mock_async_http_client
    assert paybill_delegate.token_manager is mock_async_token_manager

    assert buygoods_delegate is not None
    assert buygoods_delegate.http_client is mock_async_http_client
    assert buygoods_delegate.token_manager is mock_async_token_manager
