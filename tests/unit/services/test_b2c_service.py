"""Unit tests for B2CService class."""

import pytest
from mpesakit.services.b2c import B2CService, AsyncB2CService
from mpesakit.b2c import B2CResponse, B2CCommandIDType

from mpesakit.b2c_account_top_up import (
    B2CAccountTopUpResponse,
)

@pytest.fixture
def b2c_service(mock_http_client, mock_token_manager):
    """Fixture to create a B2CService instance with mocked dependencies."""
    return B2CService(
        http_client=mock_http_client,
        token_manager=mock_token_manager,
    )

@pytest.fixture
def async_b2c_service(mock_async_http_client, mock_async_token_manager):
    """Fixture to create an AsyncB2CService instance with mocked dependencies."""
    return AsyncB2CService(
        http_client=mock_async_http_client,
        token_manager=mock_async_token_manager,
    )

def test_send_payment_calls_b2c_send_payment(b2c_service, mock_http_client):
    """Test that send_payment calls the B2C service."""
    response_data = {
        "OriginatorConversationID": "12345",
        "ConversationID": "AG_20230601_123456789",
        "ResponseCode": "0",
        "ResponseDescription": "Request accepted successfully.",
    }
    mock_http_client.post.return_value = response_data

    resp = b2c_service.send_payment(
        originator_conversation_id="12345-67890-1",
        initiator_name="testapi",
        security_credential="encrypted_credential",
        command_id=B2CCommandIDType.BusinessPayment,
        amount=1000,
        party_a="600999",
        party_b="254712345678",
        remarks="Salary for June",
        queue_timeout_url="https://example.com/timeout",
        result_url="https://example.com/result",
        occasion="JuneSalary",
    )
    assert isinstance(resp, B2CResponse)
    assert resp.ResponseCode == "0"
    assert resp.ResponseDescription == "Request accepted successfully."

def test_account_topup_calls_account_topup(b2c_service, mock_http_client):
    """Test that account_topup calls the B2CAccountTopUp service."""
    response_data = {
        "OriginatorConversationID": "67890",
        "ConversationID": "AG_20230601_987654321",
        "ResponseCode": "0",
        "ResponseDescription": "TopUp accepted successfully.",
    }
    mock_http_client.post.return_value = response_data

    resp = b2c_service.account_topup(
        initiator="apiuser",
        security_credential="secure",
        amount=500,
        party_a="600100",
        party_b="600200",
        account_reference="RefTopUp",
        requester="254711111111",
        remarks="TopUp",
        queue_timeout_url="http://timeout.url",
        result_url="http://result.url",
    )
    assert isinstance(resp, B2CAccountTopUpResponse)
    assert resp.ResponseCode == "0"
    assert resp.ResponseDescription == "TopUp accepted successfully."

def test_send_payment_filters_kwargs(b2c_service, mock_http_client):
    """Test that send_payment filters out unexpected kwargs."""
    response_data = {
        "OriginatorConversationID": "12345",
        "ConversationID": "AG_20230601_123456789",
        "ResponseCode": "0",
        "ResponseDescription": "Request accepted successfully.",
    }
    mock_http_client.post.return_value = response_data

    resp = b2c_service.send_payment(
        originator_conversation_id="12345-67890-1",
        initiator_name="testapi",
        security_credential="encrypted_credential",
        command_id=B2CCommandIDType.BusinessPayment,
        amount=1000,
        party_a="600999",
        party_b="254712345678",
        remarks="Salary for June",
        queue_timeout_url="https://example.com/timeout",
        result_url="https://example.com/result",
        occasion="JuneSalary",
    )
    assert isinstance(resp, B2CResponse)
    assert resp.ResponseCode == "0"
    assert resp.ResponseDescription == "Request accepted successfully."

def test_account_topup_filters_kwargs(b2c_service, mock_http_client):
    """Test that account_topup filters out unexpected kwargs."""
    response_data = {
        "OriginatorConversationID": "67890",
        "ConversationID": "AG_20230601_987654321",
        "ResponseCode": "0",
        "ResponseDescription": "TopUp accepted successfully.",
    }
    mock_http_client.post.return_value = response_data

    resp = b2c_service.account_topup(
        initiator="apiuser",
        security_credential="secure",
        amount=500,
        party_a="600100",
        party_b="600200",
        account_reference="RefTopUp",
        requester="254711111111",
        remarks="TopUp",
        queue_timeout_url="http://timeout.url",
        result_url="http://result.url",
    )
    assert isinstance(resp, B2CAccountTopUpResponse)
    assert resp.ResponseCode == "0"
    assert resp.ResponseDescription == "TopUp accepted successfully."

def test_b2c_service_initializes_b2c_correctly(mock_http_client, mock_token_manager):
    """Test B2CService initializes with correct arguments."""
    service = B2CService(
        http_client=mock_http_client,
        token_manager=mock_token_manager,
    )
    assert service.http_client is mock_http_client
    assert service.token_manager is mock_token_manager
    # If B2CService has an internal b2c attribute, check its initialization
    if hasattr(service, "b2c"):
        assert service.b2c.http_client is mock_http_client
        assert service.b2c.token_manager is mock_token_manager

@pytest.mark.asyncio
async def test_async_send_payment_calls_b2c_send_payment(
    async_b2c_service, mock_async_http_client
):
    """Test that async send_payment calls the B2C service."""
    response_data = {
        "OriginatorConversationID": "12345",
        "ConversationID": "AG_20230601_123456789",
        "ResponseCode": "0",
        "ResponseDescription": "Request accepted successfully.",
    }
    mock_async_http_client.post.return_value = response_data

    resp = await async_b2c_service.send_payment(
        originator_conversation_id="12345-67890-1",
        initiator_name="testapi",
        security_credential="encrypted_credential",
        command_id=B2CCommandIDType.BusinessPayment,
        amount=1000,
        party_a="600999",
        party_b="254712345678",
        remarks="Salary for June",
        queue_timeout_url="https://example.com/timeout",
        result_url="https://example.com/result",
        occasion="JuneSalary",
    )
    assert isinstance(resp, B2CResponse)
    assert resp.ResponseCode == "0"
    assert resp.ResponseDescription == "Request accepted successfully."

@pytest.mark.asyncio
async def test_async_account_topup_calls_account_topup(
    async_b2c_service, mock_async_http_client
):
    """Test that async account_topup calls the B2CAccountTopUp service."""
    response_data = {
        "OriginatorConversationID": "67890",
        "ConversationID": "AG_20230601_987654321",
        "ResponseCode": "0",
        "ResponseDescription": "TopUp accepted successfully.",
    }
    mock_async_http_client.post.return_value = response_data

    resp = await async_b2c_service.account_topup(
        initiator="apiuser",
        security_credential="secure",
        amount=500,
        party_a="600100",
        party_b="600200",
        account_reference="RefTopUp",
        requester="254711111111",
        remarks="TopUp",
        queue_timeout_url="http://timeout.url",
        result_url="http://result.url",
    )
    assert isinstance(resp, B2CAccountTopUpResponse)
    assert resp.ResponseCode == "0"
    assert resp.ResponseDescription == "TopUp accepted successfully."

@pytest.mark.asyncio
async def test_async_send_payment_filters_kwargs(
    async_b2c_service, mock_async_http_client
):
    """Test that async send_payment filters out unexpected kwargs."""
    response_data = {
        "OriginatorConversationID": "12345",
        "ConversationID": "AG_20230601_123456789",
        "ResponseCode": "0",
        "ResponseDescription": "Request accepted successfully.",
    }
    mock_async_http_client.post.return_value = response_data

    resp = await async_b2c_service.send_payment(
        originator_conversation_id="12345-67890-1",
        initiator_name="testapi",
        security_credential="encrypted_credential",
        command_id=B2CCommandIDType.BusinessPayment,
        amount=1000,
        party_a="600999",
        party_b="254712345678",
        remarks="Salary for June",
        queue_timeout_url="https://example.com/timeout",
        result_url="https://example.com/result",
        occasion="JuneSalary",
        unexpected_field="should be ignored",
    )
    assert isinstance(resp, B2CResponse)
    assert resp.ResponseCode == "0"
    assert resp.ResponseDescription == "Request accepted successfully."

@pytest.mark.asyncio
async def test_async_account_topup_filters_kwargs(
    async_b2c_service, mock_async_http_client
):
    """Test that async account_topup filters out unexpected kwargs."""
    response_data = {
        "OriginatorConversationID": "67890",
        "ConversationID": "AG_20230601_987654321",
        "ResponseCode": "0",
        "ResponseDescription": "TopUp accepted successfully.",
    }
    mock_async_http_client.post.return_value = response_data

    resp = await async_b2c_service.account_topup(
        initiator="apiuser",
        security_credential="secure",
        amount=500,
        party_a="600100",
        party_b="600200",
        account_reference="RefTopUp",
        requester="254711111111",
        remarks="TopUp",
        queue_timeout_url="http://timeout.url",
        result_url="http://result.url",
        unexpected_field="should be ignored",
    )
    assert isinstance(resp, B2CAccountTopUpResponse)
    assert resp.ResponseCode == "0"
    assert resp.ResponseDescription == "TopUp accepted successfully."

def test_async_b2c_service_initializes_b2c_correctly(
    mock_async_http_client, mock_async_token_manager
):
    """Test AsyncB2CService initializes with correct arguments."""
    service = AsyncB2CService(
        http_client=mock_async_http_client,
        token_manager=mock_async_token_manager,
    )
    assert service.http_client is mock_async_http_client
    assert service.token_manager is mock_async_token_manager
    assert service.b2c.http_client is mock_async_http_client
    assert service.b2c.token_manager is mock_async_token_manager
    assert service._account_topup.http_client is mock_async_http_client
    assert service._account_topup.token_manager is mock_async_token_manager
