"""Unit tests for B2PService class."""

import pytest

from mpesakit.services.b2p import B2PService
from mpesakit.b2p import B2PResponse, B2PCommandIDType


@pytest.fixture
def b2p_service(mock_http_client, mock_token_manager):
    """Fixture to create a B2PService instance with mocked dependencies."""
    return B2PService(
        http_client=mock_http_client,
        token_manager=mock_token_manager,
    )


def test_send_payment_calls_b2p_send_payment(b2p_service, mock_http_client):
    """Test that send_payment calls the B2Pochi service."""
    response_data = {
        "OriginatorConversationID": "600997_Test_32et3241ed8yu",
        "ConversationID": "AG_20240706_20106e9209f64bebd05b",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_http_client.post.return_value = response_data

    resp = b2p_service.send_payment(
        originator_conversation_id="600997_Test_32et3241ed8yu",
        initiator_name="testapi",
        security_credential="encrypted_credential",
        amount=10,
        party_a="600992",
        party_b="254705912645",
        remarks="remarked",
        queue_timeout_url="https://example.com/timeout",
        result_url="https://example.com/result",
        occasion="ChristmasPay",
    )
    assert isinstance(resp, B2PResponse)
    assert resp.ResponseCode == "0"
    assert resp.is_successful is True
    args, kwargs = mock_http_client.post.call_args
    assert args[0] == "/mpesa/b2pochi/v1/paymentrequest"
    assert kwargs["json"]["CommandID"] == "BusinessPayToPochi"
    assert kwargs["json"]["Occassion"] == "ChristmasPay"


def test_send_payment_defaults_command_id(b2p_service, mock_http_client):
    """Test that send_payment defaults CommandID to BusinessPayToPochi."""
    mock_http_client.post.return_value = {
        "OriginatorConversationID": "600997_Test_32et3241ed8yu",
        "ConversationID": "AG_20240706_20106e9209f64bebd05b",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }

    b2p_service.send_payment(
        originator_conversation_id="600997_Test_32et3241ed8yu",
        initiator_name="testapi",
        security_credential="encrypted_credential",
        amount=10,
        party_a="600992",
        party_b="254705912645",
        remarks="remarked",
        queue_timeout_url="https://example.com/timeout",
        result_url="https://example.com/result",
    )

    assert (
        mock_http_client.post.call_args.kwargs["json"]["CommandID"]
        == B2PCommandIDType.BusinessPayToPochi.value
    )


def test_send_payment_filters_kwargs(b2p_service, mock_http_client):
    """Test that send_payment filters out unexpected kwargs."""
    response_data = {
        "OriginatorConversationID": "600997_Test_32et3241ed8yu",
        "ConversationID": "AG_20240706_20106e9209f64bebd05b",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_http_client.post.return_value = response_data

    resp = b2p_service.send_payment(
        originator_conversation_id="600997_Test_32et3241ed8yu",
        initiator_name="testapi",
        security_credential="encrypted_credential",
        amount=10,
        party_a="600992",
        party_b="254705912645",
        remarks="remarked",
        queue_timeout_url="https://example.com/timeout",
        result_url="https://example.com/result",
        unexpected_field="should be ignored",
    )
    assert isinstance(resp, B2PResponse)
    assert resp.ResponseCode == "0"


def test_b2p_service_initializes_b2p_correctly(mock_http_client, mock_token_manager):
    """Test B2PService initializes with correct arguments."""
    service = B2PService(
        http_client=mock_http_client,
        token_manager=mock_token_manager,
    )
    assert service.http_client is mock_http_client
    assert service.token_manager is mock_token_manager
    assert service.b2p.http_client is mock_http_client
    assert service.b2p.token_manager is mock_token_manager
