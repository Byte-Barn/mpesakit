"""Unit tests for TransactionService class."""

import pytest
from unittest.mock import MagicMock
from mpesakit.services.transaction import TransactionService
from mpesakit.transaction_status import TransactionStatusResponse
from mpesakit.auth import TokenManager
from mpesakit.http_client import HttpClient


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
def transaction_service(mock_http_client, mock_token_manager):
    """Fixture to create a TransactionService instance with mocked dependencies."""
    return TransactionService(
        http_client=mock_http_client,
        token_manager=mock_token_manager,
    )


def test_query_status_calls_transaction_status(transaction_service, mock_http_client):
    """Test that query_status calls TransactionStatus.query with correct request."""
    response_data = {
        "ConversationID": "AG_20170717_00006c6f7f5b8b6b1a62",
        "OriginatorConversationID": "12345-67890-1",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_http_client.post.return_value = response_data

    resp = transaction_service.query_status(
        initiator="testapi",
        security_credential="encrypted_credential",
        command_id="TransactionStatusQuery",
        transaction_id="LKXXXX1234",
        party_a=600999,
        identifier_type=4,
        result_url="https://example.com/result",
        queue_timeout_url="https://example.com/timeout",
        remarks="Status check for transaction",
        occasion="JuneSalary",
    )
    assert isinstance(resp, TransactionStatusResponse)
    assert resp.is_successful() is True
    assert resp.ResponseDescription == "Accept the service request successfully."

def test_query_status_default_command_id(transaction_service, mock_http_client):
    """Test that query_status calls TransactionStatus.query with correct request."""
    response_data = {
        "ConversationID": "AG_20170717_00006c6f7f5b8b6b1a62",
        "OriginatorConversationID": "12345-67890-1",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_http_client.post.return_value = response_data

    resp = transaction_service.query_status(
        initiator="testapi",
        security_credential="encrypted_credential",
        # command_id="TransactionStatusQuery", # Use the Default CommandID inside the Schemas instead
        transaction_id="LKXXXX1234",
        party_a=600999,
        identifier_type=4,
        result_url="https://example.com/result",
        queue_timeout_url="https://example.com/timeout",
        remarks="Status check for transaction",
        occasion="JuneSalary",
    )

    # Assumption is that the default CommandID is used inside the TransactionStatusRequest
    assert isinstance(resp, TransactionStatusResponse)
    assert resp.is_successful() is True
    assert resp.ResponseDescription == "Accept the service request successfully."

def test_query_status_default_remarks(transaction_service, mock_http_client):
    """Test that query_status calls TransactionStatus.query with correct request."""
    response_data = {
        "ConversationID": "AG_20170717_00006c6f7f5b8b6b1a62",
        "OriginatorConversationID": "12345-67890-1",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_http_client.post.return_value = response_data


    resp = transaction_service.query_status(
        initiator="testapi",
        security_credential="encrypted_credential",
        command_id="TransactionStatusQuery",
        transaction_id="LKXXXX1234",
        party_a=600999,
        identifier_type=4,
        result_url="https://example.com/result",
        queue_timeout_url="https://example.com/timeout",
        # remarks="Status check for transaction", # Use the Default remarks given
        occasion="JuneSalary",
    )

    # Assumption is that the default Remarks is used inside the TransactionStatusRequest
    assert isinstance(resp, TransactionStatusResponse)
    assert resp.is_successful() is True
    assert resp.ResponseDescription == "Accept the service request successfully."


def test_query_status_filters_kwargs(transaction_service, mock_http_client):
    """Test that query_status filters out unexpected kwargs."""
    response_data = {
        "ConversationID": "AG_20170717_00006c6f7f5b8b6b1a62",
        "OriginatorConversationID": "12345-67890-1",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_http_client.post.return_value = response_data

    resp = transaction_service.query_status(
        initiator="testapi",
        security_credential="encrypted_credential",
        command_id="TransactionStatusQuery",
        transaction_id="LKXXXX1234",
        party_a=600999,
        identifier_type=4,
        result_url="https://example.com/result",
        queue_timeout_url="https://example.com/timeout",
        remarks="Status check for transaction",
        occasion="JuneSalary",
        unexpected_field="should be ignored",
    )
    assert isinstance(resp, TransactionStatusResponse)
    assert resp.is_successful() is True
    # unexpected_field should not be present in the response
    assert not hasattr(resp, "unexpected_field")


def test_transaction_service_initializes_correctly(
    mock_http_client, mock_token_manager
):
    """Test TransactionService initializes with correct arguments."""
    service = TransactionService(
        http_client=mock_http_client,
        token_manager=mock_token_manager,
    )
    assert service.http_client is mock_http_client
    assert service.token_manager is mock_token_manager
