"""Unit tests for TaxService class."""

import pytest
from mpesakit.services.tax import TaxService
from mpesakit.tax_remittance import TaxRemittanceResponse

@pytest.fixture
def tax_service(mock_http_client, mock_token_manager):
    """Fixture to create a TaxService instance with mocked dependencies."""
    return TaxService(
        http_client=mock_http_client,
        token_manager=mock_token_manager,
    )

def test_remittance_calls_tax_remittance(tax_service, mock_http_client):
    """Test that remittance calls TaxRemittance.remittance with correct request."""
    response_data = {
        "OriginatorConversationID": "5118-111210482-1",
        "ConversationID": "AG_20230420_2010759fd5662ef6d054",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_http_client.post.return_value = response_data

    resp = tax_service.remittance(
        initiator="TaxPayer",
        security_credential="encrypted_credential",
        amount=239,
        party_a=888880,
        remarks="OK",
        account_reference="353353",
        result_url="https://mydomain.com/b2b/remittax/result/",
        queue_timeout_url="https://mydomain.com/b2b/remittax/queue/",
    )
    assert isinstance(resp, TaxRemittanceResponse)
    assert resp.is_successful() is True
    assert resp.ResponseDescription == "Accept the service request successfully."

def test_remittance_filters_kwargs(tax_service, mock_http_client):
    """Test that remittance filters out unexpected kwargs."""
    response_data = {
        "OriginatorConversationID": "5118-111210482-1",
        "ConversationID": "AG_20230420_2010759fd5662ef6d054",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_http_client.post.return_value = response_data

    resp = tax_service.remittance(
        initiator="TaxPayer",
        security_credential="encrypted_credential",
        amount=239,
        party_a=888880,
        remarks="OK",
        account_reference="353353",
        result_url="https://mydomain.com/b2b/remittax/result/",
        queue_timeout_url="https://mydomain.com/b2b/remittax/queue/",
        unexpected_field="should be ignored",
    )
    assert isinstance(resp, TaxRemittanceResponse)
    assert resp.is_successful() is True
    # unexpected_field should not be present in the response
    assert not hasattr(resp, "unexpected_field")

def test_tax_service_initializes_tax_correctly(mock_http_client, mock_token_manager):
    """Test TaxService initializes with correct http_client and token_manager."""
    service = TaxService(
        http_client=mock_http_client,
        token_manager=mock_token_manager,
    )
    assert service.http_client is mock_http_client
    assert service.token_manager is mock_token_manager
    # If TaxService has an internal 'tax' or similar attribute, check it as well
    if hasattr(service, "tax"):
        assert service.tax.http_client is mock_http_client
        assert service.tax.token_manager is mock_token_manager
