"""Unit tests for the Lipa Na Bonga functionality of the Mpesa SDK."""

import pytest

from mpesakit.lipa_na_bonga import (
    LipaNaBonga,
    AsyncLipaNaBonga,
    CalculatePointsRequest,
    CalculatePointsResponse,
    RedeemPointsRequest,
    RedeemPointsResponse,
)
from mpesakit.services.lipa_na_bonga import BongaService, AsyncBongaService


@pytest.fixture
def bonga(mock_http_client, mock_token_manager):
    """Fixture to create an instance of LipaNaBonga with mocked dependencies."""
    return LipaNaBonga(http_client=mock_http_client, token_manager=mock_token_manager)


@pytest.fixture
def bonga_service(mock_http_client, mock_token_manager):
    """Fixture to create an instance of BongaService facade."""
    return BongaService(http_client=mock_http_client, token_manager=mock_token_manager)


def test_calculate_points_success(bonga, mock_http_client):
    """Test that a successful Bonga points calculation can be performed."""
    request = CalculatePointsRequest(points="40")
    response_data = {
        "header": {
            "requestRefId": "55b2b8bd-0be4-4430-b0dc-792efccdc690",
            "responseCode": 200,
            "responseMessage": "Success",
            "customerMessage": "Request executed successfully.",
            "timestamp": "2025-02-24T12:29:05.484864516",
        },
        "body": {
            "amount": "8",
            "points": "40",
            "rate": "0.2",
        },
    }
    mock_http_client.post.return_value = response_data

    response = bonga.calculate_points(request)

    assert isinstance(response, CalculatePointsResponse)
    assert response.is_successful is True
    assert response.body.amount == "8"
    assert response.body.rate == "0.2"
    mock_http_client.post.assert_called_once()
    args, kwargs = mock_http_client.post.call_args
    assert args[0] == "/v1/lipa/na/bonga/calculate-points"
    assert kwargs["headers"]["Authorization"] == "Bearer test_token"


def test_calculate_points_handles_http_error(bonga, mock_http_client):
    """Test that points calculation handles HTTP errors gracefully."""
    request = CalculatePointsRequest(points="40")
    mock_http_client.post.side_effect = Exception("HTTP error")

    with pytest.raises(Exception) as excinfo:
        bonga.calculate_points(request)
    assert "HTTP error" in str(excinfo.value)


def test_redeem_points_success(bonga, mock_http_client):
    """Test that a successful Bonga points redemption request can be sent."""
    request = RedeemPointsRequest(
        msisdn="254720776155",
        amount=50,
        bongaPoints=250,
        conversionRate=0.2,
        shortCode="888880",
        accountNumber="test",
    )
    response_data = {
        "header": {
            "requestRefId": "a53a2939-7361-482f-ba1e-ccd51504acd3",
            "responseCode": 6000,
            "responseMessage": "Operation Successfully.",
            "customerMessage": "Dear customer, your request was processed successfully",
            "timestamp": "2026-03-10T09:54:28.456847481",
        },
        "body": None,
    }
    mock_http_client.post.return_value = response_data

    response = bonga.redeem_points(request)

    assert isinstance(response, RedeemPointsResponse)
    assert response.is_successful is True
    assert response.header.responseCode == 6000
    assert response.header.requestRefId == "a53a2939-7361-482f-ba1e-ccd51504acd3"
    mock_http_client.post.assert_called_once()
    args, kwargs = mock_http_client.post.call_args
    assert args[0] == "/v1/lipa/na/bonga/redeem-paybill"
    assert kwargs["headers"]["Authorization"] == "Bearer test_token"


def test_redeem_points_failure_codes(bonga, mock_http_client):
    """Test handling of business-level failure codes (e.g., 6007 CBS unavailable, 2001 Wrong PIN)."""
    request = RedeemPointsRequest(
        msisdn="254720776155",
        amount=50,
        bongaPoints=250,
        conversionRate=0.2,
        shortCode="888880",
        accountNumber="test",
    )
    # Testing response code 2001 (Wrong PIN entered)
    response_data = {
        "header": {
            "requestRefId": "err-ref-999",
            "responseCode": 2001,
            "responseMessage": "Fail",
            "customerMessage": "Wrong PIN entered",
            "timestamp": "2026-03-10T10:00:00",
        },
        "body": None,
    }
    mock_http_client.post.return_value = response_data

    response = bonga.redeem_points(request)

    assert isinstance(response, RedeemPointsResponse)
    assert response.is_successful is False
    assert response.header.responseCode == 2001


def test_bonga_service_facade_calculate(bonga_service, mock_http_client):
    """Test the BongaService facade layer for point calculation."""
    response_data = {
        "header": {
            "requestRefId": "facade-test-1",
            "responseCode": 200,
            "responseMessage": "Success",
            "customerMessage": "OK",
            "timestamp": "2026-03-10T10:00:00",
        },
        "body": {"amount": "10", "points": "50", "rate": "0.2"},
    }
    mock_http_client.post.return_value = response_data

    response = bonga_service.calculate_points(points=50)

    assert isinstance(response, CalculatePointsResponse)
    assert response.body.amount == "10"
    mock_http_client.post.assert_called_once()


def test_bonga_service_facade_redeem(bonga_service, mock_http_client):
    """Test the BongaService facade layer for points redemption."""
    response_data = {
        "header": {
            "requestRefId": "facade-test-2",
            "responseCode": 6000,
            "responseMessage": "Success",
            "customerMessage": "OK",
            "timestamp": "2026-03-10T10:00:00",
        },
        "body": None,
    }
    mock_http_client.post.return_value = response_data

    response = bonga_service.redeem_points(
        msisdn="254720000000",
        amount=20,
        bonga_points=100,
        conversion_rate=0.2,
        short_code="174379",
        account_number="TestAcc",
    )

    assert isinstance(response, RedeemPointsResponse)
    assert response.is_successful is True
    mock_http_client.post.assert_called_once()


@pytest.fixture
def async_bonga(mock_async_http_client, mock_async_token_manager):
    """Fixture to create an instance of AsyncLipaNaBonga with mocked dependencies."""
    return AsyncLipaNaBonga(
        http_client=mock_async_http_client, token_manager=mock_async_token_manager
    )


@pytest.fixture
def async_bonga_service(mock_async_http_client, mock_async_token_manager):
    """Fixture to create an instance of AsyncBongaService facade."""
    return AsyncBongaService(http_client=mock_async_http_client, token_manager=mock_async_token_manager)


@pytest.mark.asyncio
async def test_async_calculate_points_success(async_bonga, mock_async_http_client):
    """Test that successful async points calculation can be performed."""
    request = CalculatePointsRequest(points="100")
    response_data = {
        "header": {
            "requestRefId": "async-ref-123",
            "responseCode": 200,
            "responseMessage": "Success",
            "customerMessage": "OK",
            "timestamp": "2026-03-10T10:00:00",
        },
        "body": {
            "amount": "20",
            "points": "100",
            "rate": "0.2",
        },
    }
    mock_async_http_client.post.return_value = response_data

    response = await async_bonga.calculate_points(request)

    assert isinstance(response, CalculatePointsResponse)
    assert response.body.amount == "20"
    mock_async_http_client.post.assert_called_once()


@pytest.mark.asyncio
async def test_async_bonga_service_facade_redeem(async_bonga_service, mock_async_http_client):
    """Test async BongaService facade layer for redemption."""
    response_data = {
        "header": {
            "requestRefId": "async-facade-ref",
            "responseCode": 6000,
            "responseMessage": "Success",
            "customerMessage": "Processed",
            "timestamp": "2026-03-10T10:00:00",
        },
        "body": None,
    }
    mock_async_http_client.post.return_value = response_data

    response = await async_bonga_service.redeem_points(
        msisdn="254720000000",
        amount=10,
        bonga_points=50,
        conversion_rate=0.2,
        short_code="174379",
        account_number="test",
    )

    assert isinstance(response, RedeemPointsResponse)
    assert response.is_successful is True
    mock_async_http_client.post.assert_called_once()
