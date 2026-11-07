"""Unit tests for the BongaService and AsyncBongaService classes in mpesakit.services module."""

import pytest
from mpesakit.services.lipa_na_bonga import BongaService, AsyncBongaService
from mpesakit.lipa_na_bonga import CalculatePointsResponse, RedeemPointsResponse


@pytest.fixture
def bonga_service(mock_http_client, mock_token_manager):
    """Creates a BongaService instance for testing."""
    return BongaService(
        http_client=mock_http_client,
        token_manager=mock_token_manager,
    )


@pytest.fixture
def async_bonga_service(mock_async_http_client, mock_async_token_manager):
    """Creates an AsyncBongaService instance for testing."""
    return AsyncBongaService(
        http_client=mock_async_http_client,
        token_manager=mock_async_token_manager,
    )


def test_bonga_service_initializes_correctly(mock_http_client, mock_token_manager):
    """Test BongaService initializes with correct arguments."""
    service = BongaService(
        http_client=mock_http_client,
        token_manager=mock_token_manager,
    )
    assert service.http_client is mock_http_client
    assert service.token_manager is mock_token_manager
    assert service.bonga.http_client is mock_http_client
    assert service.bonga.token_manager is mock_token_manager


def test_calculate_points_success(bonga_service, mock_http_client):
    """Test successful calculation of Bonga points via the service facade."""
    response_data = {
        "header": {
            "requestRefId": "calc-ref-123",
            "responseCode": 200,
            "responseMessage": "Success",
            "customerMessage": "OK",
            "timestamp": "2026-03-10T12:00:00",
        },
        "body": {
            "amount": "10",
            "points": "50",
            "rate": "0.2",
        },
    }
    mock_http_client.post.return_value = response_data

    resp = bonga_service.calculate_points(points=50)

    assert isinstance(resp, CalculatePointsResponse)
    assert resp.body.amount == "10"
    assert resp.body.points == "50"
    assert resp.body.rate == "0.2"


def test_calculate_points_filters_kwargs(bonga_service, mock_http_client):
    """Test that extra kwargs are safely filtered out during point calculation."""
    response_data = {
        "header": {
            "requestRefId": "calc-ref-456",
            "responseCode": 200,
            "responseMessage": "Success",
            "customerMessage": "OK",
            "timestamp": "2026-03-10T12:00:00",
        },
        "body": {
            "amount": "20",
            "points": "100",
            "rate": "0.2",
        },
    }
    mock_http_client.post.return_value = response_data

    resp = bonga_service.calculate_points(points=100, ExtraField="should_be_filtered")

    assert isinstance(resp, CalculatePointsResponse)
    assert resp.body.amount == "20"


def test_redeem_points_success(bonga_service, mock_http_client):
    """Test successful redemption of Bonga points via the service facade."""
    response_data = {
        "header": {
            "requestRefId": "redeem-ref-123",
            "responseCode": 6000,
            "responseMessage": "Success",
            "customerMessage": "Processed successfully",
            "timestamp": "2026-03-10T12:00:00",
        },
        "body": None,
    }
    mock_http_client.post.return_value = response_data

    resp = bonga_service.redeem_points(
        msisdn="254720000000",
        amount=50,
        bonga_points=250,
        conversion_rate=0.2,
        short_code="174379",
        account_number="TestAcc",
    )

    assert isinstance(resp, RedeemPointsResponse)
    assert resp.is_successful is True
    assert resp.header.responseCode == 6000


def test_redeem_points_filters_kwargs(bonga_service, mock_http_client):
    """Test that extra kwargs are safely filtered out during point redemption."""
    response_data = {
        "header": {
            "requestRefId": "redeem-ref-456",
            "responseCode": 6000,
            "responseMessage": "Success",
            "customerMessage": "Processed successfully",
            "timestamp": "2026-03-10T12:00:00",
        },
        "body": None,
    }
    mock_http_client.post.return_value = response_data

    resp = bonga_service.redeem_points(
        msisdn="254720000000",
        amount=50,
        bonga_points=250,
        conversion_rate=0.2,
        short_code="174379",
        account_number="TestAcc",
        ExtraParam="filter_me",
    )

    assert isinstance(resp, RedeemPointsResponse)
    assert resp.is_successful is True


def test_async_bonga_service_initializes_correctly(
    mock_async_http_client, mock_async_token_manager
):
    """Test AsyncBongaService initializes with correct arguments."""
    service = AsyncBongaService(
        http_client=mock_async_http_client,
        token_manager=mock_async_token_manager,
    )
    assert service.http_client is mock_async_http_client
    assert service.token_manager is mock_async_token_manager
    assert service.bonga.http_client is mock_async_http_client
    assert service.bonga.token_manager is mock_async_token_manager


@pytest.mark.asyncio
async def test_async_calculate_points_success(async_bonga_service, mock_async_http_client):
    """Test successful async calculation of Bonga points."""
    response_data = {
        "header": {
            "requestRefId": "async-calc-1",
            "responseCode": 200,
            "responseMessage": "Success",
            "customerMessage": "OK",
            "timestamp": "2026-03-10T12:00:00",
        },
        "body": {
            "amount": "10",
            "points": "50",
            "rate": "0.2",
        },
    }
    mock_async_http_client.post.return_value = response_data

    resp = await async_bonga_service.calculate_points(points=50)

    assert isinstance(resp, CalculatePointsResponse)
    assert resp.body.amount == "10"


@pytest.mark.asyncio
async def test_async_redeem_points_success(async_bonga_service, mock_async_http_client):
    """Test successful async redemption of Bonga points."""
    response_data = {
        "header": {
            "requestRefId": "async-redeem-1",
            "responseCode": 6000,
            "responseMessage": "Success",
            "customerMessage": "Processed",
            "timestamp": "2026-03-10T12:00:00",
        },
        "body": None,
    }
    mock_async_http_client.post.return_value = response_data

    resp = await async_bonga_service.redeem_points(
        msisdn="254720000000",
        amount=50,
        bonga_points=250,
        conversion_rate=0.2,
        short_code="174379",
        account_number="TestAcc",
    )

    assert isinstance(resp, RedeemPointsResponse)
    assert resp.is_successful is True
