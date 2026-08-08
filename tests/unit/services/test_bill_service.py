"""Unit tests for BillService class."""

import pytest
from unittest.mock import MagicMock
from mpesakit.services.bill import BillService, AsyncBillService

from mpesakit.bill_manager import (
    BillManagerOptInResponse,
    BillManagerUpdateOptInResponse,
    BillManagerSingleInvoiceResponse,
    BillManagerBulkInvoiceResponse,
    BillManagerCancelInvoiceResponse,
    BillManagerSingleInvoiceRequest,
    InvoiceItem,
)

@pytest.fixture
def bill_service(mock_http_client, mock_token_manager):
    """Fixture to create a BillService instance with mocked dependencies."""
    return BillService(
        http_client=mock_http_client,
        token_manager=mock_token_manager,
    )

@pytest.fixture
def bill_service_with_app_key(mock_http_client, mock_token_manager):
    """Fixture to create a BillService instance with mocked dependencies."""
    return BillService(
        http_client=mock_http_client,
        token_manager=mock_token_manager,
        app_key="test_app_key",
    )


@pytest.fixture
def async_bill_service(mock_async_http_client, mock_async_token_manager):
    """Fixture to create an AsyncBillService instance with mocked dependencies."""
    return AsyncBillService(
        http_client=mock_async_http_client,
        token_manager=mock_async_token_manager,
    )


@pytest.fixture
def async_bill_service_with_app_key(
    mock_async_http_client, mock_async_token_manager
):
    """Fixture to create an AsyncBillService with an app key."""
    return AsyncBillService(
        http_client=mock_async_http_client,
        token_manager=mock_async_token_manager,
        app_key="test_app_key",
    )


def test_opt_in_calls_bill_manager_opt_in(bill_service, mock_http_client):
    """Test opt_in calls BillManager.opt_in."""
    response_data = {
        "app_key": "AG_2376487236_126732989KJ",
        "resmsg": "Success",
        "rescode": "200",
    }
    mock_http_client.post.return_value = response_data

    resp = bill_service.opt_in(
        shortcode=123456,
        email="test@example.com",
        official_contact="0712345678",
        send_reminders=1,
        logo=None,
        callback_url="https://callback.url",
    )

    assert isinstance(resp, BillManagerOptInResponse)
    resp.is_successful is True

def test_bill_manager_update_opt_in(bill_service_with_app_key, mock_http_client):
    """Test update_opt_in calls BillManager.update_opt_in."""
    response_data = {
        "resmsg": "Success",
        "rescode": "200",
    }
    mock_http_client.post.return_value = response_data

    resp = bill_service_with_app_key.update_opt_in(
        shortcode=123456,
        email="update@example.com",
        official_contact="0712345678",
        send_reminders=0,
        logo="logo.png",
        callback_url="https://callback.url",
    )
    assert isinstance(resp, BillManagerUpdateOptInResponse)
    assert resp.is_successful is True

def test_bill_manager_send_single_invoice(
    bill_service_with_app_key,
    mock_http_client,
):
    """Test send_single_invoice calls BillManager.send_single_invoice."""
    response_data = {
        "Status_Message": "Invoice sent successfully",
        "resmsg": "Success",
        "rescode": "200",
    }
    mock_http_client.post.return_value = response_data

    invoice_items = [MagicMock(spec=InvoiceItem)]

    resp = bill_service_with_app_key.send_single_invoice(
        external_reference="INV123",
        billed_full_name="John Doe",
        billed_phone_number="0712345678",
        billed_period="June 2024",
        invoice_name="June Invoice",
        due_date="2024-06-30",
        account_reference="ACC123",
        amount=1000,
        invoice_items=invoice_items,
    )

    assert isinstance(resp, BillManagerSingleInvoiceResponse)
    assert resp.is_successful is True

def test_bill_manager_send_bulk_invoice(bill_service_with_app_key, mock_http_client):
    """Test send_bulk_invoice calls BillManager.send_bulk_invoice."""
    response_data = {
        "Status_Message": "Invoice sent successfully",
        "resmsg": "Success",
        "rescode": "200",
    }
    mock_http_client.post.return_value = response_data

    invoices = [MagicMock(spec=BillManagerSingleInvoiceRequest)]

    resp = bill_service_with_app_key.send_bulk_invoice(invoices=invoices)

    assert isinstance(resp, BillManagerBulkInvoiceResponse)
    assert resp.is_successful is True

def test_bill_manager_cancel_single_invoice(
    bill_service_with_app_key,
    mock_http_client,
):
    """Test cancel_single_invoice calls BillManager.cancel_single_invoice."""
    response_data = {
        "Status_Message": "Invoice cancelled successfully.",
        "resmsg": "Success",
        "rescode": "200",
        "errors": [],
    }

    mock_http_client.post.return_value = response_data
    resp = bill_service_with_app_key.cancel_single_invoice(external_reference="INV123")
    assert isinstance(resp, BillManagerCancelInvoiceResponse)
    assert resp.is_successful is True

def test_bill_manager_cancel_bulk_invoice(bill_service_with_app_key, mock_http_client):
    """Test cancel_bulk_invoice calls BillManager.cancel_bulk_invoice."""
    response_data = {
        "Status_Message": "Invoices cancelled successfully.",
        "resmsg": "Success",
        "rescode": "200",
        "errors": [],
    }
    mock_http_client.post.return_value = response_data
    resp = bill_service_with_app_key.cancel_bulk_invoice(
        external_references=["INV123", "INV456"]
    )

    assert isinstance(resp, BillManagerCancelInvoiceResponse)
    assert resp.is_successful is True

def test_bill_service_initializes_bill_manager_correctly(
    mock_http_client, mock_token_manager
):
    """Test BillService initializes BillManager with correct arguments."""
    service = BillService(
        http_client=mock_http_client,
        token_manager=mock_token_manager,
        app_key="test_app_key",
    )
    assert service.http_client is mock_http_client
    assert service.token_manager is mock_token_manager
    assert service.bill_manager.http_client is mock_http_client
    assert service.bill_manager.token_manager is mock_token_manager
    assert service.bill_manager.app_key == "test_app_key"


@pytest.mark.asyncio
async def test_async_opt_in_calls_bill_manager_opt_in(
    async_bill_service, mock_async_http_client
):
    """Test async opt_in calls AsyncBillManager.opt_in."""
    response_data = {
        "app_key": "AG_2376487236_126732989KJ",
        "resmsg": "Success",
        "rescode": "200",
    }
    mock_async_http_client.post.return_value = response_data

    resp = await async_bill_service.opt_in(
        shortcode=123456,
        email="test@example.com",
        official_contact="0712345678",
        send_reminders=1,
        logo=None,
        callback_url="https://callback.url",
    )

    assert isinstance(resp, BillManagerOptInResponse)
    assert resp.is_successful is True


@pytest.mark.asyncio
async def test_async_bill_manager_update_opt_in(
    async_bill_service_with_app_key, mock_async_http_client
):
    """Test async update_opt_in calls AsyncBillManager.update_opt_in."""
    response_data = {
        "resmsg": "Success",
        "rescode": "200",
    }
    mock_async_http_client.post.return_value = response_data

    resp = await async_bill_service_with_app_key.update_opt_in(
        shortcode=123456,
        email="update@example.com",
        official_contact="0712345678",
        send_reminders=0,
        logo="logo.png",
        callback_url="https://callback.url",
    )
    assert isinstance(resp, BillManagerUpdateOptInResponse)
    assert resp.is_successful is True


@pytest.mark.asyncio
async def test_async_bill_manager_send_single_invoice(
    async_bill_service_with_app_key,
    mock_async_http_client,
):
    """Test async send_single_invoice calls AsyncBillManager.send_single_invoice."""
    response_data = {
        "Status_Message": "Invoice sent successfully",
        "resmsg": "Success",
        "rescode": "200",
    }
    mock_async_http_client.post.return_value = response_data

    invoice_items = [MagicMock(spec=InvoiceItem)]

    resp = await async_bill_service_with_app_key.send_single_invoice(
        external_reference="INV123",
        billed_full_name="John Doe",
        billed_phone_number="0712345678",
        billed_period="June 2024",
        invoice_name="June Invoice",
        due_date="2024-06-30",
        account_reference="ACC123",
        amount=1000,
        invoice_items=invoice_items,
    )

    assert isinstance(resp, BillManagerSingleInvoiceResponse)
    assert resp.is_successful is True


@pytest.mark.asyncio
async def test_async_bill_manager_send_bulk_invoice(
    async_bill_service_with_app_key, mock_async_http_client
):
    """Test async send_bulk_invoice calls AsyncBillManager.send_bulk_invoice."""
    response_data = {
        "Status_Message": "Invoice sent successfully",
        "resmsg": "Success",
        "rescode": "200",
    }
    mock_async_http_client.post.return_value = response_data

    invoices = [MagicMock(spec=BillManagerSingleInvoiceRequest)]

    resp = await async_bill_service_with_app_key.send_bulk_invoice(invoices=invoices)

    assert isinstance(resp, BillManagerBulkInvoiceResponse)
    assert resp.is_successful is True


@pytest.mark.asyncio
async def test_async_bill_manager_cancel_single_invoice(
    async_bill_service_with_app_key,
    mock_async_http_client,
):
    """Test async cancel_single_invoice calls AsyncBillManager.cancel_single_invoice."""
    response_data = {
        "Status_Message": "Invoice cancelled successfully.",
        "resmsg": "Success",
        "rescode": "200",
        "errors": [],
    }

    mock_async_http_client.post.return_value = response_data
    resp = await async_bill_service_with_app_key.cancel_single_invoice(
        external_reference="INV123"
    )
    assert isinstance(resp, BillManagerCancelInvoiceResponse)
    assert resp.is_successful is True


@pytest.mark.asyncio
async def test_async_bill_manager_cancel_bulk_invoice(
    async_bill_service_with_app_key, mock_async_http_client
):
    """Test async cancel_bulk_invoice calls AsyncBillManager.cancel_bulk_invoice."""
    response_data = {
        "Status_Message": "Invoices cancelled successfully.",
        "resmsg": "Success",
        "rescode": "200",
        "errors": [],
    }
    mock_async_http_client.post.return_value = response_data
    resp = await async_bill_service_with_app_key.cancel_bulk_invoice(
        external_references=["INV123", "INV456"]
    )

    assert isinstance(resp, BillManagerCancelInvoiceResponse)
    assert resp.is_successful is True


def test_async_bill_service_initializes_bill_manager_correctly(
    mock_async_http_client, mock_async_token_manager
):
    """Test AsyncBillService initializes AsyncBillManager with correct arguments."""
    service = AsyncBillService(
        http_client=mock_async_http_client,
        token_manager=mock_async_token_manager,
        app_key="test_app_key",
    )
    assert service.http_client is mock_async_http_client
    assert service.token_manager is mock_async_token_manager
    assert service.bill_manager.http_client is mock_async_http_client
    assert service.bill_manager.token_manager is mock_async_token_manager
    assert service.bill_manager.app_key == "test_app_key"
