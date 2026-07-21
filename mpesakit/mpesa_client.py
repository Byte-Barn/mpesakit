"""MpesaClient: A unified client for M-PESA services."""

from mpesakit.auth import TokenManager, AsyncTokenManager
from mpesakit.http_client import MpesaHttpClient, MpesaAsyncHttpClient
from mpesakit.services import (
    B2BService,
    AsyncB2BService,
    B2CService,
    AsyncB2CService,
    BalanceService,
    AsyncBalanceService,
    BillService,
    AsyncBillService,
    C2BService,
    AsyncC2BService,
    DynamicQRCodeService,
    AsyncDynamicQRCodeService,
    StkPushService,
    AsyncStkPushService,
    RatibaService,
    AsyncRatibaService,
    ReversalService,
    AsyncReversalService,
    TaxService,
    AsyncTaxService,
    TransactionService,
    AsyncTransactionService,
)


class MpesaClient:
    """Unified client for all M-PESA services."""

    def __init__(
        self,
        consumer_key: str,
        consumer_secret: str,
        environment: str = "sandbox",
        use_session: bool = False,
    ) -> None:
        """Initialize the MpesaClient with all service facades."""
        self.http_client = MpesaHttpClient(env=environment, use_session=use_session)
        self.token_manager = TokenManager(
            http_client=self.http_client,
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
        )

        # express => M-PESA STK Push
        self.express = StkPushService(
            http_client=self.http_client, token_manager=self.token_manager
        )
        self.stk_push = self.express.push  # Alias for convenience
        self.stk_query = self.express.query  # Alias for convenience

        # b2c => M-PESA Business to Customer services
        self.b2c = B2CService(
            http_client=self.http_client, token_manager=self.token_manager
        )

        # b2b => M-PESA Business to Business services
        self.b2b = B2BService(
            http_client=self.http_client, token_manager=self.token_manager
        )

        # transaction => M-PESA Transaction status services
        self.transactions = TransactionService(
            http_client=self.http_client, token_manager=self.token_manager
        )

        # tax => M-PESA Tax services
        self.tax = TaxService(
            http_client=self.http_client, token_manager=self.token_manager
        )

        # balance => M-PESA Account balance services
        self.balance = BalanceService(
            http_client=self.http_client, token_manager=self.token_manager
        )

        # reversal => M-PESA Transaction reversal services
        self.reversal = ReversalService(
            http_client=self.http_client, token_manager=self.token_manager
        )

        # bill => M-PESA Bill services
        self.bill = BillService(
            http_client=self.http_client, token_manager=self.token_manager
        )

        # dynamic_qr => M-PESA Dynamic QR services
        self.dynamic_qr = DynamicQRCodeService(
            http_client=self.http_client, token_manager=self.token_manager
        )

        # c2b => M-PESA Customer to Business services
        self.c2b = C2BService(
            http_client=self.http_client, token_manager=self.token_manager
        )

        # ratiba => M-PESA Ratiba services
        self.ratiba = RatibaService(
            http_client=self.http_client, token_manager=self.token_manager
        )

    def process_stk_callback(self, payload):
        """Process STK Push callback payload.

        Validates and parses STK Push simulation callback data.
        """
        from mpesakit.mpesa_express.schemas import StkPushSimulateCallback

        return StkPushSimulateCallback.model_validate(payload)

    def process_stk_query_callback(self, payload):
        """Process STK Push query response payload.

        Validates and parses STK Push query response data.
        """
        from mpesakit.mpesa_express.schemas import StkPushQueryResponse

        return StkPushQueryResponse.model_validate(payload)

    def process_account_balance_callback(self, payload):
        """Process account balance callback payload.

        Validates and parses account balance query result callback.
        """
        from mpesakit.account_balance.schemas import AccountBalanceResultCallback

        return AccountBalanceResultCallback.model_validate(payload)

    def process_account_balance_timeout(self, payload):
        """Process account balance timeout callback payload.

        Validates and parses account balance query timeout notification.
        """
        from mpesakit.account_balance.schemas import AccountBalanceTimeoutCallback

        return AccountBalanceTimeoutCallback.model_validate(payload)

    def process_b2c_callback(self, payload):
        """Process B2C (Business-to-Customer) callback payload.

        Validates and parses B2C payment result callback.
        """
        from mpesakit.b2c.schemas import B2CResultCallback

        return B2CResultCallback.model_validate(payload)

    def process_b2b_callback(self, payload):
        """Process B2B Express Checkout callback payload.

        Validates and parses B2B Express Checkout response data.
        """
        from mpesakit.b2b_express_checkout.schemas import B2BExpressCheckoutCallback

        return B2BExpressCheckoutCallback.model_validate(payload)

    def process_transactions_callback(self, payload):
        """Process transaction status callback payload.

        Validates and parses transaction status query result callback.
        """
        from mpesakit.transaction_status.schemas import TransactionStatusResultCallback

        return TransactionStatusResultCallback.model_validate(payload)

    def process_bill_manager_callback(self, payload):
        """Process bill manager callback payload.

        Validates and parses bill manager payment notification.
        """
        from mpesakit.bill_manager.schemas import BillManagerPaymentNotificationRequest

        return BillManagerPaymentNotificationRequest.model_validate(payload)

    def process_tax_remittance_callback(self, payload):
        """Process tax remittance callback payload.

        Validates and parses tax remittance result callback.
        """
        from mpesakit.tax_remittance.schemas import TaxRemittanceResultCallback

        return TaxRemittanceResultCallback.model_validate(payload)

    def process_dynamic_qr_code_callback(self, payload):
        """Process dynamic QR code callback payload.

        Validates and parses dynamic QR code generation response.
        """
        from mpesakit.dynamic_qr_code.schemas import DynamicQRGenerateResponse

        return DynamicQRGenerateResponse.model_validate(payload)

    def process_ratiba_service_callback(self, payload):
        """Process Ratiba (Standing Order) callback payload.

        Validates and parses M-PESA Ratiba services callback.
        """
        from mpesakit.mpesa_ratiba.schemas import StandingOrderCallback

        return StandingOrderCallback.model_validate(payload)

    def process_reversal_callback(self, payload):
        """Process transaction reversal callback payload.

        Validates and parses transaction reversal result callback.
        """
        from mpesakit.reversal.schemas import ReversalResultCallback

        return ReversalResultCallback.model_validate(payload)


class AsyncMpesaClient:
    """Unified async client for all M-PESA services."""

    def __init__(
        self,
        consumer_key: str,
        consumer_secret: str,
        environment: str = "sandbox",
    ) -> None:
        """Initialize the AsyncMpesaClient with all service facades."""
        self.http_client = MpesaAsyncHttpClient(env=environment)
        self.token_manager = AsyncTokenManager(
            http_client=self.http_client,
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
        )

        # express => M-PESA STK Push
        self.express = AsyncStkPushService(
            http_client=self.http_client, token_manager=self.token_manager
        )
        self.stk_push = self.express.push  # Alias for convenience
        self.stk_query = self.express.query  # Alias for convenience

        # b2c => M-PESA Business to Customer services
        self.b2c = AsyncB2CService(
            http_client=self.http_client, token_manager=self.token_manager
        )

        # b2b => M-PESA Business to Business services
        self.b2b = AsyncB2BService(
            http_client=self.http_client, token_manager=self.token_manager
        )

        # transaction => M-PESA Transaction status services
        self.transactions = AsyncTransactionService(
            http_client=self.http_client, token_manager=self.token_manager
        )

        # tax => M-PESA Tax services
        self.tax = AsyncTaxService(
            http_client=self.http_client, token_manager=self.token_manager
        )

        # balance => M-PESA Account balance services
        self.balance = AsyncBalanceService(
            http_client=self.http_client, token_manager=self.token_manager
        )

        # reversal => M-PESA Transaction reversal services
        self.reversal = AsyncReversalService(
            http_client=self.http_client, token_manager=self.token_manager
        )

        # bill => M-PESA Bill services
        self.bill = AsyncBillService(
            http_client=self.http_client, token_manager=self.token_manager
        )

        # dynamic_qr => M-PESA Dynamic QR services
        self.dynamic_qr = AsyncDynamicQRCodeService(
            http_client=self.http_client, token_manager=self.token_manager
        )

        # c2b => M-PESA Customer to Business services
        self.c2b = AsyncC2BService(
            http_client=self.http_client, token_manager=self.token_manager
        )

        # ratiba => M-PESA Ratiba services
        self.ratiba = AsyncRatibaService(
            http_client=self.http_client, token_manager=self.token_manager
        )

    def process_stk_callback(self, payload):
        """Process STK Push callback payload.

        Validates and parses STK Push simulation callback data.
        """
        from mpesakit.mpesa_express.schemas import StkPushSimulateCallback

        return StkPushSimulateCallback.model_validate(payload)

    def process_stk_query_callback(self, payload):
        """Process STK Push query response payload.

        Validates and parses STK Push query response data.
        """
        from mpesakit.mpesa_express.schemas import StkPushQueryResponse

        return StkPushQueryResponse.model_validate(payload)

    def process_account_balance_callback(self, payload):
        """Process account balance callback payload.

        Validates and parses account balance query result callback.
        """
        from mpesakit.account_balance.schemas import AccountBalanceResultCallback

        return AccountBalanceResultCallback.model_validate(payload)

    def process_account_balance_timeout(self, payload):
        """Process account balance timeout callback payload.

        Validates and parses account balance query timeout notification.
        """
        from mpesakit.account_balance.schemas import AccountBalanceTimeoutCallback

        return AccountBalanceTimeoutCallback.model_validate(payload)

    def process_b2c_callback(self, payload):
        """Process B2C (Business-to-Customer) callback payload.

        Validates and parses B2C payment result callback.
        """
        from mpesakit.b2c.schemas import B2CResultCallback

        return B2CResultCallback.model_validate(payload)

    def process_b2b_callback(self, payload):
        """Process B2B Express Checkout callback payload.

        Validates and parses B2B Express Checkout response data.
        """
        from mpesakit.b2b_express_checkout.schemas import B2BExpressCheckoutCallback

        return B2BExpressCheckoutCallback.model_validate(payload)

    def process_transactions_callback(self, payload):
        """Process transaction status callback payload.

        Validates and parses transaction status query result callback.
        """
        from mpesakit.transaction_status.schemas import TransactionStatusResultCallback

        return TransactionStatusResultCallback.model_validate(payload)

    def process_bill_manager_callback(self, payload):
        """Process bill manager callback payload.

        Validates and parses bill manager payment notification.
        """
        from mpesakit.bill_manager.schemas import BillManagerPaymentNotificationRequest

        return BillManagerPaymentNotificationRequest.model_validate(payload)

    def process_tax_remittance_callback(self, payload):
        """Process tax remittance callback payload.

        Validates and parses tax remittance result callback.
        """
        from mpesakit.tax_remittance.schemas import TaxRemittanceResultCallback

        return TaxRemittanceResultCallback.model_validate(payload)

    def process_dynamic_qr_code_callback(self, payload):
        """Process dynamic QR code callback payload.

        Validates and parses dynamic QR code generation response.
        """
        from mpesakit.dynamic_qr_code.schemas import DynamicQRGenerateResponse

        return DynamicQRGenerateResponse.model_validate(payload)

    def process_ratiba_service_callback(self, payload):
        """Process Ratiba (Standing Order) callback payload.

        Validates and parses M-PESA Ratiba services callback.
        """
        from mpesakit.mpesa_ratiba.schemas import StandingOrderCallback

        return StandingOrderCallback.model_validate(payload)

    def process_reversal_callback(self, payload):
        """Process transaction reversal callback payload.

        Validates and parses transaction reversal result callback.
        """
        from mpesakit.reversal.schemas import ReversalResultCallback

        return ReversalResultCallback.model_validate(payload)
