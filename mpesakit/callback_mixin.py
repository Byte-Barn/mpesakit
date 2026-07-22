"""Shared callback processing helpers for M-PESA clients."""


class MpesaCallbackMixin:
    """Mixin providing callback payload validation helpers.

    Used by both :class:`~mpesakit.mpesa_client.MpesaClient` and
    :class:`~mpesakit.mpesa_client.AsyncMpesaClient`.
    """

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
