"""MCP server module providing Safaricom M-Pesa operations as LLM tools."""

import os
from typing import Optional

from fastmcp import FastMCP

from mpesakit import MpesaClient
from mpesakit.errors import MpesaApiException

# Initialize FastMCP with descriptive metadata
mcp = FastMCP(
    "mpesakit",
    version="1.0.0",
    description="Exposes Safaricom M-Pesa operations to LLM agents as standard tools.",
)


class McpConfig:
    """Centralized configuration loader for MCP server environment variables."""

    @staticmethod
    def _get_var(name: str) -> str:
        val = os.environ.get(name)
        if not val:
            raise ValueError(f"Missing required environment variable: {name}")
        return val

    @property
    def consumer_key(self) -> str:
        return self._get_var("MPESA_CONSUMER_KEY")

    @property
    def consumer_secret(self) -> str:
        return self._get_var("MPESA_CONSUMER_SECRET")

    @property
    def environment(self) -> str:
        return os.environ.get("MPESA_ENV", "sandbox")

    @property
    def business_short_code(self) -> int:
        return int(self._get_var("MPESA_BUSINESS_SHORTCODE"))

    @property
    def passkey(self) -> str:
        return self._get_var("MPESA_PASSKEY")

    @property
    def callback_url(self) -> str:
        return self._get_var("MPESA_CALLBACK_URL")
    
    @property
    def result_url(self) -> str:
        return os.environ.get("MPESA_RESULT_URL", self.callback_url)
        
    @property
    def timeout_url(self) -> str:
        return os.environ.get("MPESA_TIMEOUT_URL", self.callback_url)

    @property
    def initiator_name(self) -> str:
        return self._get_var("MPESA_INITIATOR_NAME")

    @property
    def security_credential(self) -> str:
        return self._get_var("MPESA_SECURITY_CREDENTIAL")


config = McpConfig()


def get_client() -> MpesaClient:
    """Safely retrieves a configured MpesaClient instance using centralized config."""
    try:
        key = config.consumer_key
        secret = config.consumer_secret
    except ValueError as e:
        raise ValueError(
            "Missing environment credentials. "
            "Please export MPESA_CONSUMER_KEY and MPESA_CONSUMER_SECRET."
        ) from e

    return MpesaClient(
        consumer_key=key,
        consumer_secret=secret,
        environment=config.environment,
    )


def enforce_security_guard(tool_name: str):
    """Prevents LLM agents from moving real production money by default."""
    env = config.environment.lower()
    allow_live = (
        os.environ.get("MPESA_MCP_ALLOW_LIVE_MUTATIONS", "false").lower() == "true"
    )

    if env == "production" and not allow_live:
        raise PermissionError(
            f"Security Exception: Tool '{tool_name}' cannot be run in production environments "
            "unless explicitly forced by setting MPESA_MCP_ALLOW_LIVE_MUTATIONS=true."
        )


def handle_api_exceptions(func):
    """Decorator to standardize error handling across tools."""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except MpesaApiException as e:
            return {"status": "failed", "error": str(e.error)}
        except Exception as e:
            return {"status": "failed", "error": str(e)}
    # Update __name__ to maintain original name for FastMCP to register properly
    wrapper.__name__ = func.__name__
    wrapper.__doc__ = func.__doc__
    # Note: FastMCP uses inspect.signature, so we must be careful with wrappers.
    # To be safe, we'll implement standard try-except in the tool functions directly.
    pass


@mcp.tool()
def stk_push(
    phone_number: str, amount: int, account_reference: str, transaction_desc: str
) -> dict:
    """Initiate an STK Push (Lipa na M-Pesa) payment prompt directly to a customer's phone screen.

    CRITICAL INSTRUCTION FOR LLM AGENTS: This operation is strictly asynchronous. Returning success
    means the prompt was sent to the phone, NOT that the user entered their PIN or paid. You must
    inform the user to check their device, wait 15-30 seconds, and then explicitly call the
    'check_transaction_status' tool using the returned checkout_request_id to verify the transaction.

    Args:
        phone_number: Target Safaricom phone number in format '2547XXXXXXXX' (no '+').
        amount: Absolute transaction value in KES (Kenya Shillings). Must be a positive integer.
        account_reference: Tracking reference visible to the user (e.g., order ID 'INV-2201'). Max 12 chars.
        transaction_desc: Short description of the payment context. Max 20 chars.
    """
    enforce_security_guard("stk_push")
    client = get_client()

    try:
        response = client.stk_push(
            business_short_code=config.business_short_code,
            transaction_type="CustomerPayBillOnline",
            amount=amount,
            party_a=phone_number,
            party_b=str(config.business_short_code),
            phone_number=phone_number,
            callback_url=config.callback_url,
            account_reference=account_reference,
            transaction_desc=transaction_desc,
            passkey=config.passkey,
        )
        return response.model_dump() if hasattr(response, "model_dump") else dict(response)
    except MpesaApiException as e:
        return {"status": "failed", "error": str(e.error)}
    except Exception as e:
        return {"status": "failed", "error": str(e)}


@mcp.tool()
def check_transaction_status(checkout_request_id: str) -> dict:
    """Query the final processing result of a previously initiated STK Push request.

    Use this to poll for success states when 'stk_push' returns a CheckoutRequestID.

    Args:
        checkout_request_id: The unique validation or transaction tracking string returned by the stk_push tool.
    """
    client = get_client()
    try:
        response = client.stk_query(
            business_short_code=config.business_short_code,
            passkey=config.passkey,
            checkout_request_id=checkout_request_id,
        )
        return response.model_dump() if hasattr(response, "model_dump") else dict(response)
    except MpesaApiException as e:
        return {"status": "failed", "error": str(e.error)}
    except Exception as e:
        return {"status": "failed", "error": str(e)}


@mcp.tool()
def c2b_register_url(response_type: str = "Completed") -> dict:
    """Register validation and confirmation URLs for C2B payments.

    Args:
        response_type: The response type ("Completed" or "Cancelled"). Defaults to "Completed".
    """
    enforce_security_guard("c2b_register_url")
    client = get_client()
    try:
        response = client.c2b.register_url(
            short_code=config.business_short_code,
            response_type=response_type,
            confirmation_url=config.callback_url,
            validation_url=config.callback_url,
        )
        return response.model_dump() if hasattr(response, "model_dump") else dict(response)
    except MpesaApiException as e:
        return {"status": "failed", "error": str(e.error)}
    except Exception as e:
        return {"status": "failed", "error": str(e)}


@mcp.tool()
def b2c_send_payment(
    phone_number: str, amount: int, command_id: str, remarks: str, occasion: str = ""
) -> dict:
    """Send money from the Business to a Customer (B2C).

    Args:
        phone_number: Target Safaricom phone number in format '2547XXXXXXXX' (no '+').
        amount: Absolute transaction value in KES. Must be a positive integer.
        command_id: The B2C command ID (e.g. 'SalaryPayment', 'BusinessPayment', 'PromotionPayment').
        remarks: Remarks about the transaction.
        occasion: Optional occasion string.
    """
    enforce_security_guard("b2c_send_payment")
    client = get_client()
    try:
        response = client.b2c.send_payment(
            originator_conversation_id="", # often auto-generated or optional depending on the SDK implementation, using empty or a valid UUID
            initiator_name=config.initiator_name,
            security_credential=config.security_credential,
            command_id=command_id,
            amount=amount,
            party_a=str(config.business_short_code),
            party_b=phone_number,
            remarks=remarks,
            queue_timeout_url=config.timeout_url,
            result_url=config.result_url,
            occasion=occasion,
        )
        return response.model_dump() if hasattr(response, "model_dump") else dict(response)
    except MpesaApiException as e:
        return {"status": "failed", "error": str(e.error)}
    except Exception as e:
        return {"status": "failed", "error": str(e)}


@mcp.tool()
def b2c_account_topup(
    phone_number: str, amount: int, account_reference: str, requester: str, remarks: str
) -> dict:
    """Top up a customer account via B2C.

    Args:
        phone_number: Target phone number.
        amount: Transaction value in KES.
        account_reference: Reference for the account.
        requester: The entity requesting the top up.
        remarks: Transaction remarks.
    """
    enforce_security_guard("b2c_account_topup")
    client = get_client()
    try:
        response = client.b2c.account_topup(
            initiator=config.initiator_name,
            security_credential=config.security_credential,
            amount=amount,
            party_a=str(config.business_short_code),
            party_b=phone_number,
            account_reference=account_reference,
            requester=requester,
            remarks=remarks,
            queue_timeout_url=config.timeout_url,
            result_url=config.result_url,
        )
        return response.model_dump() if hasattr(response, "model_dump") else dict(response)
    except MpesaApiException as e:
        return {"status": "failed", "error": str(e.error)}
    except Exception as e:
        return {"status": "failed", "error": str(e)}


@mcp.tool()
def b2b_paybill(
    receiver_short_code: int, amount: int, account_reference: str, requester: str, remarks: str
) -> dict:
    """Send money from one Business to another via PayBill (B2B).

    Args:
        receiver_short_code: The recipient's PayBill shortcode.
        amount: Transaction value in KES.
        account_reference: Target account reference.
        requester: The requester of the transaction.
        remarks: Comments about the transaction.
    """
    enforce_security_guard("b2b_paybill")
    client = get_client()
    try:
        response = client.b2b.paybill(
            initiator=config.initiator_name,
            security_credential=config.security_credential,
            amount=amount,
            party_a=config.business_short_code,
            party_b=receiver_short_code,
            account_reference=account_reference,
            requester=requester,
            remarks=remarks,
            queue_timeout_url=config.timeout_url,
            result_url=config.result_url,
        )
        return response.model_dump() if hasattr(response, "model_dump") else dict(response)
    except MpesaApiException as e:
        return {"status": "failed", "error": str(e.error)}
    except Exception as e:
        return {"status": "failed", "error": str(e)}


@mcp.tool()
def b2b_buygoods(
    receiver_short_code: int, amount: int, account_reference: str, requester: str, remarks: str, occasion: Optional[str] = None
) -> dict:
    """Send money from one Business to another via Buy Goods (B2B).

    Args:
        receiver_short_code: The recipient's Till Number.
        amount: Transaction value in KES.
        account_reference: Target account reference.
        requester: The requester of the transaction.
        remarks: Comments about the transaction.
        occasion: Optional occasion string.
    """
    enforce_security_guard("b2b_buygoods")
    client = get_client()
    try:
        response = client.b2b.buygoods(
            initiator=config.initiator_name,
            security_credential=config.security_credential,
            amount=amount,
            party_a=config.business_short_code,
            party_b=receiver_short_code,
            account_reference=account_reference,
            requester=requester,
            remarks=remarks,
            queue_timeout_url=config.timeout_url,
            result_url=config.result_url,
            occassion=occasion,
        )
        return response.model_dump() if hasattr(response, "model_dump") else dict(response)
    except MpesaApiException as e:
        return {"status": "failed", "error": str(e.error)}
    except Exception as e:
        return {"status": "failed", "error": str(e)}


@mcp.tool()
def b2b_express_checkout(
    receiver_short_code: str, amount: int, payment_ref: str, partner_name: str, request_ref_id: str
) -> dict:
    """Initiate a B2B Express Checkout payment.

    Args:
        receiver_short_code: The recipient's shortcode.
        amount: Transaction value in KES.
        payment_ref: Payment reference string.
        partner_name: The name of the partner business.
        request_ref_id: Unique request reference ID.
    """
    enforce_security_guard("b2b_express_checkout")
    client = get_client()
    try:
        response = client.b2b.express_checkout(
            primary_short_code=str(config.business_short_code),
            receiver_short_code=receiver_short_code,
            amount=amount,
            payment_ref=payment_ref,
            callback_url=config.callback_url,
            partner_name=partner_name,
            request_ref_id=request_ref_id,
        )
        return response.model_dump() if hasattr(response, "model_dump") else dict(response)
    except MpesaApiException as e:
        return {"status": "failed", "error": str(e.error)}
    except Exception as e:
        return {"status": "failed", "error": str(e)}


@mcp.tool()
def check_account_balance(
    remarks: str, identifier_type: int = 4, command_id: str = "AccountBalance"
) -> dict:
    """Query the account balance of the configured business shortcode.

    Args:
        remarks: Comments about the balance query.
        identifier_type: The organization identifier type (default is 4 for shortcode).
        command_id: The balance command ID (default is 'AccountBalance').
    """
    client = get_client()
    try:
        response = client.balance.query(
            initiator=config.initiator_name,
            security_credential=config.security_credential,
            command_id=command_id,
            party_a=config.business_short_code,
            identifier_type=identifier_type,
            remarks=remarks,
            result_url=config.result_url,
            queue_timeout_url=config.timeout_url,
        )
        return response.model_dump() if hasattr(response, "model_dump") else dict(response)
    except MpesaApiException as e:
        return {"status": "failed", "error": str(e.error)}
    except Exception as e:
        return {"status": "failed", "error": str(e)}


@mcp.tool()
def reverse_transaction(
    transaction_id: str, amount: int, remarks: str, occasion: Optional[str] = None
) -> dict:
    """Reverse a previous successful M-Pesa transaction.

    Args:
        transaction_id: The M-Pesa transaction ID to reverse (e.g., 'OEI2AK4Q16').
        amount: The exact amount to reverse.
        remarks: Comments about the reversal reason.
        occasion: Optional occasion string.
    """
    enforce_security_guard("reverse_transaction")
    client = get_client()
    try:
        response = client.reversal.reverse(
            initiator=config.initiator_name,
            security_credential=config.security_credential,
            transaction_id=transaction_id,
            amount=amount,
            receiver_party=config.business_short_code,
            result_url=config.result_url,
            queue_timeout_url=config.timeout_url,
            remarks=remarks,
            occasion=occasion,
        )
        return response.model_dump() if hasattr(response, "model_dump") else dict(response)
    except MpesaApiException as e:
        return {"status": "failed", "error": str(e.error)}
    except Exception as e:
        return {"status": "failed", "error": str(e)}


@mcp.tool()
def remit_tax(
    amount: int, remarks: str, account_reference: str
) -> dict:
    """Remit tax directly to the Kenya Revenue Authority (KRA).

    Args:
        amount: Transaction value in KES.
        remarks: Comments about the tax remittance.
        account_reference: Tax reference or PRN.
    """
    enforce_security_guard("remit_tax")
    client = get_client()
    try:
        response = client.tax.remittance(
            initiator=config.initiator_name,
            security_credential=config.security_credential,
            amount=amount,
            party_a=config.business_short_code,
            remarks=remarks,
            account_reference=account_reference,
            result_url=config.result_url,
            queue_timeout_url=config.timeout_url,
        )
        return response.model_dump() if hasattr(response, "model_dump") else dict(response)
    except MpesaApiException as e:
        return {"status": "failed", "error": str(e.error)}
    except Exception as e:
        return {"status": "failed", "error": str(e)}


@mcp.tool()
def generate_dynamic_qr(
    merchant_name: str, ref_no: str, amount: int, trx_code: str, cpi: str, size: str = "300"
) -> dict:
    """Generate a Dynamic QR code for payments.

    Args:
        merchant_name: The name of the merchant.
        ref_no: Reference number for the QR.
        amount: The payment amount.
        trx_code: Transaction code ('BG' for Buy Goods, 'PB' for Paybill, 'WA' for Withdraw, 'SM' for Send Money).
        cpi: The shortcode or till number.
        size: Size of the generated QR code (default '300').
    """
    client = get_client()
    try:
        response = client.dynamic_qr.generate(
            merchant_name=merchant_name,
            ref_no=ref_no,
            amount=amount,
            trx_code=trx_code,
            cpi=cpi,
            size=size,
        )
        return response.model_dump() if hasattr(response, "model_dump") else dict(response)
    except MpesaApiException as e:
        return {"status": "failed", "error": str(e.error)}
    except Exception as e:
        return {"status": "failed", "error": str(e)}


if __name__ == "__main__":
    mcp.run()
