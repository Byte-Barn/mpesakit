"""Schemas for M-PESA B2B Express Checkout APIs."""

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, HttpUrl


class B2BExpressCheckoutRequest(BaseModel):
    """Request schema for B2B Express Checkout USSD Push."""

    primaryShortCode: int = Field(
        ..., description="Merchant's till (debit party) shortcode/tillNumber."
    )
    receiverShortCode: int = Field(
        ..., description="Vendor's paybill (credit party) shortcode."
    )
    amount: int = Field(..., description="Amount to be sent to vendor.")
    paymentRef: str = Field(
        ..., description="Reference for the payment (appears in text for merchant)."
    )
    callbackUrl: HttpUrl = Field(
        ..., description="Vendor system endpoint for confirmation response."
    )
    partnerName: str = Field(..., description="Vendor's organization friendly name.")
    RequestRefID: str = Field(..., description="Unique identifier for each request.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "primaryShortCode": 123456,
                "receiverShortCode": 654321,
                "amount": 100,
                "paymentRef": "Invoice123",
                "callbackUrl": "http://example.com/result",
                "partnerName": "VendorName",
                "RequestRefID": "550e8400-e29b-41d4-a716-446655440000",
            }
        }
    )


class B2BExpressCheckoutResponse(BaseModel):
    """Acknowledgment response schema for B2B Express Checkout USSD Push."""

    code: str = Field(
        ..., description="Shows if the push was successful (0) or failed."
    )
    status: str = Field(..., description="USSD initiation status message.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "code": "0",
                "status": "USSD Initiated Successfully",
            }
        }
    )

    def is_successful(self) -> bool:
        """Return True if code indicates success (e.g., '0', '00000000')."""
        code = str(self.code)
        return code.strip("0") == "" and code != ""


class B2BExpressCheckoutCallback(BaseModel):
    """Callback response schema for B2B Express Checkout USSD Push."""

    resultCode: str = Field(
        ..., description="Status code: 0=success, other=fail/cancelled."
    )
    resultDesc: str = Field(..., description="Description of transaction result.")
    amount: Optional[float] = Field(None, description="Amount initiated for payment.")
    requestId: str = Field(..., description="Unique identifier of the request.")
    paymentReference: Optional[str] = Field(
        None, description="Reference for the payment."
    )
    resultType: Optional[str] = Field(
        None, description="Status code for transaction sent to listener."
    )
    conversationID: Optional[str] = Field(
        None, description="Global unique transaction request ID from M-Pesa."
    )
    transactionId: Optional[str] = Field(
        None, description="Mpesa Receipt No of the transaction."
    )
    status: Optional[str] = Field(
        None, description="Transaction status (SUCCESS/FAILED)."
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "resultCode": "0",
                "resultDesc": "The service request is processed successfully.",
                "amount": "71.0",
                "requestId": "404e1aec-19e0-4ce3-973d-bd92e94c8021",
                "resultType": "0",
                "conversationID": "AG_20230426_2010434680d9f5a73766",
                "transactionId": "RDQ01NFT1Q",
                "status": "SUCCESS",
            }
        }
    )

    def is_successful(self) -> bool:
        """Return True if resultCode indicates success (e.g., '0', '00000000')."""
        code = str(self.resultCode)
        return code.strip("0") == "" and code != ""


class B2BExpressCallbackResponse(BaseModel):
    """Response schema for B2B Express Checkout callback."""

    ResultCode: int | str = Field(
        default=0,
        description="Result code (0=Success, other=Failure).",
    )
    ResultDesc: str = Field(
        default="Callback received successfully.",
        description="Result description.",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ResultCode": 0,
                "ResultDesc": "Callback received successfully.",
            }
        }
    )
