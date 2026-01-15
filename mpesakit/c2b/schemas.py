"""This module defines schemas for M-Pesa C2B API requests, responses, and callbacks.

It includes models for URL registration, payment notifications, validation, and confirmation.
"""

from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict, model_validator
import warnings


class C2BResponseType(str, Enum):
    """Allowed values for ResponseType in C2B URL registration."""

    COMPLETED = "Completed"
    CANCELLED = "Cancelled"


class C2BValidationResultCodeType(str, Enum):
    """Allowed result error codes for C2B validation response."""

    ACCEPTED = "0"
    INVALID_MSISDN = "C2B00011"
    INVALID_ACCOUNT_NUMBER = "C2B00012"
    INVALID_AMOUNT = "C2B00013"
    INVALID_KYC_DETAILS = "C2B00014"
    INVALID_SHORTCODE = "C2B00015"
    OTHER_ERROR = "C2B00016"


class C2BRegisterUrlRequest(BaseModel):
    """Request schema for registering C2B validation and confirmation URLs.

    https://developer.safaricom.co.ke/APIs/CustomerToBusinessRegisterURL
    """

    ShortCode: int = Field(..., description="Organization's Paybill/Till number.")
    ResponseType: str = Field(
        ...,
        description="Default action if ValidationURL is unreachable: Completed or Cancelled.",
    )
    ConfirmationURL: str = Field(
        ..., description="URL to receive payment confirmation notifications."
    )
    ValidationURL: str = Field(
        ..., description="URL to receive payment validation requests."
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ShortCode": 601426,
                "ResponseType": "Completed",
                "ConfirmationURL": "https://example.com/confirmation",
                "ValidationURL": "https://example.com/validation",
            }
        }
    )

    @model_validator(mode="before")
    @classmethod
    def validate(cls, values):
        """Validate the request fields before processing."""
        cls._validate_response_type(values)
        cls._warn_invalid_urls(values)
        return values

    @classmethod
    def _validate_response_type(cls, values):
        """Ensure ResponseType is a valid C2BResponseType value."""
        response_type = values.get("ResponseType")
        valid_types = [e.value for e in C2BResponseType]
        if response_type not in valid_types:
            raise ValueError(
                f"ResponseType must be one of {valid_types}, got '{response_type}'"
            )
        return values

    @classmethod
    def _warn_invalid_urls(cls, values):
        """Warn if ConfirmationURL or ValidationURL contains forbidden keywords."""
        forbidden_keywords = [
            "m-pesa",
            "mpesa",
            "safaricom",
            "exe",
            "exec",
            "cmd",
            "sql",
            "query",
        ]
        for field in ["ConfirmationURL", "ValidationURL"]:
            url = values.get(field, "")
            url_lower = url.lower()
            for keyword in forbidden_keywords:
                if keyword in url_lower:
                    warnings.warn(
                        f"{field} contains forbidden keyword '{keyword}'. "
                        "This URL is likely to result in a 400 Bad Request when validating with Daraja API.",
                        UserWarning,
                    )
        return values


class C2BRegisterUrlResponse(BaseModel):
    """Response schema for C2B URL registration."""

    OriginatorConversationID: Optional[str] = Field(
        ..., description="Unique ID for the registration request."
    )
    ResponseCode: str | int = Field(..., description="Status code, 0 means success.")
    ResponseDescription: str = Field(..., description="Status message.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "OriginatorCoversationID": "7619-37765134-1",
                "ResponseCode": "0",
                "ResponseDescription": "success",
            }
        }
    )

    def is_successful(self) -> bool:
        """Return True if ResponseCode indicates success (e.g., '0', '00000000')."""
        code = str(self.ResponseCode)
        # Remove zeros and check if the result is empty (i.e., all zeros)
        return code.strip("0") == "" and code != ""


class C2BValidationRequest(BaseModel):
    """Schema for payment details posted to Validation URL."""

    TransactionType: str = Field(
        ..., description="Transaction type: Pay Bill or Buy Goods."
    )
    TransID: str = Field(..., description="Unique M-Pesa transaction ID.")
    TransTime: str = Field(..., description="Timestamp in YYYYMMDDHHmmss format.")
    TransAmount: float = Field(
        ..., description="Amount transacted (whole numbers only)."
    )
    BusinessShortCode: int = Field(
        ..., description="Organization's shortcode (Paybill/Till)."
    )
    BillRefNumber: Optional[str] = Field(
        None, description="Account number for PayBill transactions (max 20 chars)."
    )
    InvoiceNumber: Optional[str] = Field(None, description="Invoice number (optional).")
    OrgAccountBalance: Optional[str] = Field(
        None, description="Organization account balance after payment."
    )
    ThirdPartyTransID: Optional[str] = Field(
        None, description="Partner transaction ID (optional)."
    )
    MSISDN: int | str = Field(..., description="Customer mobile number.")
    FirstName: Optional[str] = Field(None, description="Customer's first name.")
    MiddleName: Optional[str] = Field(None, description="Customer's middle name.")
    LastName: Optional[str] = Field(None, description="Customer's last name.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "TransactionType": "Pay Bill",
                "TransID": "RKTQDM7W6S",
                "TransTime": "20191122063845",
                "TransAmount": "10",
                "BusinessShortCode": 600638,
                "BillRefNumber": "invoice008",
                "InvoiceNumber": "",
                "OrgAccountBalance": "",
                "ThirdPartyTransID": "",
                "MSISDN": 254701234567,
                "FirstName": "John",
                "MiddleName": "",
                "LastName": "Doe",
            }
        }
    )


class C2BValidationResponse(BaseModel):
    """Schema for response to validation requests (accept/reject payment)."""

    ResultCode: int | str = Field(..., description="0 to accept, error code to reject.")
    ResultDesc: str = Field(..., description="Short description: Accepted or Rejected.")
    ThirdPartyTransID: Optional[str] = Field(
        None, description="Optional partner transaction ID."
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ResultCode": "0",
                "ResultDesc": "Accepted",
                "ThirdPartyTransID": "1234567890",
            }
        }
    )

    @model_validator(mode="before")
    @classmethod
    def validate(cls, values):
        """Unified validator for ResultCode and ResultDesc."""
        cls._validate_result_code(values)
        cls._warn_long_resultdesc(values)
        return values

    @classmethod
    def _validate_result_code(cls, values):
        """Validates ResultCode against C2BValidationResultCodeType enum."""
        code = values.get("ResultCode")
        valid_codes = [e.value for e in C2BValidationResultCodeType]
        if code is not None and code not in valid_codes:
            raise ValueError(f"ResultCode must be one of {valid_codes}, got '{code}'")
        return values

    @classmethod
    def _warn_long_resultdesc(cls, values):
        """Warn if ResultDesc exceeds 90 characters."""
        desc = values.get("ResultDesc")
        if desc and len(desc) > 90:
            warnings.warn("ResultDesc exceeds 90 characters.", UserWarning)
        return values


class C2BConfirmationResponse(BaseModel):
    """Schema for confirmation acknowledgment from your ConfirmationURL."""

    ResultCode: int | str = Field(0, description="Always 0 (success).")
    ResultDesc: str = Field("Success", description="Usually 'Success'.")

    model_config = ConfigDict(
        json_schema_extra={"example": {"ResultCode": 0, "ResultDesc": "Success"}}
    )
