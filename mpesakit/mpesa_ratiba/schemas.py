"""Schemas for M-PESA Ratiba (Standing Order) APIs."""

from pydantic import BaseModel, Field, HttpUrl, ConfigDict, model_validator
from typing import Optional, List, Any
from datetime import datetime
from enum import Enum

from mpesakit.utils.phone import normalize_phone_number


class FrequencyEnum(str, Enum):
    """Enumeration for transaction frequency in Standing Order API."""

    ONE_OFF = "1"
    DAILY = "2"
    WEEKLY = "3"
    MONTHLY = "4"
    BI_MONTHLY = "5"
    QUARTERLY = "6"
    HALF_YEAR = "7"
    YEARLY = "8"


class TransactionTypeEnum(str, Enum):
    """Enumeration for transaction types in Standing Order API."""

    STANDING_ORDER_CUSTOMER_PAY_BILL = "Standing Order Customer Pay Bill"
    STANDING_ORDER_CUSTOMER_PAY_MERCHANT = "Standing Order Customer Pay Merchant"


class ReceiverPartyIdentifierTypeEnum(str, Enum):
    """Enumeration for receiver party identifier type in Standing Order API."""

    MERCHANT_TILL = "2"  # Till Number
    BUSINESS_SHORT_CODE = "4"  # PayBill


class StandingOrderRequest(BaseModel):
    """Request schema for creating a Standing Order."""

    StandingOrderName: str = Field(
        ..., description="Unique name for the standing order per customer."
    )
    StartDate: str = Field(
        ..., description="Start date for the standing order (yyyymmdd)."
    )
    EndDate: str = Field(..., description="End date for the standing order (yyyymmdd).")
    BusinessShortCode: str = Field(
        ..., description="Business short code to receive payment."
    )
    TransactionType: TransactionTypeEnum = Field(
        ...,
        description="Transaction type: 'Standing Order Customer Pay Bill' or 'Standing Order Customer Pay Merchant'.",
    )
    ReceiverPartyIdentifierType: ReceiverPartyIdentifierTypeEnum = Field(
        ...,
        description="Receiver party identifier type: '2' for Till, '4' for PayBill.",
    )
    Amount: str = Field(
        ..., description="Amount to be transacted (whole number as string)."
    )
    PartyA: str = Field(
        ...,
        description="Customer's M-PESA registered phone number (format: 2547XXXXXXXX).",
    )
    CallBackURL: HttpUrl = Field(
        ..., description="URL to receive notifications from Standing Order Solution."
    )
    AccountReference: str = Field(
        ...,
        description="Account reference for PayBill transactions (max 12 chars).",
        max_length=12,
    )
    TransactionDesc: str = Field(
        ..., description="Additional info/comment (max 13 chars).", max_length=13
    )
    Frequency: FrequencyEnum = Field(
        ..., description="Frequency of transactions: 1=One Off, 2=Daily, ..., 8=Yearly."
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "StandingOrderName": "Test Standing Order",
                "StartDate": "20240905",
                "EndDate": "20250905",
                "BusinessShortCode": "174379",
                "TransactionType": "Standing Order Customer Pay Bill",
                "ReceiverPartyIdentifierType": "4",
                "Amount": "4500",
                "PartyA": "254708374149",
                "CallBackURL": "https://mydomain.com/pat",
                "AccountReference": "Test",
                "TransactionDesc": "Electric Bike Repayment",
                "Frequency": "2",
            }
        }
    )

    @model_validator(mode="before")
    @classmethod
    def validate(cls, values):
        """Validate the request data before processing."""
        cls._validate_and_format_date(values)
        cls._validate_phone_number(values)
        return values

    @classmethod
    def _validate_phone_number(cls, values):
        """Ensure PartyA is a valid M-PESA phone number."""
        phone = values.get("PartyA")
        normalized_phone = normalize_phone_number(phone)
        if not normalized_phone:
            raise ValueError(f"Invalid PartyA phone number: {phone}")
        values["PartyA"] = normalized_phone

    @classmethod
    def _validate_and_format_date(cls, values):
        """Ensure StartDate and EndDate are in the correct format."""
        for field in ["StartDate", "EndDate"]:
            date_str = values.get(field)
            if date_str:
                formatted_date = cls.format_date(date_str)
                values[field] = formatted_date

    @classmethod
    def format_date(cls, date_str: str) -> str:
        """Format date string to 'yyyymmdd' and validate it."""
        # Normalize date separators to empty string
        normalized_date_str = "".join(filter(str.isdigit, date_str))
        if len(normalized_date_str) != 8:
            raise ValueError("Date must be in 'yyyymmdd' format and a valid date.")
        try:
            dt = datetime.strptime(normalized_date_str, "%Y%m%d")
            return dt.strftime("%Y%m%d")
        except ValueError:
            raise ValueError("Date must be in 'yyyymmdd' format and a valid date.")


class StandingOrderResponseHeader(BaseModel):
    """Response header metadata for Standing Order API."""

    responseRefID: str = Field(..., description="Unique reference ID for the response.")
    requestRefID: Optional[str] = Field(
        None, description="Unique reference ID for the request (callback only)."
    )
    responseCode: str = Field(
        ..., description="HTTP response code: '200', '401', '500', etc."
    )
    responseDescription: str = Field(
        ..., description="Description of the response status."
    )
    ResultDesc: Optional[str] = Field(
        None, description="Additional result description (optional)."
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "responseRefID": "4dd9b5d9-d738-42ba-9326-2cc99e966000",
                "requestRefID": "c8c2bb31-3b3a-402e-84fc-21ef35161e48",
                "responseCode": "200",
                "responseDescription": "Request accepted for processing",
                "ResultDesc": "The service request is processed successfully.",
            }
        }
    )


class StandingOrderResponseBody(BaseModel):
    """Response body metadata for Standing Order API."""

    responseDescription: Optional[str] = Field(
        None, description="Descriptive message for the async request."
    )
    responseCode: Optional[str] = Field(
        None, description="HTTP response code: '200', '401', '500', etc."
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "responseDescription": "Request accepted for processing",
                "responseCode": "200",
            }
        }
    )


class StandingOrderResponse(BaseModel):
    """Immediate response schema for Standing Order request."""

    ResponseHeader: StandingOrderResponseHeader = Field(
        ..., description="Response header metadata."
    )
    ResponseBody: StandingOrderResponseBody = Field(
        ..., description="Response body metadata."
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ResponseHeader": {
                    "responseRefID": "4dd9b5d9-d738-42ba-9326-2cc99e966000",
                    "responseCode": "200",
                    "responseDescription": "Request accepted for processing",
                    "ResultDesc": "The service request is processed successfully.",
                },
                "ResponseBody": {
                    "responseDescription": "Request accepted for processing",
                    "responseCode": "200",
                },
            }
        }
    )

    @property
    def is_successful(self) -> bool:
        """Check if the response indicates a successful transaction."""
        return self.ResponseHeader.responseCode == "200"


class StandingOrderCallbackDataItem(BaseModel):
    """Key-value pair for callback response data."""

    Name: str = Field(
        ...,
        description="Name of the response data item (e.g., TransactionID, responseCode, Status, Msisdn).",
    )
    Value: Any = Field(..., description="Value of the response data item.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "Name": "TransactionID",
                "Value": "SC8F2IQMH5",
            }
        }
    )


class StandingOrderCallbackBody(BaseModel):
    """Callback response body containing response data."""

    ResponseData: List[StandingOrderCallbackDataItem] = Field(
        default_factory=list, description="List of response data items."
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ResponseData": [
                    {"Name": "TransactionID", "Value": "SC8F2IQMH5"},
                    {"Name": "responseCode", "Value": "0"},
                    {"Name": "Status", "Value": "OKAY"},
                    {"Name": "Msisdn", "Value": "254******867"},
                ]
            }
        }
    )


class StandingOrderCallback(BaseModel):
    """Callback response schema for Standing Order."""

    ResponseHeader: StandingOrderResponseHeader = Field(
        ..., description="Response header metadata."
    )
    ResponseBody: StandingOrderCallbackBody = Field(
        ..., description="Response body with response data."
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ResponseHeader": {
                    "responseRefID": "0acc0239-20fa-4a52-8b9d-9bd64c0465c3",
                    "requestRefID": "0acc0239-20fa-4a52-8b9d-9bd64c0465c3",
                    "responseCode": "0",
                    "responseDescription": "The service request is processed successfully",
                },
                "ResponseBody": {
                    "ResponseData": [
                        {"Name": "TransactionID", "Value": "SC8F2IQMH5"},
                        {"Name": "responseCode", "Value": "0"},
                        {"Name": "Status", "Value": "OKAY"},
                        {"Name": "Msisdn", "Value": "254******867"},
                    ]
                },
            }
        }
    )

    @property
    def is_successful(self) -> bool:
        """Check if the callback indicates a successful transaction."""
        for item in self.ResponseBody.ResponseData:
            if item.Name.lower() == "responsecode" and str(item.Value) == "0":
                return True
        return False


class StandingOrderCallbackResponse(BaseModel):
    """Response after  receiving a callback from the Standing Order API."""

    ResultDesc: str = Field(
        default="The service request is processed successfully",
        description="Description of the result of the callback processing.",
    )
    ResultCode: str = Field(
        default="0",
        description="Result code indicating success (0) or failure (non-zero).",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ResultDesc": "The service request is processed successfully",
                "ResultCode": "0",
            }
        }
    )
