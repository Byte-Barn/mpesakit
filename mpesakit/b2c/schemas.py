"""This module defines schemas for M-Pesa B2C API requests and responses.

It includes models for payment requests, responses, and result notifications.
"""

from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from mpesakit.utils.phone import normalize_phone_number


class B2CCommandIDType(str, Enum):
    """Allowed values for CommandID in B2C payment requests."""

    SalaryPayment = "SalaryPayment"  # Both Registeren and Unregistered M-Pesa users
    BusinessPayment = "BusinessPayment"  # Only for registered M-Pesa users
    PromotionPayment = (
        "PromotionPayment"  # Congratulatory payments for registered M-Pesa users only
    )


class B2CRequest(BaseModel):
    """Request schema for B2C payment initiation.

    This schema is used to initiate Business-to-Customer (B2C) payments via the Safaricom Daraja API.

    NOTE: To use this API in production, you must apply for a Bulk Disbursement Account and obtain a Shortcode.
    The Shortcode required here is NOT the same as a Buy Goods Till or Paybill Till Number.
    For more information and to apply for a Bulk Disbursement Account, refer to the official documentation:
    https://developer.safaricom.co.ke/dashboard/apis?api=BusinessToCustomer

    Attributes:
        OriginatorConversationID (str): Unique identifier for the specific request.
        InitiatorName (str): Username used to initiate the request.
        SecurityCredential (str): Encrypted security credential.
        CommandID (str): Type of transaction to perform. Must be a valid B2CCommandIDType.
        Amount (int): Amount to be sent to the customer.
        PartyA (int): Shortcode sending the funds (Bulk Disbursement Account Shortcode).
        PartyB (int): Mobile number receiving the funds (must be a valid Kenyan phone number).
        Remarks (str): Comments for the transaction.
        QueueTimeOutURL (str): URL for timeout notifications.
        ResultURL (str): URL for result notifications.
        Occasion (Optional[str]): Optional occasion for payment.
    """

    OriginatorConversationID: str = Field(
        ..., description="Unique identifier for the specific request."
    )
    InitiatorName: str = Field(
        ..., description="Username used to initiate the request."
    )
    SecurityCredential: str = Field(..., description="Encrypted security credential.")
    CommandID: str = Field(..., description="Type of transaction to perform.")
    Amount: int = Field(..., description="Amount to be sent to customer.")
    PartyA: int = Field(..., description="Shortcode sending the funds.")
    PartyB: int = Field(..., description="Mobile number receiving the funds.")
    Remarks: str = Field(..., description="Comments for the transaction.")
    QueueTimeOutURL: str = Field(..., description="URL for timeout notifications.")
    ResultURL: str = Field(..., description="URL for result notifications.")
    Occasion: Optional[str] = Field(None, description="Optional occasion for payment.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "OriginatorConversationID": "12345-67890-1",
                "InitiatorName": "testapi",
                "SecurityCredential": "encrypted_credential",
                "CommandID": "BusinessPayment",
                "Amount": 1000,
                "PartyA": 600999,
                "PartyB": 254712345678,
                "Remarks": "Salary for June",
                "QueueTimeOutURL": "https://example.com/timeout",
                "ResultURL": "https://example.com/result",
                "Occasion": "JuneSalary",
            }
        }
    )

    @model_validator(mode="before")
    @classmethod
    def validate(cls, values):
        """Validate CommandID and URLs."""
        cls._validate_command_id(values)
        cls._validate_partyb(values)
        cls._validate_remarks(values)
        cls._validate_occasion(values)
        return values

    @classmethod
    def _validate_occasion(cls, values):
        """Ensure Occasion is not more than 100 characters."""
        occasion = values.get("Occasion")
        if occasion is not None and len(occasion) > 100:
            raise ValueError("Occasion must not exceed 100 characters.")
        return values

    @classmethod
    def _validate_remarks(cls, values):
        """Ensure Remarks is not more than 100 characters."""
        remarks = values.get("Remarks")
        if remarks is not None and len(remarks) > 100:
            raise ValueError("Remarks must not exceed 100 characters.")
        return values

    @classmethod
    def _validate_partyb(cls, values):
        """Ensure PartyB is a valid Kenyan phone number."""
        partyb = values.get("PartyB")
        normalized = normalize_phone_number(str(partyb))
        if normalized is None:
            raise ValueError(
                f"PartyB must be a valid Kenyan phone number, got '{partyb}'"
            )
        values["PartyB"] = int(normalized)
        return values

    @classmethod
    def _validate_command_id(cls, values):
        """Ensure CommandID is a valid B2CCommandIDType value."""
        command_id = values.get("CommandID")
        valid_ids = [e.value for e in B2CCommandIDType]
        if command_id not in valid_ids:
            raise ValueError(
                f"CommandID must be one of {valid_ids}, got '{command_id}'"
            )
        return values


class B2CResponse(BaseModel):
    """Response schema for B2C payment initiation."""

    ConversationID: Optional[str] = Field(
        ..., description="Unique ID for the payment request."
    )
    OriginatorConversationID: Optional[str] = Field(
        ..., description="ID for tracking the request."
    )
    ResponseCode: str | int = Field(..., description="Status code, 0 means success.")
    ResponseDescription: str = Field(..., description="Status message.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ConversationID": "AG_20170717_00006c6f7f5b8b6b1a62",
                "OriginatorConversationID": "12345-67890-1",
                "ResponseCode": "0",
                "ResponseDescription": "Accept the service request successfully.",
            }
        }
    )

    def is_successful(self) -> bool:
        """Return True if ResponseCode indicates success (e.g., '0', '00000000')."""
        code = str(self.ResponseCode)
        return code.strip("0") == "" and code != ""


class B2CResultParameter(BaseModel):
    """Parameter item in B2C result notification."""

    Key: str = Field(..., description="Parameter name.")
    Value: str | int | float = Field(..., description="Parameter value.")


class B2CResultMetadata(BaseModel):
    """Metadata for B2C result notification."""

    ResultType: int = Field(..., description="Type of result (0=Success, 1=Failure).")
    ResultCode: int | str = Field(..., description="Result code (0=Success).")
    ResultDesc: str = Field(..., description="Result description.")
    OriginatorConversationID: str = Field(
        ..., description="Originator conversation ID."
    )
    ConversationID: str = Field(..., description="Conversation ID.")
    TransactionID: Optional[str] = Field(
        None, description="M-Pesa transaction ID (if successful)."
    )
    ResultParameters: Optional[list[B2CResultParameter]] = Field(
        None, description="List of result parameters."
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ResultType": 0,
                "ResultCode": 0,
                "ResultDesc": "The service request is processed successfully.",
                "OriginatorConversationID": "12345-67890-1",
                "ConversationID": "AG_20170717_00006c6f7f5b8b6b1a62",
                "TransactionID": "LKXXXX1234",
                "ResultParameters": [
                    {"Key": "TransactionAmount", "Value": 1000},
                    {"Key": "TransactionReceipt", "Value": "LKXXXX1234"},
                ],
            }
        }
    )

    def __init__(self, **data):
        """Initialize B2CResultMetadata and cache parameters as a dictionary."""
        super().__init__(**data)

        self._parameters_dict = {
            param.Key: param.Value for param in self.ResultParameters or []
        }

    @property
    def transaction_amount(self) -> Optional[int | float]:
        """Return the TransactionAmount value if present."""
        return self._parameters_dict.get("TransactionAmount")

    @property
    def transaction_receipt(self) -> Optional[str]:
        """Return the TransactionReceipt value if present."""
        return self._parameters_dict.get("TransactionReceipt")

    @property
    def recipient_is_registered(self) -> Optional[bool]:
        """Return True if B2CRecipientIsRegisteredCustomer is 'Y', False if 'N', None if missing."""
        val = self._parameters_dict.get("B2CRecipientIsRegisteredCustomer")
        if val == "Y":
            return True
        if val == "N":
            return False
        return None

    @property
    def receiver_party_public_name(self) -> Optional[str]:
        """Return the ReceiverPartyPublicName value if present."""
        return self._parameters_dict.get("ReceiverPartyPublicName")

    @property
    def transaction_completed_datetime(self) -> Optional[str]:
        """Return the TransactionCompletedDateTime value if present."""
        return self._parameters_dict.get("TransactionCompletedDateTime")

    @property
    def charges_paid_account_available_funds(self) -> Optional[float]:
        """Return the B2CChargesPaidAccountAvailableFunds value if present."""
        return self._parameters_dict.get("B2CChargesPaidAccountAvailableFunds")

    @property
    def utility_account_available_funds(self) -> Optional[float]:
        """Return the B2CUtilityAccountAvailableFunds value if present."""
        return self._parameters_dict.get("B2CUtilityAccountAvailableFunds")

    @property
    def working_account_available_funds(self) -> Optional[float]:
        """Return the B2CWorkingAccountAvailableFunds value if present."""
        return self._parameters_dict.get("B2CWorkingAccountAvailableFunds")


class B2CResultCallback(BaseModel):
    """Schema for B2C result notification sent to ResultURL."""

    Result: B2CResultMetadata = Field(..., description="Result metadata.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "Result": {
                    "ResultType": 0,
                    "ResultCode": 0,
                    "ResultDesc": "The service request is processed successfully.",
                    "OriginatorConversationID": "12345-67890-1",
                    "ConversationID": "AG_20170717_00006c6f7f5b8b6b1a62",
                    "TransactionID": "LKXXXX1234",
                    "ResultParameters": [
                        {"Key": "TransactionAmount", "Value": 1000},
                        {"Key": "TransactionReceipt", "Value": "LKXXXX1234"},
                    ],
                }
            }
        }
    )

    def is_successful(self) -> bool:
        """Return True if ResultCode indicates success (e.g., '0', '00000000')."""
        code = str(self.Result.ResultCode)
        return code.strip("0") == "" and code != ""


class B2CResultCallbackResponse(BaseModel):
    """Schema for response to B2C result callback."""

    ResultCode: int | str = Field(
        default=0, description="Result code (0=Success, other=Failure)."
    )
    ResultDesc: str = Field(
        default="Result received and processed successfully.",
        description="Result description.",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ResultCode": 0,
                "ResultDesc": "Result received and processed successfully.",
            }
        }
    )


class B2CTimeoutCallback(BaseModel):
    """Schema for B2C timeout notification sent to QueueTimeOutURL."""

    Result: B2CResultMetadata = Field(..., description="Result metadata.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "Result": {
                    "ResultType": 0,
                    "ResultCode": 0,
                    "ResultDesc": "The service request is processed successfully.",
                    "OriginatorConversationID": "12345-67890-1",
                    "ConversationID": "AG_20170717_00006c6f7f5b8b6b1a62",
                }
            }
        }
    )


class B2CTimeoutCallbackResponse(BaseModel):
    """Schema for response to B2C timeout callback."""

    ResultCode: int | str = Field(
        default=0,
        description="Result code (0=Success, other=Failure).",
    )
    ResultDesc: str = Field(
        default="Timeout notification received and processed successfully.",
        description="Result description.",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ResultCode": 0,
                "ResultDesc": "Timeout notification received and processed successfully.",
            }
        }
    )
