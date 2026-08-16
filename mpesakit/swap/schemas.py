"""This module defines schemas for M-Pesa Swap API requests and responses.

It includes models for swap requests and  responses.
"""
from pydantic import BaseModel, ConfigDict, Field, field_validator
from mpesakit.utils.phone import normalize_phone_number


class SwapRequest(BaseModel):
    """Request schema for Safaricom Daraja SWAP API (/imsi/v2/checkATI)."""

    customerNumber: str = Field(
        ..., description="The customer MSISDN in '254XXXXXXXXX' format."
    )

    model_config = ConfigDict(
        json_schema_extra={"example": {"customerNumber": "254722000000"}}
    )

    @field_validator("customerNumber")
    @classmethod
    def validate_customer_number(cls, v: str) -> str:
        """Conform the customer Number to required format."""
        normalized = normalize_phone_number(str(v))
        if not normalized:
            raise ValueError(f"Invalid Kenyan MSISDN: '{v}'")
        return str(normalized)


class SwapResponse(BaseModel):
    """Response schema returned by Safaricom SWAP API."""

    requestRefID: str = Field(..., description="Unique transaction ID.")
    responseCode: str = Field(..., description="API result status code (e.g. '200').")
    responseDesc: str = Field(..., description="Human-readable response message.")
    lastSwapDate: str = Field(..., description="SIM last swap timestamp string.")

    @property
    def is_successful(self) -> bool:
        """Returns True if response code indicates success ('200')."""
        return str(self.responseCode).strip() == "200"

    @property
    def is_recently_swapped(self) -> bool:
        """Returns False if lastSwapDate returns default non-swap date (01-01-1900)."""
        return self.is_successful and not self.lastSwapDate.startswith("01-01-1900")
