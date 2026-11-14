"""This module defines schemas for Lipa Na Bonga API requests, responses, and headers.

It includes models for point calculation and paybill point redemption.
"""

from typing import Optional, Any
from pydantic import BaseModel, Field, ConfigDict


class BongaResponseHeader(BaseModel):
    """Common header returned across Lipa Na Bonga responses."""

    requestRefId: Optional[str] = Field(..., description="Unique ID associated with the request.")
    responseCode: int | str = Field(..., description="Response code (e.g., 200 for success).")
    responseMessage: str = Field(..., description="Message associated with the response code.")
    customerMessage: Optional[str] = Field(None, description="Customer-facing message.")
    timestamp: Optional[str] = Field(..., description="Timestamp of the transaction.")


class CalculatePointsRequest(BaseModel):
    """Request schema for calculating Bonga points conversion to KES."""

    points: int | str = Field(..., description="Loyalty points accumulated by customer.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "points": "40"
            }
        }
    )


class CalculatePointsBody(BaseModel):
    """Body contents of the points calculation response."""

    amount: str | float = Field(..., description="Equivalent amount in Kenya Shillings.")
    points: str | int = Field(..., description="Equivalent points to be deducted.")
    rate: str | float = Field(..., description="Bonga conversion rate (typically 0.2).")


class CalculatePointsResponse(BaseModel):
    """Response schema for Bonga points calculation."""

    header: BongaResponseHeader
    body: CalculatePointsBody

    @property
    def is_successful(self) -> bool:
        """Return True if responseCode indicates success (e.g.,  200)."""
        return str(self.header.responseCode) == "200"


class RedeemPointsRequest(BaseModel):
    """Request schema for redeeming Bonga points towards a paybill/till."""

    msisdn: str | int = Field(..., description="Subscriber mobile number.")
    amount: float | int = Field(..., description="Monetary value of the points.")
    bongaPoints: int = Field(..., description="Points to be deducted.")
    conversionRate: float = Field(..., description="The conversion rate (e.g., 0.2).")
    shortCode: str | int = Field(..., description="Merchant Paybill or Buy Goods Till number.")
    accountNumber: str = Field(..., description="Account reference name.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "msisdn": "254720776155",
                "amount": 50,
                "bongaPoints": 250,
                "conversionRate": 0.2,
                "shortCode": "174379",
                "accountNumber": "test"
            }
        }
    )


class RedeemPointsResponse(BaseModel):
    """Response schema for Bonga points redemption request."""

    header: BongaResponseHeader
    body: Optional[Any] = Field(None, description="Response body data if any.")

    @property
    def is_successful(self) -> bool:
        """Return True if responseCode indicates success (6000)."""
        return str(self.header.responseCode) == "6000"
