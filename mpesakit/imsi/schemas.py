"""This module defines schemas for Safaricom Daraja IMSI API requests and responses (V1, V2, V3)."""
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field, field_validator
from mpesakit.utils.phone import normalize_phone_number


class IMSIRequest(BaseModel):
    """Request payload for Safaricom Daraja IMSI API endpoints."""

    customerNumber: str = Field(
        ..., description="The customer MSISDN in '2547XXXXXXXX' format."
    )

    model_config = ConfigDict(
        json_schema_extra={"example": {"customerNumber": "254722000000"}}
    )

    @field_validator("customerNumber")
    @classmethod
    def validate_customer_number(cls, v: str) -> str:
        """Conforms the phone number to standard 12-digit format."""
        normalized = normalize_phone_number(str(v))
        if not normalized:
            raise ValueError(f"Invalid Kenyan MSISDN: '{v}'")
        return str(normalized)


class StatusMixin:
    """Provides status indicator helpers based on Daraja response codes."""

    @property
    def is_successful(self) -> bool:
        """Returns True if response code indicates success ('200')."""
        response_code = getattr(self, "responseCode", None)
        if not response_code:
            return False
        return str(response_code).strip() == "200"


class SwapCheckMixin:
    """Provides helpers to evaluate SIM swap activity."""

    lastSwapDate: Optional[str] = None

    @property
    def is_recently_swapped(self) -> bool:
        """Returns True if the SIM card was swapped within the last 3 months.

        Safaricom returns '01-01-1900' if swapped more than 3 months ago or never.
        """
        if not self.lastSwapDate:
            return False
        return not self.lastSwapDate.startswith("01-01-1900")


class NetworkAgeMixin:
    """Provides helpers to evaluate network age registration date."""

    msisdnRegistrationDate: Optional[str] = None

    @property
    def is_registration_default(self) -> bool:
        """Returns True if default non-registration date '01-01-1900' is returned."""
        if not self.msisdnRegistrationDate:
            return False
        return self.msisdnRegistrationDate.startswith("01-01-1900")


class BaseIMSISelfResponse(BaseModel, StatusMixin):
    """Core fields returned by all IMSI API versions."""

    requestRefID: str = Field(..., description="Unique transaction reference ID.")
    responseCode: Optional[str] = Field(None, description="API status code e.g. '200'.")
    responseDesc: Optional[str] = Field(None, description="Response status description.")
    customerNumber: Optional[str] = Field(None, description="The requested MSISDN.")


class IMSIV1Response(BaseIMSISelfResponse, SwapCheckMixin, NetworkAgeMixin):
    """IMSI V1 response schema (Hashed IMSI, Network Age, and Last Swap Date)."""

    imsi: Optional[str] = Field(None, description="Hashed IMSI string.")


class IMSIV2Response(BaseIMSISelfResponse, NetworkAgeMixin):
    """IMSI V2 response schema (Age on Network / SIM registration date)."""

    pass


class IMSIV3Response(BaseIMSISelfResponse):
    """IMSI V3 response schema (Hashed IMSI)."""

    imsi: Optional[str] = Field(None, description="Hashed IMSI string.")


