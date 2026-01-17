"""Dynamic M-Pesa error handling using Pydantic.

This module defines the MpesaError and MpesaApiException classes for handling M-Pesa API errors dynamically.
It uses Pydantic for validation and serialization of error data.
"""

from typing import Dict, Any, Optional
from pydantic import BaseModel, Field


class MpesaError(BaseModel):
    """Dynamic M-Pesa error container using Pydantic."""

    request_id: Optional[str] = Field(default=None)
    error_code: Optional[str] = Field(default=None)
    error_message: Optional[str] = Field(default=None)
    status_code: Optional[int] = Field(default=None)
    raw_response: Optional[Dict[str, Any]] = Field(default=None)

    def __str__(self) -> str:
        parts = []
        if self.error_code:
            parts.append(f"Error Code: {self.error_code}")
        if self.error_message:
            parts.append(f"Message: {self.error_message}")
        if self.request_id:
            parts.append(f"Request ID: {self.request_id}")
        if not parts:
            parts.append("Unknown M-Pesa API error")
        return " | ".join(parts)


class MpesaApiException(Exception):
    """Dynamic M-Pesa API exception."""

    def __init__(self, error: MpesaError):
        """Initialize the MpesaApiException with an MpesaError."""
        self.error = error
        super().__init__(str(error))

    @property
    def error_code(self) -> Optional[str]:
        """Returns the error code from the M-Pesa error."""
        return self.error.error_code

    @property
    def request_id(self) -> Optional[str]:
        """Returns the request ID from the M-Pesa error."""
        return self.error.request_id
