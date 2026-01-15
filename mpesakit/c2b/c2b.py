"""C2B: Handles M-Pesa C2B (Customer to Business) API interactions.

This module provides functionality to register C2B URLs, validate payments, and send confirmation responses
using the M-Pesa API. Requires a valid access token for authentication and uses the HttpClient for HTTP requests.
"""

from pydantic import BaseModel, ConfigDict
from mpesakit.auth import TokenManager
from mpesakit.http_client import HttpClient


from .schemas import (
    C2BRegisterUrlRequest,
    C2BRegisterUrlResponse,
)


class C2B(BaseModel):
    """Represents the C2B API client for M-Pesa Customer to Business operations.

    https://developer.safaricom.co.ke/APIs/CustomerToBusinessRegisterURL

    Attributes:
        http_client (HttpClient): HTTP client for making requests to the M-Pesa API.
        token_manager (TokenManager): Manages access tokens for authentication.
    """

    http_client: HttpClient
    token_manager: TokenManager

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def register_url(self, request: C2BRegisterUrlRequest) -> C2BRegisterUrlResponse:
        """Registers validation and confirmation URLs for C2B payments.

        Returns:
            C2BRegisterUrlResponse: Response from the M-Pesa API after URL registration.
        """
        url = "/mpesa/c2b/v1/registerurl"
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }
        response_data = self.http_client.post(url, json=request.model_dump(by_alias=True), headers=headers)

        # Safaricom API Bug: There is a typo in the response field name
        # "OriginatorCoversationID" should be "OriginatorConversationID"
        if "OriginatorCoversationID" in response_data:
            # Rename the field to match the expected schema
            # This is a workaround for the API inconsistency
            # and should be removed once the API is fixed.
            response_data["OriginatorConversationID"] = response_data.pop(
                "OriginatorCoversationID"
            )

        return C2BRegisterUrlResponse(**response_data)
