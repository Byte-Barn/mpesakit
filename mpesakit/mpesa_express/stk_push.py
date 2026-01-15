"""STK Push: Initiates an M-Pesa STK Push transaction.

This module provides functionality to initiate an M-Pesa STK Push transaction using the M-Pesa API.
It requires a valid access token for authentication and uses the HttpClient for making HTTP requests.
"""

from pydantic import BaseModel, ConfigDict

from mpesakit.auth import TokenManager
from mpesakit.http_client import HttpClient
from .schemas import (
    StkPushSimulateRequest,
    StkPushSimulateResponse,
    StkPushQueryRequest,
    StkPushQueryResponse,
)


class StkPush(BaseModel):
    """Represents the request payload for initiating an M-Pesa STK Push transaction.

    https://developer.safaricom.co.ke/APIs/MpesaExpressQuery
    https://developer.safaricom.co.ke/APIs/MpesaExpressSimulate
    Attributes:
        http_client (HttpClient): The HTTP client used to make requests to the M-Pesa API.
        request (StkPushSimulateRequest): The request data for the STK Push transaction.
    """

    http_client: HttpClient
    token_manager: TokenManager

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def push(self, request: StkPushSimulateRequest) -> StkPushSimulateResponse:
        """Initiates an M-Pesa STK Push transaction.

        Returns:
            StkPushSimulateResponse: The response from the M-Pesa API after initiating the STK Push.
        """
        url = "/mpesa/stkpush/v1/processrequest"
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }

        response_data = self.http_client.post(url, json=request.model_dump(by_alias=True), headers=headers)

        return StkPushSimulateResponse(**response_data)

    def query(self, request: StkPushQueryRequest) -> StkPushQueryResponse:
        """Queries the status of an M-Pesa STK Push transaction.

        Returns:
            StkPushQueryResponse: The response from the M-Pesa API after querying the transaction status.
        """
        url = "/mpesa/stkpushquery/v1/query"
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }

        response_data = self.http_client.post(url, json=dict(request), headers=headers)

        return StkPushQueryResponse(**response_data)
