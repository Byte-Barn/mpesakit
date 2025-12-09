"""TokenManager: Handles retrieval, storage, and refreshing of access tokens for M-Pesa API authentication."""

import base64
from datetime import datetime
from pydantic import BaseModel, PrivateAttr, ConfigDict
from typing import Optional, ClassVar

from mpesakit.http_client import HttpClient
from mpesakit.http_client import AsyncHttpClient
from mpesakit.auth import AccessToken
from mpesakit.errors import MpesaError, MpesaApiException


class TokenManager(BaseModel):
    """Handles retrieval, storage, and refreshing of access tokens for M-Pesa API authentication."""

    consumer_key: str
    consumer_secret: str
    http_client: HttpClient

    _access_token: Optional[AccessToken] = PrivateAttr(default=None)

    model_config: ClassVar[ConfigDict] = {"arbitrary_types_allowed": True}

    def _get_basic_auth_header(self) -> str:
        credentials = f"{self.consumer_key}:{self.consumer_secret}"
        encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode(
            "utf-8"
        )
        return f"Basic {encoded_credentials}"

    def get_token(self, force_refresh: bool = False) -> str:
        """Retrieves the access token, refreshing it if necessary.

        Args:
            force_refresh (bool): If True, forces a refresh of the token even if it is not expired.

        Returns:
            str: The access token string.
        """
        # Check if the token is already available and not expired
        if (
            self._access_token
            and not self._access_token.is_expired()
            and not force_refresh
        ):
            return self._access_token.token

        url = "/oauth/v1/generate"
        params = {"grant_type": "client_credentials"}
        headers = {"Authorization": self._get_basic_auth_header()}

        try:
            response = self.http_client.get(url, headers=headers, params=params)
        except MpesaApiException as e:
            if e.error.status_code == 400 and (
                e.error.error_message is None or len(e.error.error_message) == 0
            ):
                raise MpesaApiException(
                    MpesaError(
                        error_code="AUTH_INVALID_CREDENTIALS",
                        error_message="Invalid credentials provided. Please check your consumer key and secret.",
                        status_code=400,
                    )
                ) from e  # Preserve traceback
            # Re-raise other errors as-is
            raise

        token = response.get("access_token")
        expires_in = int(response.get("expires_in", 3600))

        if not token:
            raise MpesaApiException(
                MpesaError(
                    error_code="TOKEN_MISSING",
                    error_message="No access token returned by Mpesa API.",
                    status_code=None,
                    raw_response=response,
                )
            )

        self._access_token = AccessToken(
            token=token,
            creation_datetime=datetime.now(),
            expiration_time=expires_in,
        )
        return self._access_token.token


class AsyncTokenManager(BaseModel):
    """Handles retrieval, storage, and refreshing of access tokens for M-Pesa API authentication asynchronously."""

    consumer_key: str
    consumer_secret: str
    http_client: AsyncHttpClient

    _access_token: Optional[AccessToken] = PrivateAttr(default=None)

    model_config: ClassVar[ConfigDict] = {"arbitrary_types_allowed": True}

    def _get_basic_auth_header(self) -> str:
        """Utility to generate the Basic Auth header."""
        credentials = f"{self.consumer_key}:{self.consumer_secret}"
        encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode(
            "utf-8"
        )
        return f"Basic {encoded_credentials}"

    async def get_token(self, force_refresh: bool = False) -> str:
        """Retrieves the access token asynchronously, refreshing it if necessary.

        Args:
            force_refresh (bool): If True, forces a refresh of the token even if it is not expired.

        Returns:
            str: The access token string.
        """
        # Check if the token is already available and not expired
        if (
            self._access_token
            and not self._access_token.is_expired()
            and not force_refresh
        ):
            return self._access_token.token

        url = "/oauth/v1/generate"
        params = {"grant_type": "client_credentials"}
        headers = {"Authorization": self._get_basic_auth_header()}

        try:

            response = await self.http_client.get(url, headers=headers, params=params)
        except MpesaApiException as e:
            if e.error.status_code == 400 and (
                e.error.error_message is None or len(e.error.error_message) == 0
            ):
                raise MpesaApiException(
                    MpesaError(
                        error_code="AUTH_INVALID_CREDENTIALS",
                        error_message="Invalid credentials provided. Please check your consumer key and secret.",
                        status_code=400,
                    )
                ) from e
            # Re-raise other errors as-is
            raise

        token = response.get("access_token")
        expires_in = int(response.get("expires_in", 3600))

        if not token:
            raise MpesaApiException(
                MpesaError(
                    error_code="TOKEN_MISSING",
                    error_message="No access token returned by Mpesa API.",
                    status_code=None,
                    raw_response=response,
                )
            )

        self._access_token = AccessToken(
            token=token,
            creation_datetime=datetime.now(),
            expiration_time=expires_in,
        )
        return self._access_token.token
