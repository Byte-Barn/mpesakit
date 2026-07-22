"""Facade for M-Pesa C2B (Customer to Business) API interactions."""

from mpesakit.auth import TokenManager, AsyncTokenManager
from mpesakit.http_client import HttpClient, AsyncHttpClient

from mpesakit.c2b import (
    C2B,
    AsyncC2B,
    C2BRegisterUrlRequest,
    C2BRegisterUrlResponse,
)


class C2BService:
    """Facade for M-Pesa C2B operations."""

    def __init__(self, http_client: HttpClient, token_manager: TokenManager) -> None:
        """Initialize the C2B service."""
        self.http_client = http_client
        self.token_manager = token_manager
        self.c2b = C2B(
            http_client=self.http_client,
            token_manager=self.token_manager,
        )

    def register_url(
        self,
        short_code: int,
        response_type: str,
        confirmation_url: str,
        validation_url: str,
        **kwargs,
    ) -> C2BRegisterUrlResponse:
        """Register validation and confirmation URLs for C2B payments.

        Args:
            short_code: The business short code.
            response_type: The response type ("Completed" or "Cancelled").
            confirmation_url: The confirmation URL.
            validation_url: The validation URL.
            **kwargs: Additional fields for C2BRegisterUrlRequest.

        Returns:
            C2BRegisterUrlResponse: Response from the M-Pesa API.
        """
        request = C2BRegisterUrlRequest(
            ShortCode=short_code,
            ResponseType=response_type,
            ConfirmationURL=confirmation_url,
            ValidationURL=validation_url,
            **{
                k: v
                for k, v in kwargs.items()
                if k in C2BRegisterUrlRequest.model_fields
            },
        )
        return self.c2b.register_url(request)


class AsyncC2BService:
    """Async facade for M-Pesa C2B operations."""

    def __init__(
        self, http_client: AsyncHttpClient, token_manager: AsyncTokenManager
    ) -> None:
        """Initialize the async C2B service."""
        self.http_client = http_client
        self.token_manager = token_manager
        self.c2b = AsyncC2B(
            http_client=self.http_client,
            token_manager=self.token_manager,
        )

    async def register_url(
        self,
        short_code: int,
        response_type: str,
        confirmation_url: str,
        validation_url: str,
        **kwargs,
    ) -> C2BRegisterUrlResponse:
        """Register validation and confirmation URLs for C2B payments asynchronously."""
        request = C2BRegisterUrlRequest(
            ShortCode=short_code,
            ResponseType=response_type,
            ConfirmationURL=confirmation_url,
            ValidationURL=validation_url,
            **{
                k: v
                for k, v in kwargs.items()
                if k in C2BRegisterUrlRequest.model_fields
            },
        )
        return await self.c2b.register_url(request)
