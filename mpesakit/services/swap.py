"""Facade for M-Pesa Swap APIs."""

from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.http_client import AsyncHttpClient, HttpClient
from mpesakit.swap import AsyncSwap, Swap, SwapRequest, SwapResponse


class SwapService:
    """Facade for all M-Pesa Swap APIs."""

    def __init__(
        self,
        http_client: HttpClient,
        token_manager: TokenManager,
    ) -> None:
        """Initialize the Swap service facade."""
        self.http_client = http_client
        self.token_manager = token_manager
        self._swap = Swap(
            http_client=self.http_client,
            token_manager=self.token_manager,
        )

    def swap_query(self, customer_number: str) -> SwapResponse:
        """Initiate a Swap query using a phone number string."""
        request = SwapRequest(customerNumber=customer_number)
        return self._swap.swap_request(request)

    def swap_request(self, request: SwapRequest) -> SwapResponse:
        """Execute a Swap query using a SwapRequest model."""
        return self._swap.swap_request(request)


class AsyncSwapService:
    """Async facade for all M-Pesa Swap APIs."""

    def __init__(
        self,
        http_client: AsyncHttpClient,
        token_manager: AsyncTokenManager,
    ) -> None:
        """Initialize the Swap service facade."""
        self.http_client = http_client
        self.token_manager = token_manager
        self._swap = AsyncSwap(
            http_client=self.http_client,
            token_manager=self.token_manager,
        )

    async def swap_query(self, customer_number: str) -> SwapResponse:
        """Initiate an async Swap query using a phone number string."""
        request = SwapRequest(customerNumber=customer_number)
        return await self._swap.swap_request(request)

    async def swap_request(self, request: SwapRequest) -> SwapResponse:
        """Execute an async Swap query using a SwapRequest model."""
        return await self._swap.swap_request(request)
