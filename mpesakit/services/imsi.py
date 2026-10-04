"""Facade for M-Pesa IMSI / SIM Swap API interactions."""

from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.http_client import AsyncHttpClient, HttpClient
from mpesakit.imsi import (
    AsyncIMSI,
    IMSI,
    IMSIRequest,
    IMSIV1Response,
    IMSIV2Response,
    IMSIV3Response,
    IMSIV1Strategy,
    IMSIV2Strategy,
    IMSIV3Strategy,
)


class IMSIService:
    """Facade for M-Pesa IMSI API operations (V1, V2, and V3)."""

    def __init__(self, http_client: HttpClient, token_manager: TokenManager) -> None:
        """Initialize the IMSI service."""
        self.http_client = http_client
        self.token_manager = token_manager

        self._v1_client: IMSI[IMSIV1Response] = IMSI(
            http_client=self.http_client,
            token_manager=self.token_manager,
            version_strategy=IMSIV1Strategy(),
        )
        self._v2_client: IMSI[IMSIV2Response] = IMSI(
            http_client=self.http_client,
            token_manager=self.token_manager,
            version_strategy=IMSIV2Strategy(),
        )
        self._v3_client: IMSI[IMSIV3Response] = IMSI(
            http_client=self.http_client,
            token_manager=self.token_manager,
            version_strategy=IMSIV3Strategy(),
        )

    def query_v1(self, customer_number: str) -> IMSIV1Response:
        """Query IMSI V1 (returns Hashed IMSI, network age, and last swap date)."""
        request = IMSIRequest(customerNumber=customer_number)
        return self._v1_client.query(request)

    def query_v2(self, customer_number: str) -> IMSIV2Response:
        """Query IMSI V2 (returns network age / SIM registration date)."""
        request = IMSIRequest(customerNumber=customer_number)
        return self._v2_client.query(request)

    def query_v3(self, customer_number: str) -> IMSIV3Response:
        """Query IMSI V3 (returns Hashed IMSI only)."""
        request = IMSIRequest(customerNumber=customer_number)
        return self._v3_client.query(request)


class AsyncIMSIService:
    """Async facade for M-Pesa IMSI API operations (V1, V2, and V3)."""

    def __init__(
        self, http_client: AsyncHttpClient, token_manager: AsyncTokenManager
    ) -> None:
        """Initialize the async IMSI service."""
        self.http_client = http_client
        self.token_manager = token_manager

        self._v1_client: AsyncIMSI[IMSIV1Response] = AsyncIMSI(
            http_client=self.http_client,
            token_manager=self.token_manager,
            version_strategy=IMSIV1Strategy(),
        )
        self._v2_client: AsyncIMSI[IMSIV2Response] = AsyncIMSI(
            http_client=self.http_client,
            token_manager=self.token_manager,
            version_strategy=IMSIV2Strategy(),
        )
        self._v3_client: AsyncIMSI[IMSIV3Response] = AsyncIMSI(
            http_client=self.http_client,
            token_manager=self.token_manager,
            version_strategy=IMSIV3Strategy(),
        )

    async def query_v1(self, customer_number: str) -> IMSIV1Response:
        """Query IMSI V1 asynchronously."""
        request = IMSIRequest(customerNumber=customer_number)
        return await self._v1_client.query(request)

    async def query_v2(self, customer_number: str) -> IMSIV2Response:
        """Query IMSI V2 asynchronously."""
        request = IMSIRequest(customerNumber=customer_number)
        return await self._v2_client.query(request)

    async def query_v3(self, customer_number: str) -> IMSIV3Response:
        """Query IMSI V3 asynchronously."""
        request = IMSIRequest(customerNumber=customer_number)
        return await self._v3_client.query(request)
