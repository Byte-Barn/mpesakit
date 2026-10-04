"""This module provides client implementations (sync & async) for Safaricom IMSI APIs.

Please note this is a paid API product from Safaricom.

For further references,consult:
    https://developer.safaricom.co.ke/apis/IMSI
"""
from abc import ABC, abstractmethod
from typing import Generic, Type, TypeVar
from pydantic import BaseModel, ConfigDict

from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.http_client import AsyncHttpClient, HttpClient

from .schemas import (
    IMSIRequest,
    IMSIV1Response,
    IMSIV2Response,
    IMSIV3Response,
    BaseIMSISelfResponse,
)

R = TypeVar("R", bound=BaseIMSISelfResponse)


class BaseIMSIVersionStrategy(ABC, Generic[R]):
    """Abstract Strategy defining endpoint details and target response schema for IMSI versions."""

    @property
    @abstractmethod
    def endpoint_path(self) -> str:
        """Target URI endpoint for the version strategy."""
        pass

    @property
    @abstractmethod
    def response_schema(self) -> Type[R]:
        """Target response schema for this API version."""
        pass


class IMSIV1Strategy(BaseIMSIVersionStrategy[IMSIV1Response]):
    """Strategy for IMSI V1 Endpoint (/imsi/v1/checkATI)."""
    endpoint_path = "/imsi/v1/checkATI"
    response_schema = IMSIV1Response


class IMSIV2Strategy(BaseIMSIVersionStrategy[IMSIV2Response]):
    """Strategy for IMSI V2 Endpoint (/imsi/v2/checkATI)."""
    endpoint_path = "/imsi/v2/checkATI"
    response_schema = IMSIV2Response


class IMSIV3Strategy(BaseIMSIVersionStrategy[IMSIV3Response]):
    """Strategy for IMSI V3 Endpoint (/imsi/v3/checkATI)."""
    endpoint_path = "/imsi/v3/checkATI"
    response_schema = IMSIV3Response


class IMSI(BaseModel, Generic[R]):
    """Represents the Synchronous IMSI API client."""

    http_client: HttpClient
    token_manager: TokenManager
    version_strategy: BaseIMSIVersionStrategy[R] = IMSIV1Strategy()  # type: ignore[assignment]

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def query(self, request: IMSIRequest) -> R:
        """Executes the IMSI query."""
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }

        response_data = self.http_client.post(
            self.version_strategy.endpoint_path,
            json=request.model_dump(by_alias=True),
            headers=headers,
        )
        return self.version_strategy.response_schema(**response_data)


class AsyncIMSI(BaseModel, Generic[R]):
    """Represents the Asynchronous IMSI API client."""

    http_client: AsyncHttpClient
    token_manager: AsyncTokenManager
    version_strategy: BaseIMSIVersionStrategy[R] = IMSIV1Strategy() # type: ignore[assignment]

    model_config = ConfigDict(arbitrary_types_allowed=True)

    async def query(self, request: IMSIRequest) -> R:
        """Executes the async IMSI query."""
        token = await self.token_manager.get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        response_data = await self.http_client.post(
            self.version_strategy.endpoint_path,
            json=request.model_dump(by_alias=True),
            headers=headers,
        )
        return self.version_strategy.response_schema(**response_data)
