import abc
from abc import ABC, abstractmethod
from typing import Any

class HttpClient(ABC, metaclass=abc.ABCMeta):
    @abstractmethod
    def post(self, url: str, json: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]: ...
    @abstractmethod
    def get(self, url: str, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None) -> dict[str, Any]: ...

class AsyncHttpClient(ABC, metaclass=abc.ABCMeta):
    @abstractmethod
    async def post(self, url: str, json: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]: ...
    @abstractmethod
    async def get(self, url: str, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None) -> dict[str, Any]: ...
