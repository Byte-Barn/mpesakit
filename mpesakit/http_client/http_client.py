"""http_client.py: Defines an abstract base HTTP client class for making HTTP requests.

Provides a reusable interface for GET and POST requests.
"""

from typing import Dict, Any, Optional
from abc import ABC, abstractmethod


class HttpClient(ABC):
    """Abstract base HTTP client for making GET and POST requests."""

    @abstractmethod
    def post(
        self, url: str, json: Dict[str, Any], headers: Dict[str, str]
    ) -> Dict[str, Any]:
        """Sends a POST request."""
        pass

    @abstractmethod
    def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Sends a GET request."""
        pass

class AsyncHttpClient(ABC):
    """Abstract base HTTP client for making asynchronous GET and POST requests.

    This is the base class for all asynchronous HTTP client implementations.
    It provides the foundation for making asynchronous HTTP requests, which are
    essential for interacting with the Mpesa API non-blockingly.
    """

    @abstractmethod
    async def post(
        self, url: str, json: Dict[str, Any], headers: Dict[str, str]
    ) -> Dict[str, Any]:
        """Sends an asynchronous POST request."""
        pass

    @abstractmethod
    async def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Sends an asynchronous GET request."""
        pass
