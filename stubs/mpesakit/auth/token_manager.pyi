from mpesakit.auth import AccessToken as AccessToken
from mpesakit.errors import MpesaApiException as MpesaApiException, MpesaError as MpesaError
from mpesakit.http_client import HttpClient as HttpClient
from pydantic import BaseModel, ConfigDict as ConfigDict
from typing import ClassVar

class TokenManager(BaseModel):
    consumer_key: str
    consumer_secret: str
    http_client: HttpClient
    model_config: ClassVar[ConfigDict]
    def get_token(self, force_refresh: bool = False) -> str: ...
