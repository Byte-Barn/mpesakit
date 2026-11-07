from typing import Optional, Any
from pydantic import BaseModel


class BongaResponseHeader(BaseModel):
    requestRefId: str 
    responseCode: int | str 
    responseMessage: str  
    customerMessage: Optional[str] 
    timestamp: str


class CalculatePointsRequest(BaseModel):
    points: int | str 


class CalculatePointsBody(BaseModel):
    amount: str | float 
    points: str | int 
    rate: str | float 


class CalculatePointsResponse(BaseModel):
    header: BongaResponseHeader
    body: CalculatePointsBody
    @property
    def is_successful(self) -> bool:...

class RedeemPointsRequest(BaseModel):
    msisdn: str | int 
    amount: float | int 
    bongaPoints: int 
    conversionRate: float 
    shortCode: str | int 
    accountNumber: str 


class RedeemPointsResponse(BaseModel):
    header: BongaResponseHeader
    body: Optional[Any] 
    def is_successful(self) -> bool:...