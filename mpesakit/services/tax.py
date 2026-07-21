"""Facade for M-Pesa Tax Remittance API interactions."""

from mpesakit.auth import TokenManager, AsyncTokenManager
from mpesakit.http_client import HttpClient, AsyncHttpClient

from mpesakit.tax_remittance import (
    TaxRemittance,
    AsyncTaxRemittance,
    TaxRemittanceRequest,
    TaxRemittanceResponse,
)


class TaxService:
    """Facade for M-Pesa Tax Remittance operations."""

    def __init__(self, http_client: HttpClient, token_manager: TokenManager) -> None:
        """Initialize the Tax service."""
        self.http_client = http_client
        self.token_manager = token_manager
        self.tax_remittance = TaxRemittance(
            http_client=self.http_client,
            token_manager=self.token_manager,
        )

    def remittance(
        self,
        initiator: str,
        security_credential: str,
        amount: int,
        party_a: int,
        remarks: str,
        account_reference: str,
        result_url: str,
        queue_timeout_url: str,
        **kwargs,
    ) -> TaxRemittanceResponse:
        """Initiate a tax remittance transaction.

        Args:
            initiator: Name of the initiator.
            security_credential: Security credential for authentication.
            amount: Amount to remit.
            party_a: Sender's shortcode.
            remarks: Additional remarks.
            account_reference: Account reference for the transaction.
            result_url: URL for result notification.
            queue_timeout_url: URL for timeout notification.
            **kwargs: Additional fields for TaxRemittanceRequest.

        Returns:
            TaxRemittanceResponse: Response from the M-Pesa API.
        """
        request = TaxRemittanceRequest(
            Initiator=initiator,
            SecurityCredential=security_credential,
            Amount=amount,
            PartyA=party_a,
            Remarks=remarks,
            AccountReference=account_reference,
            ResultURL=result_url,
            QueueTimeOutURL=queue_timeout_url,
            **{
                k: v
                for k, v in kwargs.items()
                if k in TaxRemittanceRequest.model_fields
            },
        )
        return self.tax_remittance.remittance(request)


class AsyncTaxService:
    """Async facade for M-Pesa Tax Remittance operations."""

    def __init__(
        self, http_client: AsyncHttpClient, token_manager: AsyncTokenManager
    ) -> None:
        """Initialize the async Tax service."""
        self.http_client = http_client
        self.token_manager = token_manager
        self.tax_remittance = AsyncTaxRemittance(
            http_client=self.http_client,
            token_manager=self.token_manager,
        )

    async def remittance(
        self,
        initiator: str,
        security_credential: str,
        amount: int,
        party_a: int,
        remarks: str,
        account_reference: str,
        result_url: str,
        queue_timeout_url: str,
        **kwargs,
    ) -> TaxRemittanceResponse:
        """Initiate a tax remittance transaction asynchronously."""
        request = TaxRemittanceRequest(
            Initiator=initiator,
            SecurityCredential=security_credential,
            Amount=amount,
            PartyA=party_a,
            Remarks=remarks,
            AccountReference=account_reference,
            ResultURL=result_url,
            QueueTimeOutURL=queue_timeout_url,
            **{
                k: v
                for k, v in kwargs.items()
                if k in TaxRemittanceRequest.model_fields
            },
        )
        return await self.tax_remittance.remittance(request)
