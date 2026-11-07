"""End-to-End Test for Lipa Na Bonga Calculate Points and Redeem Paybill.

This test interacts with the live Safaricom Sandbox environment using credentials
provided in the .env file. Marked with @pytest.mark.live so it can be selectively executed.
"""

import os
import pytest
from dotenv import load_dotenv

from mpesakit.auth import TokenManager
from mpesakit.http_client import MpesaHttpClient
from mpesakit.services.lipa_na_bonga import BongaService

load_dotenv()
pytestmark = pytest.mark.live


@pytest.fixture
def bonga_service():
    """Initialize the M-Pesa Lipa Na Bonga service with live credentials."""
    http_client = MpesaHttpClient(env=os.getenv("MPESA_ENVIRONMENT", "sandbox"))
    token_manager = TokenManager(
        consumer_key=os.getenv("MPESA_CONSUMER_KEY"),
        consumer_secret=os.getenv("MPESA_CONSUMER_SECRET"),
        http_client=http_client,
    )
    return BongaService(http_client=http_client, token_manager=token_manager)


def test_bonga_calculate_points_e2e(bonga_service):
    """End-to-end live test for Lipa Na Bonga calculate-points endpoint."""
    print("🔗 Starting E2E Test: Lipa Na Bonga Calculate Points")

    try:
        response = bonga_service.calculate_points(points=50)
        print(f"✅ Calculate Points response: {response}")

        assert response.header is not None
        assert response.body is not None
        assert str(response.body.points) == "50"
    except Exception as e:
        # Sandbox API endpoints for niche products like Bonga are occasionally stubbed or 404
        if "404" in str(e):
            pytest.xfail(
                "Lipa Na Bonga calculate-points endpoint returned 404 in Sandbox (environment limitation)."
            )
        raise e


def test_bonga_redeem_points_e2e(bonga_service):
    """End-to-end live test for Lipa Na Bonga redeem-paybill endpoint."""
    print("🔗 Starting E2E Test: Lipa Na Bonga Redeem Paybill")

    try:
        response = bonga_service.redeem_points(
            msisdn=os.getenv("MPESA_RECIPIENT_PHONE", "254722001231"),
            amount=10,
            bonga_points=50,
            conversion_rate=0.2,
            short_code="174379",
            account_number="E2ETestAcc",
        )
        print(f"✅ Redeem Points response: {response}")

        assert response.header is not None
        assert response.header.requestRefId is not None
    except Exception as e:
        if "404" in str(e):
            pytest.xfail(
                "Lipa Na Bonga redeem-paybill endpoint returned 404 in Sandbox (environment limitation)."
            )
        raise e
