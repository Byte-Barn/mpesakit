"""End-to-End Test for M-Pesa STK Push with Callback and Query.

This test simulates an M-Pesa STK Push transaction, waits for the callback,
and queries the transaction status to ensure everything works end-to-end.
"""

import atexit
import os
import time
from threading import Thread

import httpx
import pytest
from dotenv import load_dotenv
from pyngrok import ngrok

from mpesakit.auth import TokenManager
from mpesakit.http_client import MpesaHttpClient
from mpesakit.mpesa_express import (
    StkPush,
    StkPushQueryRequest,
    StkPushSimulateCallback,
    StkPushSimulateRequest,
)

pytestmark = pytest.mark.live

load_dotenv()


@pytest.fixture(scope="session")
def fastapi_server():
    """Start the FastAPI callback server in a separate thread."""

    def run():
        from uvicorn import run

        run(
            "tests.integration.mpesa_express.callback_server:app",
            host="127.0.0.1",
            port=8000,
            log_level="warning",
        )

    thread = Thread(target=run, daemon=True)
    thread.start()
    time.sleep(2)
    yield


@pytest.fixture(scope="function")
def ngrok_tunnel():
    """Create an ngrok tunnel to expose the FastAPI server."""
    tunnel = ngrok.connect(8000, bind_tls=True)
    public_url = tunnel.public_url
    print(f"🚇 ngrok tunnel: {public_url}")
    atexit.register(ngrok.disconnect, tunnel.public_url)
    yield public_url
    ngrok.disconnect(tunnel.public_url)


@pytest.fixture
def stk_service():
    """Initialize the M-Pesa STK Push service with authentication."""
    http_client = MpesaHttpClient(env=os.getenv("MPESA_ENV", "sandbox"))
    token_manager = TokenManager(
        consumer_key=os.getenv("MPESA_CONSUMER_KEY"),
        consumer_secret=os.getenv("MPESA_CONSUMER_SECRET"),
        http_client=http_client,
    )
    return StkPush(http_client=http_client, token_manager=token_manager)


def test_stk_push_full_e2e_with_query(stk_service, fastapi_server, ngrok_tunnel):
    """End-to-end test for M-Pesa STK Push with callback and query validation.

    1. Sends an STK Push request.
    2. Waits for the callback from the FastAPI server.
    3. Queries the transaction status.
    4. Validates the callback and query results.
    """
    print("🔗 Starting E2E Test: STK Push, Callback, and Query")
    client = httpx.Client(timeout=45)

    try:
        # 1. Clear previous callbacks
        callback_base_url = f"{ngrok_tunnel}/mpesa/callback"
        client.post(f"{callback_base_url}/clear")

        callback_url = f"{ngrok_tunnel}/mpesa/callback"
        print(f"📨 Using callback URL: {callback_url}")

        # 2. Send STK Push
        request = StkPushSimulateRequest(
            BusinessShortCode=int(os.getenv("MPESA_SHORTCODE")),
            Passkey=os.getenv("MPESA_PASSKEY"),
            TransactionType="CustomerPayBillOnline",
            Amount=1,
            PartyA=os.getenv("MPESA_TEST_PHONE"),
            PartyB=os.getenv("MPESA_SHORTCODE"),
            PhoneNumber=os.getenv("MPESA_TEST_PHONE"),
            CallBackURL=callback_url,
            AccountReference="E2E-Test",
            TransactionDesc="AutomatedTest",
        )

        print("📤 Sending STK Push request...")
        response = stk_service.push(request=request)
        checkout_id = response.CheckoutRequestID
        merchant_request_id = response.MerchantRequestID
        print(f"✅ STK Push sent. CheckoutRequestID: {checkout_id}")

        # 3. Wait for callback (up to 30 sec)
        print("⏳ Waiting up to 30 seconds for callback...")
        callback_received = False
        callback = None
        for _ in range(30):
            time.sleep(1)
            r = client.get(f"{callback_base_url}/latest")
            if r.status_code == 200:
                callback_received = True
                callback_json = r.json()["parsed"]
                callback = StkPushSimulateCallback.model_validate(callback_json)
                body = callback.Body.stkCallback
                print(
                    f"🎉 Callback received: ResultCode={body.ResultCode}, Desc={body.ResultDesc}"
                )
                break

        if not callback_received:
            print(
                "🟡 No callback received within 30 seconds. Will use query to check status."
            )

        # 4. Query Daraja for final status
        print(f"🔍 Querying transaction status for CheckoutRequestID: {checkout_id}")
        query_request = StkPushQueryRequest(
            BusinessShortCode=int(os.getenv("MPESA_SHORTCODE")),
            Passkey=os.getenv("MPESA_PASSKEY"),
            CheckoutRequestID=checkout_id,
        )

        final_result = None
        start_time = time.time()
        while time.time() - start_time < 60:  # Max 60s
            try:
                query_response = stk_service.query(request=query_request)
                result_code = getattr(query_response, "ResponseCode", None)
                result_desc = getattr(query_response, "ResultDesc", "")

                print(f"📊 Query result: Code={result_code}, Desc='{result_desc}'")

                if query_response.is_successful():
                    final_result = query_response
                    print("✅ Query returned a valid result.")
                    break
                time.sleep(2)
            except Exception as e:
                print(f"⚠️ Query failed: {str(e)}")
                # If we already have a valid result, break out of the loop
                if final_result is not None:
                    break

                # If HTTP 429 (rate limit), skip retrying for a short time
                if "HTTP_429" in str(e):
                    time.sleep(5)
                else:
                    time.sleep(2)

        assert final_result is not None, "❌ Query never returned a valid result"

        result_code = final_result.ResultCode
        result_desc = final_result.ResultDesc

        print(f"✅ Final result: {result_code} – {result_desc}")

        result_messages = {
            "0": "🟢 Transaction successful!",
            "1025": (
                "🟡 Error 1025: System error or USSD message too long (>182 chars).\n"
                "Solution: Retry the request, ensure your system is working, and keep messages under 182 characters."
            ),
            "9999": (
                "🟡 Error 9999: An error occurred while sending a push request.\n"
                "Solution: Retry the request."
            ),
            "2001": (
                "🔴 Error 2001: The initiator information is invalid.\n"
                "Cause: Invalid password or wrong M-PESA PIN entered.\n"
                "Solution: Use correct credentials and PIN."
            ),
            "1019": (
                "🟡 Error 1019: Transaction has expired.\n"
                "Cause: Transaction took too long to process."
            ),
            "1001": (
                "🟡 Error 1001: Unable to lock subscriber, transaction already in process.\n"
                "Cause: Duplicate MSISDN, ongoing USSD session, or supplementary services barred.\n"
                "Solution: Close existing sessions, wait 2-3 minutes, send one push at a time, or contact Safaricom."
            ),
            "1037": (
                "🟡 Error 1037: No response from user or DS timeout.\n"
                "Cause: Prompt not delivered (SIM too old, phone offline) or user did not respond in time.\n"
                "Solution: Update/upgrade SIM, ensure phone is online, or retry after callback."
            ),
            "1032": "🟡 Transaction cancelled by user.",
            "1": "🔴 Insufficient funds or other error.",
        }

        msg = result_messages.get(str(result_code))
        if msg:
            print(msg)
        else:
            pytest.fail(f"❌ Unexpected ResultCode: {result_code} – {result_desc}")

        # 5. If callback was received, cross-validate with query
        if callback_received:
            assert callback.Body.stkCallback.CheckoutRequestID == checkout_id
            assert (
                str(callback.Body.stkCallback.MerchantRequestID) == merchant_request_id
            )

        print("🎉 E2E Test Passed: STK Push, Callback (optional), and Query validated.")

    finally:
        client.close()
