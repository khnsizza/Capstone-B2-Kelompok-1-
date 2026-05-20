from locust import HttpUser, task, between, events
import random
import time

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "Bearer dummy",
    "X-TIMESTAMP": "2024-01-01T10:00:00+07:00",
    "X-SIGNATURE": "dummy",
    "X-PARTNER-ID": "partner1",
    "X-EXTERNAL-ID": "ext001",
    "CHANNEL-ID": "95221",
}

QR_CONTENT = "00020101021126650013ID.CO.BCA.WWW011893600014000234564002150008850023456400303UMI51440014ID.CO.QRIS.WWW0215ID10243234537600303UMI5204569153033605802ID5922Seruni Kolor & Daleman6006SRAGEN61055721562070703A0163045AAA"
    
class NormalLatencyTest(HttpUser):
    wait_time = between(0.1, 0.5)

    def on_start(self):
        # set normal latency config before test
        self.client.post("/admin/config", json={
            "latencyMinMs": 50,
            "latencyMaxMs": 100,
            "jitterMs": 10,
            "errorRate": 7,
        }, headers=HEADERS)

    @task(2)
    def test_decode(self):
        with self.client.post(
            "/v1.0/qr/qr-mpm-decode",
            json={
                "qrContent": QR_CONTENT,
                "scanTime": "2024-01-01T10:00:00+07:00",
            },
            headers=HEADERS,
            catch_response=True,
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"status {response.status_code}")

    @task(1)
    def test_payment(self):
        partner_ref = f"2024010100000000{random.randint(100000, 999999)}"

        with self.client.post(
            "/v1.0/qr/qr-mpm-payment",
            json={
                "partnerReferenceNo": partner_ref,
                "merchantId": "00007100010926",
                "amount": {
                    "value": "25000.00",
                    "currency": "IDR"
                },
            },
            headers=HEADERS,
            catch_response=True,
        ) as create:

            body = create.json()
            original_reference_no = body["referenceNo"]

            while True:

                query = self.client.post(
                    "/v1.0/qr/qr-mpm-query",
                    json={
                        "originalReferenceNo": original_reference_no,
                        "originalPartnerReferenceNo": partner_ref,
                        "merchantId": "00007100010926",
                    },
                    headers=HEADERS,
                )

                status = query.json()["latestTransactionStatus"]

                if status == "00":
                    create.success()
                    return

                if status == "06":
                    create.failure("payment failed")
                    return

                time.sleep(0.5)