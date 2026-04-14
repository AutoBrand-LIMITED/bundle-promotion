"""
Local test script for create_free_order + tagging.

Before running:
  1. Fill in the TEST_* values below with real IDs from your Shopline store.
  2. Make sure your .env file has SHOPLINE_API_TOKEN set.

Run with:
  python test_create_order.py
"""

import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

API_TOKEN = os.environ.get("SHOPLINE_API_TOKEN")
if not API_TOKEN:
    raise RuntimeError("Missing SHOPLINE_API_TOKEN in .env")

BASE_URL = "https://open.shopline.io/v1"
HEADERS = {"accept": "application/json", "authorization": f"Bearer {API_TOKEN}"}

# ─── FILL THESE IN ────────────────────────────────────────────────────────────
TEST_CUSTOMER_ID = "69a7ef96efa09500109c6aab"  # test-fratz-autobrand
TEST_GIFT_PRODUCT_IDS = ["69d3760be010c00001243506"]  # XV120-XV122 bundle gift product
TEST_GIFT_QTY = 1
TEST_TAGS = ["automation", "free-gift"]

# Pulled from real order 69d4a5165edc8121db848afe (standard delivery + Alipay)
TEST_LAST_ORDER = {
    "customer_name": "test-fratz-autobrand",
    "customer_phone_country_code": "63",
    "customer_phone": "09065234400",
    "order_delivery": {
        "delivery_type": "custom",
        "delivery_option_id": "6554ab64e576ed000eb34e8e",  # $35 標準派送
        "address": {
            "address1": "123 Test Street",
            "address2": "",
            "city": "Central",
            "province": "",
            "country": "HK",
            "zip": "999077",
        },
    },
    "order_payment": {
        "payment_method_id": "6102dcee260bb7003847d196",  # Alipay HK - SHOPLINE Payments
    },
}
# ──────────────────────────────────────────────────────────────────────────────


def create_order(customer_id, gift_product_ids, gift_qty, last_order, note=""):
    last_delivery = last_order.get("order_delivery", {}) or {}
    last_address = last_delivery.get("address", {}) or {}

    items = [
        {
            "item_type": "CustomProduct",
            "item_price": 0,
            "quantity": gift_qty,
            "total": 0,
            "item_data": {"name": f"Free Gift ({pid})", "price": "0"},
        }
        for pid in gift_product_ids
    ]

    last_payment = last_order.get("order_payment", {}) or {}

    payload = {
        "order": {
            "customer_id": customer_id,
            "items": items,
            "order_remarks": note or "Free gift order — automated",
            "subtotal": 0,
            "total": 0,
            "delivery_option_id": last_delivery.get("delivery_option_id", ""),
            "payment_method_id": last_payment.get("payment_method_id", ""),
            "delivery_address": {
                "recipient_name": last_order.get("customer_name", ""),
                "recipient_phone": f"+{last_order.get('customer_phone_country_code', '')}{last_order.get('customer_phone', '')}",
                "address_1": last_address.get("address1", ""),
                "address_2": last_address.get("address2", ""),
                "city": last_address.get("city", ""),
                "state": last_address.get("province", ""),
                "country": last_address.get("country", ""),
                "country_code": last_address.get("country", ""),
                "postcode": last_address.get("zip", ""),
            },
        }
    }

    print("\n── Step 1: Create Order ─────────────────────────────────────────")
    print(f"POST {BASE_URL}/orders")
    print("Payload:", json.dumps(payload, ensure_ascii=False, indent=2))

    resp = requests.post(
        f"{BASE_URL}/orders",
        headers={**HEADERS, "content-type": "application/json"},
        json=payload,
    )

    print(f"\nStatus: {resp.status_code}")
    print("Response:", json.dumps(resp.json(), ensure_ascii=False, indent=2))
    resp.raise_for_status()

    return resp.json()


def tag_order(order_id, tags):
    url = f"{BASE_URL}/orders/{order_id}/tags"
    tag_payload = {"tags": tags}

    print("\n── Step 2: Tag Order ────────────────────────────────────────────")
    print(f"PUT {url}")
    print("Payload:", json.dumps(tag_payload, indent=2))

    resp = requests.put(
        url,
        headers={**HEADERS, "content-type": "application/json"},
        json=tag_payload,
    )

    print(f"\nStatus: {resp.status_code}")
    print("Response:", json.dumps(resp.json(), ensure_ascii=False, indent=2))
    resp.raise_for_status()

    return resp.json()


def mark_order_paid(order_id, mail_notify=False):
    url = f"{BASE_URL}/orders/{order_id}/order_payment_status"
    patch_payload = {"status": "completed", "mail_notify": mail_notify}

    print("\n── Step 3: Mark Order Paid ──────────────────────────────────────")
    print(f"PATCH {url}")
    print("Payload:", json.dumps(patch_payload, indent=2))

    resp = requests.patch(
        url,
        headers={**HEADERS, "content-type": "application/json"},
        json=patch_payload,
    )

    print(f"\nStatus: {resp.status_code}")
    print("Response:", json.dumps(resp.json(), ensure_ascii=False, indent=2))
    resp.raise_for_status()

    return resp.json()


if __name__ == "__main__":
    print("=== Test: Create Free Gift Order + Tag + Mark Paid ===")

    result = create_order(
        customer_id=TEST_CUSTOMER_ID,
        gift_product_ids=TEST_GIFT_PRODUCT_IDS,
        gift_qty=TEST_GIFT_QTY,
        last_order=TEST_LAST_ORDER,
        note="[TEST] Free gift order",
    )

    order_id = result.get("id")
    order_number = result.get("order_number") or order_id or "?"

    if order_id:
        print(f"\nOrder created: #{order_number} (id={order_id})")
        tag_order(order_id, TEST_TAGS)
        mark_order_paid(order_id)
        print(f"\n✓ Done — order #{order_number} created, tagged with {TEST_TAGS}, and marked as paid")
    else:
        print("\n⚠ Order created but no ID returned — skipping tag step")
        print("Full response:", json.dumps(result, ensure_ascii=False, indent=2))
