from flask import Flask, jsonify, request
import json
import math
import os
import requests as req
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import time
import threading

app = Flask(__name__)

# ─── CONFIG ───────────────────────────────────────────────────────────────────
API_TOKEN = os.environ.get("SHOPLINE_API_TOKEN")
if not API_TOKEN:
    raise RuntimeError("Missing required environment variable: SHOPLINE_API_TOKEN")
BASE_URL  = "https://open.shopline.io/v1"
HEADERS   = {
    "accept":        "application/json",
    "authorization": f"Bearer {API_TOKEN}"
}
PER_PAGE  = 100
MAX_PAGE  = 100
DAYS_BACK = 5
# ──────────────────────────────────────────────────────────────────────────────

_lock = threading.Lock()


# ─── PROMOTIONS ───────────────────────────────────────────────────────────────
def fetch_bundle_gift_promotions():
    all_promos = []
    page = 1
    while True:
        resp = req.get(f"{BASE_URL}/promotions", headers=HEADERS,
                       params={"per_page": PER_PAGE, "page": page})
        resp.raise_for_status()
        data  = resp.json()
        items = data.get("items", [])
        all_promos.extend([
            p for p in items
            if p.get("discount_type") == "bundle_gift" and p.get("status") == "active"
        ])
        if page >= data.get("pagination", {}).get("total_pages", 1):
            break
        page += 1

    tier_groups = defaultdict(list)
    for p in all_promos:
        cond        = (p.get("conditions") or [{}])[0]
        product_ids = tuple(sorted(cond.get("whitelisted_product_ids", [])))
        if not product_ids:
            continue
        tier_groups[product_ids].append({
            "promo_id":                p["id"],
            "title":                   p.get("title_translations", {}).get("zh-hant")
                                       or p.get("title_translations", {}).get("en")
                                       or p["id"],
            "min_item_count":          cond.get("min_item_count") or 0,
            "gift_product_ids":        p.get("discountable_product_ids", []),
            "gift_qty":                p.get("discountable_quantity") or 1,
            "whitelisted_product_ids": list(product_ids)
        })

    for key in tier_groups:
        tier_groups[key].sort(key=lambda t: t["min_item_count"])

    return tier_groups


# ─── ORDERS ───────────────────────────────────────────────────────────────────
def fetch_all_orders(days=5):
    now    = datetime.now(timezone.utc)
    chunks = []
    for i in range(days - 1, -1, -1):
        chunks.append({
            "from": (now - timedelta(days=i + 1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to":   (now - timedelta(days=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        })

    all_orders = []
    seen       = set()

    for chunk in chunks:
        page = 1
        while True:
            resp = req.get(f"{BASE_URL}/orders", headers=HEADERS, params={
                "per_page":       PER_PAGE,
                "page":           page,
                "created_at_min": chunk["from"],
                "created_at_max": chunk["to"],
            })
            resp.raise_for_status()
            data        = resp.json()
            items       = data.get("items", [])
            pagination  = data.get("pagination", {})
            total_pages = min(pagination.get("total_pages", 1), MAX_PAGE)

            paid = [o for o in items if o.get("order_payment", {}).get("status") == "completed"]
            new  = [o for o in paid if o["order_number"] not in seen]
            seen.update(o["order_number"] for o in new)
            all_orders.extend(new)

            if page >= total_pages:
                break
            page += 1
            time.sleep(0.2)
        time.sleep(0.3)

    return all_orders


# ─── QUALIFICATION ────────────────────────────────────────────────────────────
def check_qualification(all_orders, tier_groups, tracker):
    """
    tracker is a dict passed in from n8n (read from Google Sheets):
    { "customer_id|promo_id": free_orders_already_given }
    """
    customer_map = defaultdict(lambda: {
        "customer_name":  "",
        "customer_phone": "",
        "customer_email": "",
        "order_numbers":  [],
        "last_order":     None,
        "product_qty":    defaultdict(int)
    })

    for order in all_orders:
        cid = order.get("customer_id")
        if not cid:
            continue
        c = customer_map[cid]
        c["customer_name"]  = order.get("customer_name", "")
        c["customer_phone"] = f"+{order.get('customer_phone_country_code','')}{order.get('customer_phone','')}"
        c["customer_email"] = order.get("customer_email", "")
        c["order_numbers"].append(order["order_number"])
        if not c["last_order"] or order["created_at"] > c["last_order"]["created_at"]:
            c["last_order"] = order
        for item in order.get("subtotal_items", []):
            if item.get("item_type") != "Product":
                continue
            pid = item.get("item_id")
            qty = item.get("quantity", 0)
            if pid and qty:
                c["product_qty"][pid] += qty

    to_fulfill = []
    for product_ids_key, tiers in tier_groups.items():
        product_ids = list(product_ids_key)
        for cid, c in customer_map.items():
            total_qty = sum(c["product_qty"].get(pid, 0) for pid in product_ids)
            if total_qty == 0:
                continue
            for tier in tiers:
                min_qty = tier["min_item_count"]
                if min_qty <= 0:
                    continue
                tkey    = f"{cid}|{tier['promo_id']}"
                owed    = math.floor(total_qty / min_qty)
                given   = tracker.get(tkey, 0)
                to_give = owed - given
                if to_give > 0:
                    to_fulfill.append({
                        "customer_id":           cid,
                        "customer_name":         c["customer_name"],
                        "customer_phone":        c["customer_phone"],
                        "customer_email":        c["customer_email"],
                        "total_qty":             total_qty,
                        "free_orders_to_create": to_give,
                        "tier":                  tier,
                        "tracker_key":           tkey,
                        "last_order":            c["last_order"]
                    })

    return to_fulfill


# ─── CREATE FREE ORDER ────────────────────────────────────────────────────────
def create_free_order(customer, tier, last_order):
    last_delivery      = last_order.get("order_delivery", {})
    last_address       = last_order.get("delivery_address", {})
    last_delivery_data = last_order.get("delivery_data", {})

    payload = {
        "customer_id":   customer["customer_id"],
        "currency_iso":  "HKD",
        "order_remarks": f"[AUTO] Free gift from promo: {tier['title']}",
        "subtotal_items": [
            {
                "item_id":    gift_pid,
                "quantity":   tier["gift_qty"],
                "item_price": {"cents": 0, "currency_iso": "HKD"}
            }
            for gift_pid in tier["gift_product_ids"]
        ],
        "order_delivery": {
            "delivery_option_id": last_delivery.get("delivery_option_id"),
            "delivery_type":      last_delivery.get("delivery_type"),
        },
        "delivery_address": {
            "recipient_name":               last_address.get("recipient_name"),
            "recipient_phone":              last_address.get("recipient_phone"),
            "recipient_phone_country_code": last_address.get("recipient_phone_country_code"),
            "country_code":                 last_address.get("country_code", "HK"),
            "address_1":                    last_address.get("address_1"),
            "address_2":                    last_address.get("address_2"),
            "district":                     last_address.get("district"),
            "city":                         last_address.get("city"),
        },
        **({"delivery_data": {"location_code": last_delivery_data.get("location_code")}}
           if last_delivery_data.get("location_code") else {})
    }

    resp = req.post(
        f"{BASE_URL}/orders",
        headers={**HEADERS, "content-type": "application/json"},
        json=payload
    )

    if resp.status_code in (200, 201):
        order = resp.json()
        return order.get("order_number") or order.get("id")
    else:
        print(f"Failed to create order for {customer['customer_name']}: {resp.status_code} {resp.text[:200]}")
        return None


# ─── MAIN JOB ─────────────────────────────────────────────────────────────────
def run_job(tracker):
    tier_groups = fetch_bundle_gift_promotions()
    all_orders  = fetch_all_orders(days=DAYS_BACK)
    to_fulfill  = check_qualification(all_orders, tier_groups, tracker)

    created          = []
    updated_tracker  = dict(tracker)  # copy so we can return changes to n8n

    for entry in to_fulfill:
        for _ in range(entry["free_orders_to_create"]):
            order_number = create_free_order(entry, entry["tier"], entry["last_order"])
            if order_number:
                tkey = entry["tracker_key"]
                updated_tracker[tkey] = updated_tracker.get(tkey, 0) + 1
                created.append({
                    "tracker_key":      tkey,
                    "customer_id":      entry["customer_id"],
                    "customer_name":    entry["customer_name"],
                    "customer_phone":   entry["customer_phone"],
                    "customer_email":   entry["customer_email"],
                    "promo_title":      entry["tier"]["title"],
                    "order_number":     order_number,
                    "gift_product_ids": entry["tier"]["gift_product_ids"],
                    "gift_qty":         entry["tier"]["gift_qty"]
                })
            time.sleep(0.5)

    return {
        "run_at":               datetime.now(timezone.utc).isoformat(),
        "total_orders_checked": len(all_orders),
        "free_orders_created":  len(created),
        "details":              created,
        "updated_tracker":      updated_tracker  # n8n writes this back to Google Sheets
    }


# ─── FLASK ROUTES ─────────────────────────────────────────────────────────────
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "time": datetime.now(timezone.utc).isoformat()})


@app.route("/run", methods=["POST"])
def run():
    """
    n8n calls POST /run with the current tracker from Google Sheets in the body.
    Returns results + updated_tracker for n8n to write back to Google Sheets.

    Expected body:
    {
      "tracker": {
        "customer_id|promo_id": 1,
        "customer_id|promo_id": 2,
        ...
      }
    }
    """
    if not _lock.acquire(blocking=False):
        return jsonify({"error": "Job already running. Try again shortly."}), 429

    try:
        body    = request.get_json(silent=True) or {}
        tracker = body.get("tracker", {})  # tracker passed in from n8n Google Sheets
        report  = run_job(tracker)
        return jsonify(report), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        _lock.release()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
