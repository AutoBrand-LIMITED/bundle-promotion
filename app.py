from flask import Flask, jsonify, request
import json
import math
import os
import requests as req
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import time
import threading
import logging
import gspread
from google.oauth2.service_account import Credentials

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)
# ──────────────────────────────────────────────────────────────────────────────

app = Flask(__name__)

# ─── CONFIG ───────────────────────────────────────────────────────────────────
API_TOKEN = os.environ.get("SHOPLINE_API_TOKEN")
if not API_TOKEN:
    raise RuntimeError("Missing required environment variable: SHOPLINE_API_TOKEN")

GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
if not GOOGLE_SERVICE_ACCOUNT_JSON:
    raise RuntimeError("Missing required environment variable: GOOGLE_SERVICE_ACCOUNT_JSON")

GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")
if not GOOGLE_SHEET_ID:
    raise RuntimeError("Missing required environment variable: GOOGLE_SHEET_ID")

BASE_URL  = "https://open.shopline.io/v1"
HEADERS   = {
    "accept":        "application/json",
    "authorization": f"Bearer {API_TOKEN}"
}
PER_PAGE  = 100
MAX_PAGE  = 100
DAYS_BACK = 5
SHEET_TAB = "Tracker"
# ──────────────────────────────────────────────────────────────────────────────

_lock = threading.Lock()
_job  = {
    "status":      "idle",
    "started_at":  None,
    "finished_at": None,
    "result":      None,
    "error":       None
}


# ─── GOOGLE SHEETS ────────────────────────────────────────────────────────────
def get_sheet():
    log.info("Connecting to Google Sheets...")
    creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    scopes     = ["https://www.googleapis.com/auth/spreadsheets"]
    creds      = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client     = gspread.authorize(creds)
    sheet      = client.open_by_key(GOOGLE_SHEET_ID)
    ws         = sheet.worksheet(SHEET_TAB)
    log.info("Connected to Google Sheets successfully")
    return ws


def load_tracker():
    try:
        ws      = get_sheet()
        records = ws.get_all_records()
        tracker = {}
        for row in records:
            key   = str(row.get("tracker_key", "")).strip()
            given = int(row.get("free_orders_given", 0) or 0)
            if key:
                tracker[key] = given
        log.info(f"Loaded tracker with {len(tracker)} entries from Google Sheets")
        return tracker
    except Exception as e:
        log.error(f"Could not load tracker from Google Sheets: {e}")
        return {}


def save_tracker_row(tracker_key, free_orders_given):
    try:
        ws      = get_sheet()
        records = ws.get_all_records()
        headers = ws.row_values(1)

        for i, row in enumerate(records, start=2):
            if str(row.get("tracker_key", "")).strip() == tracker_key:
                col_given   = headers.index("free_orders_given") + 1
                col_updated = headers.index("last_updated") + 1
                ws.update_cell(i, col_given,   free_orders_given)
                ws.update_cell(i, col_updated, datetime.now(timezone.utc).isoformat())
                log.info(f"Updated tracker row: {tracker_key} → {free_orders_given}")
                return

        ws.append_row([
            tracker_key,
            free_orders_given,
            datetime.now(timezone.utc).isoformat()
        ])
        log.info(f"Appended new tracker row: {tracker_key} → {free_orders_given}")

    except Exception as e:
        log.error(f"Could not save tracker row for {tracker_key}: {e}")


# ─── PROMOTIONS ───────────────────────────────────────────────────────────────
def fetch_bundle_gift_promotions():
    log.info("Fetching bundle_gift promotions...")
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
        total_pages = data.get("pagination", {}).get("total_pages", 1)
        log.info(f"Promotions page {page}/{total_pages}")
        if page >= total_pages:
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

    log.info(f"Found {len(all_promos)} bundle_gift promo records → {len(tier_groups)} promo group(s)")
    return tier_groups


# ─── ORDERS ───────────────────────────────────────────────────────────────────
def fetch_all_orders(days=5):
    log.info(f"Fetching paid orders from last {days} days...")
    now    = datetime.now(timezone.utc)
    chunks = []
    for i in range(days - 1, -1, -1):
        chunks.append({
            "from": (now - timedelta(days=i + 1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to":   (now - timedelta(days=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        })

    all_orders = []
    seen       = set()

    for idx, chunk in enumerate(chunks, 1):
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
            total_count = pagination.get("total_count", 0)

            paid = [o for o in items if o.get("order_payment", {}).get("status") == "completed"]
            new  = [o for o in paid if o["order_number"] not in seen]
            seen.update(o["order_number"] for o in new)
            all_orders.extend(new)

            log.info(f"Chunk {idx}/5 | Page {page}/{total_pages} ({total_count} total) | +{len(new)} new orders | running total: {len(all_orders)}")

            if page >= total_pages:
                break
            page += 1
            time.sleep(0.2)
        time.sleep(0.3)

    log.info(f"Total unique paid orders fetched: {len(all_orders)}")
    return all_orders


# ─── QUALIFICATION ────────────────────────────────────────────────────────────
def check_qualification(all_orders, tier_groups, tracker):
    log.info(f"Checking qualification for {len(all_orders)} orders across {len(tier_groups)} promo groups...")

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

    log.info(f"Unique customers: {len(customer_map)}")

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
                    log.info(f"QUALIFY: {c['customer_name']} | {tier['title'][:40]} | qty={total_qty} owed={owed} given={given} → create {to_give}")
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

    log.info(f"Total free orders to create: {sum(f['free_orders_to_create'] for f in to_fulfill)}")
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

    log.info(f"Creating free order for {customer['customer_name']} | promo: {tier['title'][:40]}")
    resp = req.post(
        f"{BASE_URL}/orders",
        headers={**HEADERS, "content-type": "application/json"},
        json=payload
    )

    if resp.status_code in (200, 201):
        order        = resp.json()
        order_number = order.get("order_number") or order.get("id")
        log.info(f"Free order created: {order_number} for {customer['customer_name']}")
        return order_number
    else:
        log.error(f"Failed to create order for {customer['customer_name']}: {resp.status_code} {resp.text[:300]}")
        return None


# ─── MAIN JOB ─────────────────────────────────────────────────────────────────
def run_job():
    global _job
    try:
        _job["status"]     = "running"
        _job["started_at"] = datetime.now(timezone.utc).isoformat()
        _job["result"]     = None
        _job["error"]      = None

        log.info("=== JOB STARTED ===")

        tracker     = load_tracker()
        tier_groups = fetch_bundle_gift_promotions()
        all_orders  = fetch_all_orders(days=DAYS_BACK)
        to_fulfill  = check_qualification(all_orders, tier_groups, tracker)

        created = []
        for entry in to_fulfill:
            for _ in range(entry["free_orders_to_create"]):
                order_number = create_free_order(entry, entry["tier"], entry["last_order"])
                if order_number:
                    tkey = entry["tracker_key"]
                    tracker[tkey] = tracker.get(tkey, 0) + 1
                    save_tracker_row(tkey, tracker[tkey])
                    created.append({
                        "tracker_key":      tkey,
                        "customer_name":    entry["customer_name"],
                        "customer_phone":   entry["customer_phone"],
                        "customer_email":   entry["customer_email"],
                        "promo_title":      entry["tier"]["title"],
                        "order_number":     order_number,
                        "gift_product_ids": entry["tier"]["gift_product_ids"],
                        "gift_qty":         entry["tier"]["gift_qty"]
                    })
                time.sleep(0.5)

        result = {
            "run_at":               datetime.now(timezone.utc).isoformat(),
            "total_orders_checked": len(all_orders),
            "free_orders_created":  len(created),
            "details":              created
        }

        _job["status"]      = "done"
        _job["finished_at"] = datetime.now(timezone.utc).isoformat()
        _job["result"]      = result
        log.info(f"=== JOB DONE — {len(created)} free orders created ===")

    except Exception as e:
        log.exception(f"=== JOB ERROR: {e} ===")
        _job["status"]      = "error"
        _job["finished_at"] = datetime.now(timezone.utc).isoformat()
        _job["error"]       = str(e)
    finally:
        _lock.release()


# ─── FLASK ROUTES ─────────────────────────────────────────────────────────────
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "time": datetime.now(timezone.utc).isoformat()})


@app.route("/run", methods=["POST"])
def run():
    if not _lock.acquire(blocking=False):
        return jsonify({"status": "already_running", "message": "Job is already running. Poll /status for progress."}), 200

    thread = threading.Thread(target=run_job)
    thread.daemon = True
    thread.start()

    log.info("Job triggered via POST /run")
    return jsonify({"status": "started", "message": "Job started. Poll /status to check progress."}), 200


@app.route("/status", methods=["GET"])
def status():
    return jsonify({
        "status":      _job["status"],
        "started_at":  _job["started_at"],
        "finished_at": _job["finished_at"],
        "result":      _job["result"],
        "error":       _job["error"]
    }), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    log.info(f"Starting app on port {port}")
    app.run(host="0.0.0.0", port=port)
