import json
import logging
import math
import os
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import gspread
import requests as req
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from google.oauth2.service_account import Credentials

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
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
    raise RuntimeError(
        "Missing required environment variable: GOOGLE_SERVICE_ACCOUNT_JSON"
    )

GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")
if not GOOGLE_SHEET_ID:
    raise RuntimeError("Missing required environment variable: GOOGLE_SHEET_ID")

BASE_URL = "https://open.shopline.io/v1"
HEADERS = {"accept": "application/json", "authorization": f"Bearer {API_TOKEN}"}
PER_PAGE = 100
MAX_PAGE = 100
DAYS_BACK = 14
SHEET_TAB = "Tracker"

XL_FILTER = "XL"  # only process orders containing products from promotions with this in the title
# ──────────────────────────────────────────────────────────────────────────────

_lock = threading.Lock()
_stop_flag = threading.Event()
_job = {
    "status": "idle",
    "started_at": None,
    "finished_at": None,
    "result": None,
    "error": None,
}


# ─── GOOGLE SHEETS ────────────────────────────────────────────────────────────
def connect_sheets():
    """
    Connect once and return both worksheets.
    Call this once per job run — do NOT call per row.
    Returns (tracker_ws, qualified_ws)
    """
    log.info("Connecting to Google Sheets...")
    creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)

    # Tracker tab
    tracker_ws = spreadsheet.worksheet(SHEET_TAB)

    # Qualified tab — create if missing
    try:
        qualified_ws = spreadsheet.worksheet("Qualified")
    except gspread.exceptions.WorksheetNotFound:
        qualified_ws = spreadsheet.add_worksheet(title="Qualified", rows=5000, cols=20)
        qualified_ws.append_row(
            [
                "logged_at",
                "customer_id",
                "customer_name",
                "customer_phone",
                "customer_email",
                "promo_title",
                "total_qty_bought",
                "gift_product_ids",
                "gift_qty",
                "free_orders_count",
                "order_numbers",
                "last_order_number",
                "tracker_key",
                "automation_id",
                "created_order_numbers",
                "Order ID",
            ]
        )
        log.info("Created 'Qualified' sheet tab")

    log.info("Connected to Google Sheets successfully")
    return tracker_ws, qualified_ws


def load_tracker(tracker_ws):
    """Read tracker tab into a dict. Pass in the worksheet object."""
    try:
        records = tracker_ws.get_all_records()
        tracker = {}
        for row in records:
            key = str(row.get("tracker_key", "")).strip()
            given = int(row.get("free_orders_given", 0) or 0)
            if key:
                tracker[key] = given
        log.info(f"Loaded tracker with {len(tracker)} entries from Google Sheets")
        return tracker
    except Exception as e:
        log.error(f"Could not load tracker: {e}")
        return {}


def save_tracker_row(
    tracker_ws, tracker_records, tracker_headers, tracker_key, free_orders_given
):
    """
    Upsert a tracker row. Pass in the worksheet + already-fetched records/headers
    so we don't re-read the sheet on every call.
    """
    try:
        for i, row in enumerate(tracker_records, start=2):
            if str(row.get("tracker_key", "")).strip() == tracker_key:
                col_given = tracker_headers.index("free_orders_given") + 1
                col_updated = tracker_headers.index("last_updated") + 1
                tracker_ws.update_cell(i, col_given, free_orders_given)
                tracker_ws.update_cell(
                    i, col_updated, datetime.now(timezone.utc).isoformat()
                )
                log.info(f"Updated tracker: {tracker_key} → {free_orders_given}")
                return

        tracker_ws.append_row(
            [tracker_key, free_orders_given, datetime.now(timezone.utc).isoformat()]
        )
        # Add to in-memory records so subsequent lookups in same run work
        tracker_records.append(
            {
                "tracker_key": tracker_key,
                "free_orders_given": free_orders_given,
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }
        )
        log.info(f"Appended tracker: {tracker_key} → {free_orders_given}")

    except Exception as e:
        log.error(f"Could not save tracker row for {tracker_key}: {e}")


# ─── PROMOTIONS ───────────────────────────────────────────────────────────────
def fetch_promo_page(page):
    """Fetch a single promotions page — only valid (active) promotions."""
    return rate_limited_get(
        f"{BASE_URL}/promotions",
        {
            "per_page": PER_PAGE,
            "page": page,
            "scope": "valid",
        },
    )


def fetch_bundle_gift_promotions():
    """
    Fetch all active bundle_gift promotions — filtered at API level.
    """
    log.info("Fetching active bundle_gift promotions...")

    # Fetch page 1 to get total_pages
    data = fetch_promo_page(1)
    total_pages = data.get("pagination", {}).get("total_pages", 1)
    total_count = data.get("pagination", {}).get("total_count", 0)
    all_items = data.get("items", [])
    log.info(
        f"Total active bundle_gift promotions: {total_count} across {total_pages} pages — fetching in parallel..."
    )

    # Fetch remaining pages with 2 workers (respects 2 req/s limit)
    if total_pages > 1:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(fetch_promo_page, p): p
                for p in range(2, total_pages + 1)
            }
            for future in as_completed(futures):
                try:
                    all_items.extend(future.result().get("items", []))
                except Exception as e:
                    log.warning(f"Failed to fetch promo page: {e}")

    # Filter to bundle_gift only in code (API has no discount_type filter param)
    all_promos = [
        p for p in all_items
        if p.get("discount_type") == "bundle_gift" and p.get("status") == "active"
    ]
    log.info(f"Fetched {len(all_items)} valid promotions → {len(all_promos)} are active bundle_gift")

    # Build XL product ID set — products from promotions with XL_FILTER in the title
    # Used to filter orders to only those containing XL products
    xl_product_ids = set()
    if XL_FILTER:
        for p in all_promos:
            title = (
                (p.get("title_translations") or {}).get("zh-hant", "")
                or (p.get("title_translations") or {}).get("en", "")
            )
            if XL_FILTER in title:
                for cond in (p.get("conditions") or []):
                    xl_product_ids.update(cond.get("whitelisted_product_ids", []))
        log.info(f"XL filter: found {len(xl_product_ids)} XL product IDs across all XL promotions")

    tier_groups = defaultdict(list)
    for p in all_promos:
        cond = (p.get("conditions") or [{}])[0]
        product_ids = tuple(sorted(cond.get("whitelisted_product_ids", [])))
        if not product_ids:
            continue
        tier_groups[product_ids].append(
            {
                "promo_id": p["id"],
                "title": p.get("title_translations", {}).get("zh-hant")
                or p.get("title_translations", {}).get("en")
                or p["id"],
                "min_item_count": cond.get("min_item_count") or 0,
                "gift_product_ids": p.get("discountable_product_ids", []),
                "gift_qty": p.get("discountable_quantity") or 1,
                "whitelisted_product_ids": list(product_ids),
                "is_accumulated": p.get(
                    "is_accumulated", True
                ),  # True = all tiers stack, False = highest tier only
            }
        )

    for key in tier_groups:
        tier_groups[key].sort(key=lambda t: t["min_item_count"])

    # Remove tiers where min_item_count = 1 — these auto-apply at checkout
    # We only need to track promos that require cumulative qty across multiple orders
    filtered_groups = {}
    for key, tiers in tier_groups.items():
        eligible_tiers = [t for t in tiers if t["min_item_count"] > 1]
        if eligible_tiers:
            filtered_groups[key] = eligible_tiers

    skipped = len(tier_groups) - len(filtered_groups)
    log.info(
        f"Found {len(all_promos)} bundle_gift records → {len(tier_groups)} groups → {len(filtered_groups)} groups after removing auto-apply (min_qty=1) promos ({skipped} skipped)"
    )
    return filtered_groups, xl_product_ids


# ─── RATE LIMITER ────────────────────────────────────────────────────────────
# Shopline allows 2 GET requests/second — use a semaphore + throttle to stay safe
_rate_semaphore = threading.Semaphore(2)  # max 2 concurrent GET requests
_rate_lock = threading.Lock()
_last_request_time = 0.0


def rate_limited_get(url, params):
    """GET with rate limiting: max 2 concurrent, min 0.5s between requests."""
    global _last_request_time
    with _rate_semaphore:
        with _rate_lock:
            elapsed = time.time() - _last_request_time
            if elapsed < 0.5:
                time.sleep(0.5 - elapsed)
            _last_request_time = time.time()

        for attempt in range(3):
            resp = req.get(url, headers=HEADERS, params=params)
            if resp.status_code in (429, 502, 503):
                wait = 10 * (attempt + 1)
                log.warning(
                    f"Got {resp.status_code}, retrying in {wait}s (attempt {attempt + 1}/3)..."
                )
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()


def rate_limited_post(url, payload):
    """POST with rate limiting: max 2 concurrent, min 0.5s between requests."""
    global _last_request_time
    post_headers = {**HEADERS, "content-type": "application/json"}
    with _rate_semaphore:
        with _rate_lock:
            elapsed = time.time() - _last_request_time
            if elapsed < 0.5:
                time.sleep(0.5 - elapsed)
            _last_request_time = time.time()

        for attempt in range(3):
            resp = req.post(url, headers=post_headers, json=payload)
            if resp.status_code in (429, 502, 503):
                wait = 10 * (attempt + 1)
                log.warning(
                    f"POST got {resp.status_code}, retrying in {wait}s (attempt {attempt + 1}/3)..."
                )
                time.sleep(wait)
                continue
            if not resp.ok:
                log.error(f"POST {url} failed {resp.status_code}: {resp.text}")
                resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()


# ─── ORDER CREATION ───────────────────────────────────────────────────────────
def create_free_order(customer_id, gift_product_ids, gift_qty, last_order, note=""):
    """
    Create a free gift order via Shopline API.
    Each gift_product_id gets its own line item with qty = gift_qty and price = 0.
    Returns the created order dict, or None on failure.
    """
    last_delivery = last_order.get("order_delivery", {}) or {}
    last_address = last_delivery.get("address", {}) or {}

    line_items = [
        {
            "product_id": pid,
            "quantity": gift_qty,
            "price": 0,
            "original_price": 0,
        }
        for pid in gift_product_ids
    ]

    payload = {
        "customer_id": customer_id,
        "line_items": line_items,
        "note": note or "Free gift order — automated",
        "financial_status": "paid",
        "tags": ["automation", "free-gift"],
        "shipping_address": {
            "first_name": last_order.get("customer_name", ""),
            "phone": f"+{last_order.get('customer_phone_country_code', '')}{last_order.get('customer_phone', '')}",
            "address1": last_address.get("address1", ""),
            "address2": last_address.get("address2", ""),
            "city": last_address.get("city", ""),
            "province": last_address.get("province", ""),
            "country": last_address.get("country", ""),
            "zip": last_address.get("zip", ""),
        },
        "shipping_lines": [
            {
                "price": 0,
                "title": last_delivery.get("delivery_type", ""),
            }
        ],
    }

    log.info(
        f"Creating free order for customer {customer_id} | products={gift_product_ids} qty={gift_qty}"
    )
    log.debug(f"Order payload: {json.dumps(payload, ensure_ascii=False)}")

    try:
        result = rate_limited_post(f"{BASE_URL}/orders", payload)
        order_number = result.get("order_number") or result.get("id", "?")
        log.info(f"Created order {order_number} for customer {customer_id}")
        return result
    except Exception as e:
        log.error(f"Failed to create order for customer {customer_id}: {e}")
        return None


# ─── ORDERS ───────────────────────────────────────────────────────────────────
def fetch_order_page(chunk_from, chunk_to, page):
    return rate_limited_get(
        f"{BASE_URL}/orders",
        {
            "per_page": PER_PAGE,
            "page": page,
            "created_after": chunk_from,
            "created_before": chunk_to,
            "status[]": ["confirmed"],  # only confirmed orders at API level
        },
    )


def fetch_chunk_orders(chunk, chunk_idx, total_chunks):
    """Fetch page 1 first to get total_pages, then fetch remaining pages concurrently."""
    data = fetch_order_page(chunk["from"], chunk["to"], 1)
    pagination = data.get("pagination", {})
    total_pages = min(pagination.get("total_pages", 1), MAX_PAGE)
    total_count = pagination.get("total_count", 0)
    all_items = data.get("items", [])

    log.info(
        f"Chunk {chunk_idx}/{total_chunks} | {total_count} orders | {total_pages} pages"
    )

    if total_pages > 1:
        # 2 workers respects the 2 req/s limit
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(fetch_order_page, chunk["from"], chunk["to"], p): p
                for p in range(2, total_pages + 1)
            }
            for future in as_completed(futures):
                try:
                    all_items.extend(future.result().get("items", []))
                except Exception as e:
                    log.warning(f"Failed page in chunk {chunk_idx}: {e}")


    # Include orders where payment is confirmed:
    # - order_payment.status == "completed" (all payment methods)
    # - OR order_payment.payment_data.status_code == "PR005" (Payme-specific)
    def is_paid(order):
        op = order.get("order_payment") or {}
        if op.get("status") == "completed":
            return True
        if (op.get("payment_data") or {}).get("status_code") == "PR005":
            return True
        return False

    paid = [o for o in all_items if is_paid(o)]
    log.info(
        f"Chunk {chunk_idx}/{total_chunks} done — {len(paid)} paid out of {len(all_items)} total orders"
    )
    return paid


def fetch_all_orders(days=5):
    log.info(f"Fetching paid orders from last {days} days...")
    now = datetime.now(timezone.utc)
    chunks = []
    for i in range(days - 1, -1, -1):
        chunks.append(
            {
                "from": (now - timedelta(days=i + 1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "to": (now - timedelta(days=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        )

    all_orders = []
    seen = set()

    # Process chunks sequentially but pages within each chunk in parallel
    # This avoids overwhelming the API while still being faster than fully sequential
    for idx, chunk in enumerate(chunks, 1):
        if _stop_flag.is_set():
            log.info("Stop requested — halting order fetch")
            break
        chunk_orders = fetch_chunk_orders(chunk, idx, len(chunks))
        new = [o for o in chunk_orders if o["order_number"] not in seen]
        seen.update(o["order_number"] for o in new)
        all_orders.extend(new)

    log.info(f"Total unique paid orders fetched: {len(all_orders)}")
    return all_orders


# ─── QUALIFICATION ────────────────────────────────────────────────────────────
def get_all_gift_product_ids(tier_groups):
    """Collect all gift product IDs across all promotions for fast lookup."""
    gift_ids = set()
    for tiers in tier_groups.values():
        for tier in tiers:
            gift_ids.update(tier["gift_product_ids"])
    return gift_ids


def order_has_gift_applied(order, gift_product_ids):
    """
    Check if an order already has a gift auto-applied by Shopline.
    Looks for subtotal_items where item_type = 'Gift' AND item_id is one of the gift products.
    """
    for item in order.get("subtotal_items", []):
        if item.get("item_type", "").lower() == "gift":
            if item.get("item_id") in gift_product_ids:
                return True
    return False


def check_qualification(all_orders, tier_groups, tracker):
    log.info(
        f"Checking qualification for {len(all_orders)} orders across {len(tier_groups)} promo groups..."
    )

    # Get all gift product IDs so we can detect auto-applied gifts
    all_gift_product_ids = get_all_gift_product_ids(tier_groups)

    customer_map = defaultdict(
        lambda: {
            "customer_name": "",
            "customer_phone": "",
            "customer_email": "",
            "order_numbers": [],
            "last_order": None,
            "product_qty": defaultdict(
                int
            ),  # qty from orders WITHOUT gift already applied
            "gifted_qty": defaultdict(
                int
            ),  # qty from orders where gift was already applied
        }
    )

    gifts_already_applied = 0

    for order in all_orders:
        cid = order.get("customer_id")
        if not cid:
            continue
        c = customer_map[cid]
        c["customer_name"] = order.get("customer_name", "")
        c["customer_phone"] = (
            f"+{order.get('customer_phone_country_code', '')}{order.get('customer_phone', '')}"
        )
        c["customer_email"] = order.get("customer_email", "")
        c["order_numbers"].append(order["order_number"])
        if not c["last_order"] or order["created_at"] > c["last_order"]["created_at"]:
            c["last_order"] = order

        # Check if this order already had a gift auto-applied
        gift_applied = order_has_gift_applied(order, all_gift_product_ids)
        if gift_applied:
            gifts_already_applied += 1

        for item in order.get("subtotal_items", []):
            if item.get("item_type") != "Product":
                continue
            pid = item.get("item_id")
            qty = item.get("quantity", 0)
            if pid and qty:
                if gift_applied:
                    # Gift already applied in this order — track separately
                    # so we don't double-count these units
                    c["gifted_qty"][pid] += qty
                else:
                    # No gift applied — these units count toward next free gift
                    c["product_qty"][pid] += qty

    log.info(f"Unique customers: {len(customer_map)}")
    log.info(f"Orders with gift already applied by Shopline: {gifts_already_applied}")

    to_fulfill = []
    for product_ids_key, tiers in tier_groups.items():
        product_ids = list(product_ids_key)

        # Check if this promo group is accumulated (stacking) or not
        # is_accumulated = True  → all qualifying tiers are awarded (buy 3 AND buy 6)
        # is_accumulated = False → only the highest qualifying tier is awarded
        is_accumulated = all(t.get("is_accumulated", True) for t in tiers)

        for cid, c in customer_map.items():
            total_qty = sum(c["product_qty"].get(pid, 0) for pid in product_ids)
            if total_qty == 0:
                continue

            if is_accumulated:
                # Award ALL tiers the customer qualifies for
                tiers_to_check = tiers
            else:
                # Award ONLY the highest tier the customer qualifies for
                qualifying_tiers = [
                    t
                    for t in tiers
                    if total_qty >= t["min_item_count"] and t["min_item_count"] > 0
                ]
                tiers_to_check = [qualifying_tiers[-1]] if qualifying_tiers else []

            for tier in tiers_to_check:
                min_qty = tier["min_item_count"]
                if min_qty <= 0:
                    continue
                tkey = f"{cid}|{tier['promo_id']}"
                owed = math.floor(total_qty / min_qty) if is_accumulated else 1
                given = tracker.get(tkey, 0)
                to_give = owed - given
                if to_give > 0:
                    log.info(
                        f"QUALIFY: {c['customer_name']} | {tier['title'][:40]} | qty={total_qty} owed={owed} given={given} → {to_give} ({'accumulated' if is_accumulated else 'highest tier only'})"
                    )
                    to_fulfill.append(
                        {
                            "customer_id": cid,
                            "customer_name": c["customer_name"],
                            "customer_phone": c["customer_phone"],
                            "customer_email": c["customer_email"],
                            "total_qty": total_qty,
                            "free_orders_to_create": to_give,
                            "tier": tier,
                            "tracker_key": tkey,
                            "last_order": c["last_order"],
                            "order_numbers": c["order_numbers"],
                        }
                    )

    log.info(
        f"Total free orders to create: {sum(f['free_orders_to_create'] for f in to_fulfill)}"
    )
    return to_fulfill


# ─── BATCH WRITE TO GOOGLE SHEETS ───────────────────────────────────────────
def batch_write_qualified(qualified_ws, qualified_rows):
    """
    Write all qualified rows to the Qualified sheet in a single batch call.
    Much more efficient than one row at a time.
    """
    if not qualified_rows:
        log.info("No qualified rows to write")
        return

    try:
        qualified_ws.append_rows(qualified_rows, value_input_option="RAW")
        log.info(f"Batch wrote {len(qualified_rows)} rows to Qualified sheet")
    except Exception as e:
        log.error(f"Failed to batch write to Qualified sheet: {e}")


def batch_write_tracker(tracker_ws, tracker_records, tracker_headers, tracker_updates):
    """
    Write all tracker updates in a single batch call.
    tracker_updates = { tracker_key: free_orders_given }
    """
    if not tracker_updates:
        return

    try:
        now = datetime.now(timezone.utc).isoformat()
        col_key = tracker_headers.index("tracker_key") + 1
        col_given = tracker_headers.index("free_orders_given") + 1
        col_updated = tracker_headers.index("last_updated") + 1

        # Separate into updates (existing rows) and appends (new rows)
        existing_keys = {
            str(r.get("tracker_key", "")).strip(): i + 2
            for i, r in enumerate(tracker_records)
        }

        cells_to_update = []
        rows_to_append = []

        for tkey, given in tracker_updates.items():
            if tkey in existing_keys:
                row_num = existing_keys[tkey]
                cells_to_update.append(gspread.Cell(row_num, col_given, given))
                cells_to_update.append(gspread.Cell(row_num, col_updated, now))
            else:
                rows_to_append.append([tkey, given, now])

        if cells_to_update:
            tracker_ws.update_cells(cells_to_update)
            log.info(f"Batch updated {len(cells_to_update) // 2} existing tracker rows")

        if rows_to_append:
            tracker_ws.append_rows(rows_to_append, value_input_option="RAW")
            log.info(f"Batch appended {len(rows_to_append)} new tracker rows")

    except Exception as e:
        log.error(f"Failed to batch write tracker: {e}")


# ─── MAIN JOB ─────────────────────────────────────────────────────────────────
def run_job():
    global _job
    try:
        _job["status"] = "running"
        _job["started_at"] = datetime.now(timezone.utc).isoformat()
        _job["result"] = None
        _job["error"] = None
        _stop_flag.clear()

        log.info("=== JOB STARTED ===")

        # Generate automation ID: AT-0001, AT-0002, etc.
        # Derive next number by reading the last automation_id in the Qualified sheet
        def next_automation_id(ws):
            try:
                all_vals = ws.get_all_values()
                if len(all_vals) <= 1:
                    return "AT-0001"
                header = all_vals[0]
                if "automation_id" not in header:
                    return "AT-0001"
                col = header.index("automation_id")
                ids = [
                    row[col]
                    for row in all_vals[1:]
                    if len(row) > col and row[col].startswith("AT-")
                ]
                if not ids:
                    return "AT-0001"
                last_num = max(
                    int(i.split("-")[1]) for i in ids if i.split("-")[1].isdigit()
                )
                return f"AT-{last_num + 1:04d}"
            except Exception:
                return "AT-0001"

        # Connect to Google Sheets ONCE — reuse for all reads/writes
        tracker_ws, qualified_ws = connect_sheets()
        tracker_records = tracker_ws.get_all_records()
        tracker_headers = tracker_ws.row_values(1)
        tracker = load_tracker(tracker_ws)

        automation_id = next_automation_id(qualified_ws)
        log.info(f"Automation ID for this run: {automation_id}")

        tier_groups, xl_product_ids = fetch_bundle_gift_promotions()
        all_orders = fetch_all_orders(days=DAYS_BACK)

        # Filter orders to only those containing at least one XL product
        if xl_product_ids:
            before = len(all_orders)
            all_orders = [
                o for o in all_orders
                if any(
                    item.get("item_id") in xl_product_ids
                    for item in o.get("subtotal_items", [])
                    if item.get("item_type") == "Product"
                )
            ]
            log.info(f"XL order filter: {len(all_orders)} of {before} orders contain XL products")

        to_fulfill = check_qualification(all_orders, tier_groups, tracker)

        written = []  # for job result summary
        qualified_rows = []  # batch write to Qualified sheet
        tracker_updates = {}  # batch write to Tracker sheet

        # Group by customer_id + promo so each customer gets ONE row per promo
        # key = (customer_id, promo_id) → aggregated entry
        grouped = {}

        for entry in to_fulfill:
            if _stop_flag.is_set():
                log.info("Stop requested — halting")
                break

            tkey = entry["tracker_key"]
            group_key = (entry["customer_id"], entry["tier"]["promo_id"])

            if group_key not in grouped:
                grouped[group_key] = {
                    "customer_id": entry["customer_id"],
                    "customer_name": entry["customer_name"],
                    "customer_phone": entry["customer_phone"],
                    "customer_email": entry["customer_email"],
                    "total_qty": entry["total_qty"],
                    "tier": entry["tier"],
                    "last_order": entry["last_order"],
                    "tracker_key": tkey,
                    "order_numbers": entry["order_numbers"],
                    "free_orders_to_create": entry["free_orders_to_create"],
                }
            else:
                # Same customer, same promo — accumulate
                grouped[group_key]["free_orders_to_create"] += entry[
                    "free_orders_to_create"
                ]

        # Build rows and create Shopline orders — one per customer per promo
        now = datetime.now(timezone.utc).isoformat()
        for group_key, e in grouped.items():
            if _stop_flag.is_set():
                log.info("Stop requested — halting order creation")
                break

            tkey = e["tracker_key"]
            count = e["free_orders_to_create"]
            gift_qty_per_order = e["tier"]["gift_qty"]
            total_gift_qty = gift_qty_per_order * count
            created_order_numbers = []
            created_order_ids = []

            # Create one Shopline order per free order owed
            for i in range(count):
                note = (
                    f"Free gift — {e['tier']['title']} | "
                    f"automation_id={automation_id} | "
                    f"qualifying_orders={', '.join(e.get('order_numbers', []))}"
                )
                result_order = create_free_order(
                    customer_id=e["customer_id"],
                    gift_product_ids=e["tier"]["gift_product_ids"],
                    gift_qty=gift_qty_per_order,
                    last_order=e["last_order"],
                    note=note,
                )
                if result_order:
                    on = result_order.get("order_number") or result_order.get("id", "?")
                    oid = result_order.get("id", "?")
                    created_order_numbers.append(str(on))
                    created_order_ids.append(str(oid))

            orders_created = len(created_order_numbers)
            log.info(
                f"Customer {e['customer_name']} — {orders_created}/{count} orders created: {created_order_numbers} | IDs: {created_order_ids}"
            )

            qualified_rows.append(
                [
                    now,
                    e["customer_id"],
                    e["customer_name"],
                    e["customer_phone"],
                    e["customer_email"],
                    e["tier"]["title"],
                    e["total_qty"],
                    ", ".join(e["tier"]["gift_product_ids"]),
                    total_gift_qty,  # total gift qty owed
                    count,  # how many free orders this represents
                    ", ".join(e.get("order_numbers", [])),  # all qualifying order numbers
                    e["last_order"].get("order_number", ""),
                    tkey,
                    automation_id,
                    ", ".join(created_order_numbers),  # Shopline order numbers created
                    ", ".join(created_order_ids),       # Shopline internal order IDs
                ]
            )

            # Only advance tracker for orders we successfully created
            if orders_created > 0:
                tracker[tkey] = tracker.get(tkey, 0) + orders_created
                tracker_updates[tkey] = tracker[tkey]

            written.append(
                {
                    "customer_name": e["customer_name"],
                    "customer_phone": e["customer_phone"],
                    "customer_email": e["customer_email"],
                    "promo_title": e["tier"]["title"],
                    "free_orders_to_give": count,
                    "free_orders_created": orders_created,
                    "created_order_numbers": created_order_numbers,
                    "total_gift_qty": total_gift_qty,
                }
            )

        # Batch write everything at once — 2 API calls total
        log.info(
            f"Batch writing {len(qualified_rows)} qualified rows and {len(tracker_updates)} tracker updates..."
        )
        batch_write_qualified(qualified_ws, qualified_rows)
        batch_write_tracker(
            tracker_ws, tracker_records, tracker_headers, tracker_updates
        )

        result = {
            "run_at": datetime.now(timezone.utc).isoformat(),
            "automation_id": automation_id,
            "total_orders_checked": len(all_orders),
            "qualified_written": len(written),
            "details": written,
        }

        _job["status"] = "done"
        _job["finished_at"] = datetime.now(timezone.utc).isoformat()
        _job["result"] = result
        log.info(
            f"=== JOB DONE — {len(written)} qualified customers written to Google Sheets ==="
        )

    except Exception as e:
        log.exception(f"=== JOB ERROR: {e} ===")
        _job["status"] = "error"
        _job["finished_at"] = datetime.now(timezone.utc).isoformat()
        _job["error"] = str(e)
    finally:
        _lock.release()


# ─── FLASK ROUTES ─────────────────────────────────────────────────────────────
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "time": datetime.now(timezone.utc).isoformat()})


@app.route("/run", methods=["POST"])
def run():
    if not _lock.acquire(blocking=False):
        return jsonify(
            {
                "status": "already_running",
                "message": "Job is already running. Poll /status for progress.",
            }
        ), 200

    thread = threading.Thread(target=run_job)
    thread.daemon = True
    thread.start()

    log.info("Job triggered via POST /run")
    return jsonify(
        {"status": "started", "message": "Job started. Poll /status to check progress."}
    ), 200


@app.route("/status", methods=["GET"])
def status():
    return jsonify(
        {
            "status": _job["status"],
            "started_at": _job["started_at"],
            "finished_at": _job["finished_at"],
            "result": _job["result"],
            "error": _job["error"],
        }
    ), 200


@app.route("/stop", methods=["POST"])
def stop():
    if _job["status"] != "running":
        return jsonify({"message": "No job is currently running."}), 200
    _stop_flag.set()
    log.info("Stop requested via POST /stop")
    return jsonify(
        {"message": "Stop signal sent. Job will halt at the next checkpoint."}
    ), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    log.info(f"Starting app on port {port}")
    app.run(host="0.0.0.0", port=port)
