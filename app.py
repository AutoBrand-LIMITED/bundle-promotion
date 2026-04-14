import json
import logging
import math
import os
import re
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
DAYS_BACK = 30        # days back for orders
PROMOS_DAYS_BACK = 30 # days back for promotions — longer window so older sessions aren't missed
SHEET_TAB = "Tracker"

XL_FILTER = None  # optional: set to a session code (e.g. "XL") to restrict order filtering
CREATE_ORDERS = False  # set to True to create Shopline orders; False = log to sheet only
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
    Connect once and return all worksheets.
    Call this once per job run — do NOT call per row.
    Returns (tracker_ws, promo_ws, gift_ws)
      - tracker_ws       : Tracker tab (dedup tracker)
      - promo_ws         : Qualified Promotions tab (qty-based bundle_gift)
      - gift_ws          : Qualified Gifts tab (spend-based gift)
    """
    log.info("Connecting to Google Sheets...")
    creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)

    def ensure_tab(title, headers, rows=5000, cols=20):
        try:
            ws = spreadsheet.worksheet(title)
            existing = [h for h in ws.row_values(1) if h.strip()]
            if not existing:
                # Row 1 is empty or all blank — write headers into row 1
                ws.update("A1", [headers])
                log.info(f"Added headers to '{title}' sheet (row 1 was empty)")
        except gspread.exceptions.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)
            ws.update("A1", [headers])
            log.info(f"Created '{title}' sheet tab with headers")
        return ws

    TRACKER_HEADERS = ["tracker_key", "free_orders_given", "last_updated"]

    # Qty-based promos (bundle_gift) — customer buys X items → gets a gift
    PROMO_HEADERS = [
        "logged_at",
        "customer_id",
        "customer_name",
        "customer_phone",
        "customer_email",
        "promo_title",
        "session_date",
        "gift_sku",
        "total_qty_bought",
        "gift_names",
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

    # Spend-based gifts (gift) — customer spends $X → gets a gift
    GIFT_HEADERS = [
        "logged_at",
        "customer_id",
        "customer_name",
        "customer_phone",
        "customer_email",
        "promo_title",
        "session_date",
        "gift_sku",
        "total_amount",
        "gift_names",
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

    # Combined tab — mirrors GWS sheet structure for easy cross-reference
    UNFILTERED_HEADERS = [
        "session_date",       # session this promo belongs to
        "customer_id",        # 顧客 ID
        "customer_name",
        "customer_phone",
        "total_amount",       # 合計
        "gift_sku",           # e.g. XLGIFT1, XLGIFT2, XLGIFT3
        "gift_names",         # full gift name
        "應計",               # total gifts owed
        "已計",               # already applied by Shopline auto
        "補回",               # needs to be manually given (應計 - 已計)
        "type",
        "promo_title",
        "order_numbers",
    ]

    tracker_ws      = ensure_tab(SHEET_TAB, TRACKER_HEADERS, rows=5000, cols=5)
    promo_ws        = ensure_tab("Qualified Promotions", PROMO_HEADERS)
    gift_ws         = ensure_tab("Qualified Gifts", GIFT_HEADERS)
    unfiltered_ws   = ensure_tab("Unfiltered Qualified customers", UNFILTERED_HEADERS, cols=14)

    log.info("Connected to Google Sheets successfully")
    return tracker_ws, promo_ws, gift_ws, unfiltered_ws


def load_tracker(tracker_ws):
    """Read tracker tab into a dict. Pass in the worksheet object."""
    try:
        records = tracker_ws.get_all_records(expected_headers=["tracker_key", "free_orders_given", "last_updated"])
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


# ─── GIFT CATALOG ─────────────────────────────────────────────────────────────
def fetch_gift_catalog():
    """
    Fetch active gift products from /v1/gifts.
    Gifts are returned newest-first, so we stop early once we pass the DAYS_BACK cutoff.
    Returns a dict: { gift_id: gift_name }
    """
    log.info(f"Fetching gift catalog from /v1/gifts (last {PROMOS_DAYS_BACK} days)...")
    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=PROMOS_DAYS_BACK)
    catalog = {}
    page = 1

    while True:
        data = rate_limited_get(f"{BASE_URL}/gifts", {"per_page": PER_PAGE, "page": page})
        items = data.get("items", [])
        if not items:
            break

        stop = False
        for item in items:
            created_raw = item.get("created_at") or ""
            try:
                dt = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                if dt < cutoff_dt:
                    stop = True  # past our window — no need to fetch more pages
                    break
            except Exception:
                pass  # no date — include it

            gift_id = item.get("id")
            title = (
                (item.get("title_translations") or {}).get("zh-hant")
                or (item.get("title_translations") or {}).get("en")
                or gift_id
            )
            if gift_id:
                catalog[gift_id] = title

        total_pages = data.get("pagination", {}).get("total_pages", 1)
        log.info(f"  gifts page {page}/{total_pages} — {len(catalog)} gifts so far")

        if stop or page >= total_pages:
            break
        page += 1

    log.info(f"Gift catalog loaded: {len(catalog)} gifts within last {PROMOS_DAYS_BACK} days")
    return catalog


# ─── PROMOTIONS ───────────────────────────────────────────────────────────────
def fetch_promo_page(page, discount_type=None):
    """Fetch a single promotions page — only valid (active) promotions."""
    params = {
        "per_page": PER_PAGE,
        "page": page,
        "scope": "valid",
    }
    if discount_type:
        params["discount_type"] = discount_type
    return rate_limited_get(f"{BASE_URL}/promotions", params)


def extract_session_code(title):
    """
    Extract 2-4 letter session code from a promotion title.
    e.g. 'GWS XL LIVE 買滿 $1080' → 'XL'
         'QU Live 滿$3800'         → 'QU'
         'TU直播購物滿$1200'        → 'TU'
    """
    m = re.search(r'\b([A-Z]{2,4})\s*(?:LIVE|Live|直播)', title or "")
    return m.group(1) if m else None


def extract_sku_prefix(sku):
    """
    Extract alphabetic prefix from a product SKU.
    e.g. 'XL001' → 'XL', 'TUGIFT2' → 'TU', 'QU123' → 'QU'
    """
    m = re.match(r'^([A-Za-z]+)', sku or "")
    return m.group(1).upper() if m else None


def fetch_promo_pages_for_type(discount_type):
    """Fetch all pages for a given discount_type, capped at MAX_PAGE."""
    data = fetch_promo_page(1, discount_type=discount_type)
    total_pages = min(data.get("pagination", {}).get("total_pages", 1), MAX_PAGE)
    total_count = data.get("pagination", {}).get("total_count", 0)
    items = data.get("items", [])
    log.info(f"  {discount_type}: {total_count} total → fetching {total_pages} pages")

    if total_pages > 1:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(fetch_promo_page, p, discount_type): p
                for p in range(2, total_pages + 1)
            }
            for future in as_completed(futures):
                try:
                    items.extend(future.result().get("items", []))
                except Exception as e:
                    log.warning(f"Failed to fetch {discount_type} promo page: {e}")
    return items


def fetch_bundle_gift_promotions(gift_catalog=None):
    """
    Fetch all active bundle_gift and gift promotions.
    Each type is fetched separately using the discount_type API filter.
    gift_catalog is used to extract session prefixes from gift titles,
    so spend-based promos in the same session (e.g. XLGIFT1/2/3) are grouped together
    for the greedy tier algorithm.
    """
    log.info("Fetching active gift promotions (bundle_gift + gift)...")
    all_items = []
    for dtype in ("bundle_gift", "gift"):
        all_items.extend(fetch_promo_pages_for_type(dtype))

    GIFT_TYPES = {"bundle_gift", "gift"}
    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=PROMOS_DAYS_BACK)

    def promo_in_window(p):
        """Keep promotions whose start_at is within the last PROMOS_DAYS_BACK days."""
        start_at = p.get("start_at") or ""
        if not start_at:
            return True  # no date — include by default
        try:
            dt = datetime.fromisoformat(start_at.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt >= cutoff_dt
        except Exception:
            return True  # parse error — include by default

    raw_promos = [
        p for p in all_items
        if p.get("discount_type") in GIFT_TYPES
        and p.get("status") == "active"
        and promo_in_window(p)
    ]
    # Deduplicate by promo ID — the API can return the same promo on multiple pages,
    # which would cause double-counting of owed gifts per customer.
    seen_ids: set = set()
    all_promos = []
    for p in raw_promos:
        pid = p.get("id")
        if pid and pid not in seen_ids:
            seen_ids.add(pid)
            all_promos.append(p)
    log.info(
        f"Fetched {len(all_items)} valid promotions → {len(raw_promos)} active gift promos "
        f"({len(raw_promos) - len(all_promos)} duplicates removed) → {len(all_promos)} unique "
        f"started within last {PROMOS_DAYS_BACK} days"
    )

    # Build XL product ID set — used to filter orders to XL products if XL_FILTER is set
    xl_product_ids = set()
    if XL_FILTER:
        for p in all_promos:
            title = (
                (p.get("title_translations") or {}).get("zh-hant", "")
                or (p.get("title_translations") or {}).get("en", "")
            )
            if XL_FILTER in title and p.get("discount_type") == "bundle_gift":
                for cond in (p.get("conditions") or []):
                    xl_product_ids.update(cond.get("whitelisted_product_ids", []))
        log.info(f"XL product IDs (from bundle_gift promos): {len(xl_product_ids)} products")

    # Build session_product_map: (session_prefix, date) → product IDs from bundle_gift promos.
    # The bundle_gift (qty-based) promos have whitelisted_product_ids — the exact products
    # sold during that session. We use these to scope spend-based orders: only count orders
    # that contain at least one of those session products.
    # More reliable than SKU prefix matching since it uses Shopline-configured product IDs.
    # (prefix, start_date) → {"end_at": str, "pids": set}
    # Stores the bundle_gift session's end date — used as a fallback end cap for spend promos
    # whose own end_at is null, preventing orders from later sessions leaking in.
    session_product_map: dict[tuple, dict] = {}
    for p in all_promos:
        if p.get("discount_type") != "bundle_gift":
            continue
        start_date = (p.get("start_at") or "")[:10]
        if not start_date:
            continue
        pids: list = []
        for cond in (p.get("conditions") or []):
            pids.extend(cond.get("whitelisted_product_ids", []))
        if not pids:
            continue
        # Extract session prefix from gift title (e.g. "XLGIFT1 ..." → "XL")
        gift_ids_local = tuple(sorted(p.get("discountable_product_ids", [])))
        prefix = None
        if gift_catalog and gift_ids_local:
            for gid in gift_ids_local:
                title = gift_catalog.get(gid, "")
                m = re.match(r'^([A-Z]+)GIFT', title)
                if m:
                    prefix = m.group(1)
                    break
        if prefix:
            key_sp = (prefix, start_date)
            if key_sp not in session_product_map:
                session_product_map[key_sp] = {"end_at": p.get("end_at") or "", "pids": set()}
            session_product_map[key_sp]["pids"].update(pids)
            # Keep the earliest non-empty end_at across multiple bundle_gift promos for the session
            existing_end = session_product_map[key_sp]["end_at"]
            new_end = p.get("end_at") or ""
            if new_end and (not existing_end or new_end < existing_end):
                session_product_map[key_sp]["end_at"] = new_end
    session_map_summary = {f"{k[0]}/{k[1]}": v["end_at"][:10] for k, v in session_product_map.items()}
    log.info(f"Session product map (prefix/date → end_at): {session_map_summary}")

    tier_groups = defaultdict(list)
    for p in all_promos:
        cond = (p.get("conditions") or [{}])[0]
        whitelisted_ids = tuple(sorted(cond.get("whitelisted_product_ids", [])))
        gift_ids = tuple(sorted(p.get("discountable_product_ids", [])))
        min_price_obj = cond.get("min_price") or {}
        min_spend = min_price_obj.get("dollars") or 0
        min_item_count = cond.get("min_item_count") or 0

        # Skip promos with no trigger condition (no spend threshold, no qty threshold)
        if min_spend == 0 and min_item_count == 0:
            continue

        session_prefix: str | None = None
        session_end_at = ""
        if min_spend > 0:
            # Group spend promos by session: promos with the same gift-title prefix and
            # start date belong to the same session (e.g. XLGIFT1/2/3 all start with "XL").
            # This allows greedy tier fill across all gifts in the same session.
            start_at_date = (p.get("start_at") or "")[:10]
            if gift_catalog and gift_ids:
                for gid in gift_ids:
                    title = gift_catalog.get(gid, "")
                    m = re.match(r'^([A-Z]+)GIFT', title)
                    if m:
                        session_prefix = m.group(1)
                        break
            if session_prefix and start_at_date:
                key = ("__SPEND__", session_prefix, start_at_date)
                session_info = session_product_map.get((session_prefix, start_at_date), {})
                if isinstance(session_info, dict):
                    session_end_at = session_info.get("end_at", "")
                    # Products sold in this session (from bundle_gift whitelisted_product_ids).
                    # Used to scope spend calculation: only orders containing these products
                    # count as "session orders", naturally bounding the session without end_at.
                    session_product_ids_list = list(session_info.get("pids", set()))
                else:
                    session_product_ids_list = []
            else:
                # Fallback: treat as standalone (one promo = one group)
                key = ("__SPEND__",) + gift_ids if gift_ids else ("__SPEND__", p["id"])
                session_product_ids_list = []
        else:
            # Quantity-threshold promo: grouped by which products the customer must buy
            key = whitelisted_ids
            if not key:
                continue  # qty promo with no product whitelist — skip
            session_product_ids_list = []

        tier_groups[key].append(
            {
                "promo_id": p["id"],
                "title": p.get("title_translations", {}).get("zh-hant")
                or p.get("title_translations", {}).get("en")
                or p["id"],
                "min_item_count": min_item_count,
                "min_spend": min_spend,
                "gift_product_ids": list(gift_ids),
                "gift_qty": p.get("discountable_quantity") or 1,
                "whitelisted_product_ids": list(whitelisted_ids),
                "spend_all_products": not whitelisted_ids and min_spend > 0,
                "is_accumulated": p.get("is_accumulated", True),
                # start_at is kept as a lower bound (exclude pre-session orders).
                "start_at": p.get("start_at") or "",
                "end_at": p.get("end_at") or "",
                "session_prefix": session_prefix or "",
                "session_end_at": session_end_at,
                # Product IDs from the session's bundle_gift promos.
                # An order is counted as a "session order" only if it contains at least
                # one of these products — replacing end_at as the session boundary.
                # Empty list = no product filter (falls back to date-only scoping).
                "session_product_ids": session_product_ids_list,
            }
        )
        log.info(
            f"[PROMO] {p.get('title','?')} | id={p.get('id')} "
            f"| start_at={p.get('start_at')} | end_at={p.get('end_at')} "
            f"| min_spend={min_spend} | type={p.get('promotion_type')}"
        )

    # Sort tiers within each group by their threshold (ascending)
    for key, tiers in tier_groups.items():
        if tiers[0]["min_spend"] > 0:
            tiers.sort(key=lambda t: t["min_spend"])
        else:
            tiers.sort(key=lambda t: t["min_item_count"])

    # Quantity groups: remove min_item_count=1 tiers (auto-apply at checkout)
    # Spend groups: keep all tiers (never auto-applied)
    filtered_groups = {}
    for key, tiers in tier_groups.items():
        if tiers[0]["min_spend"] > 0:
            filtered_groups[key] = tiers
        else:
            eligible = [t for t in tiers if t["min_item_count"] > 1]
            if eligible:
                filtered_groups[key] = eligible

    qty_groups = sum(1 for t in filtered_groups.values() if t[0]["min_spend"] == 0)
    spend_groups = sum(1 for t in filtered_groups.values() if t[0]["min_spend"] > 0)
    log.info(
        f"Found {len(all_promos)} gift promos → {len(filtered_groups)} groups "
        f"({qty_groups} quantity-based, {spend_groups} spend-based)"
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


def rate_limited_put(url, payload):
    """PUT with rate limiting: max 2 concurrent, min 0.5s between requests."""
    global _last_request_time
    put_headers = {**HEADERS, "content-type": "application/json"}
    with _rate_semaphore:
        with _rate_lock:
            elapsed = time.time() - _last_request_time
            if elapsed < 0.5:
                time.sleep(0.5 - elapsed)
            _last_request_time = time.time()

        for attempt in range(3):
            resp = req.put(url, headers=put_headers, json=payload)
            if resp.status_code in (429, 502, 503):
                wait = 10 * (attempt + 1)
                log.warning(
                    f"PUT got {resp.status_code}, retrying in {wait}s (attempt {attempt + 1}/3)..."
                )
                time.sleep(wait)
                continue
            if not resp.ok:
                log.error(f"PUT {url} failed {resp.status_code}: {resp.text}")
                resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()


def rate_limited_patch(url, payload):
    """PATCH with rate limiting: max 2 concurrent, min 0.5s between requests."""
    global _last_request_time
    patch_headers = {**HEADERS, "content-type": "application/json"}
    with _rate_semaphore:
        with _rate_lock:
            elapsed = time.time() - _last_request_time
            if elapsed < 0.5:
                time.sleep(0.5 - elapsed)
            _last_request_time = time.time()

        for attempt in range(3):
            resp = req.patch(url, headers=patch_headers, json=payload)
            if resp.status_code in (429, 502, 503):
                wait = 10 * (attempt + 1)
                log.warning(
                    f"PATCH got {resp.status_code}, retrying in {wait}s (attempt {attempt + 1}/3)..."
                )
                time.sleep(wait)
                continue
            if not resp.ok:
                log.error(f"PATCH {url} failed {resp.status_code}: {resp.text}")
                resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()


# ─── ORDER CREATION ───────────────────────────────────────────────────────────
def create_free_order(customer_id, gift_product_ids, gift_qty, last_order, note="", gift_catalog=None):
    """
    Create a free gift order via Shopline API.
    Each gift_product_id gets its own line item with qty = gift_qty and price = 0.
    Uses gift_catalog to resolve real gift product names where available.
    Returns the created order dict, or None on failure.
    """
    last_delivery = last_order.get("order_delivery", {}) or {}
    last_address = last_delivery.get("address", {}) or {}
    catalog = gift_catalog or {}

    items = [
        {
            "item_type": "CustomProduct",
            "item_price": 0,
            "quantity": gift_qty,
            "total": 0,
            "item_data": {
                "name": catalog.get(pid) or f"Free Gift ({pid})",
                "price": "0",
            },
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
            "opt_in_auto_reward": False,      # prevent Shopline from auto-applying gift promos on top of this order
            "is_inventory_fulfillment": True, # automatically deduct gift stock when order is created
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

    log.info(
        f"Creating free order for customer {customer_id} | products={gift_product_ids} qty={gift_qty}"
    )
    log.debug(f"Order payload: {json.dumps(payload, ensure_ascii=False)}")

    try:
        result = rate_limited_post(f"{BASE_URL}/orders", payload)
        order_id = result.get("id")
        order_number = result.get("order_number") or order_id or "?"
        log.info(f"Created order {order_number} for customer {customer_id}")

        # Tags cannot be set during order creation — add them via a separate PUT call
        if order_id:
            try:
                rate_limited_put(
                    f"{BASE_URL}/orders/{order_id}/tags",
                    {"tags": ["automation", "free-gift"]},
                )
                log.info(f"Tagged order {order_number} with [automation, free-gift]")
            except Exception as tag_err:
                log.warning(f"Order {order_number} created but tagging failed: {tag_err}")

            # Mark payment as completed (paid)
            try:
                rate_limited_patch(
                    f"{BASE_URL}/orders/{order_id}/order_payment_status",
                    {"status": "completed", "mail_notify": False},
                )
                log.info(f"Marked order {order_number} payment as completed")
            except Exception as pay_err:
                log.warning(f"Order {order_number} created but marking paid failed: {pay_err}")

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


def check_qualification(all_orders, tier_groups, tracker, unfiltered=False):
    """
    unfiltered=False (default): excludes orders where Shopline already applied a gift.
                                Used for Qualified Promotions + Qualified Gifts tabs.
    unfiltered=True:            counts ALL paid orders regardless of gift-applied status.
                                Used for Unfiltered Qualified customers tab.
    """
    log.info(
        f"Checking qualification ({'unfiltered' if unfiltered else 'filtered'}) "
        f"for {len(all_orders)} orders across {len(tier_groups)} promo groups..."
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
            "product_qty": defaultdict(int),     # qty per product_id (for qty-based promos)
            "product_spend": defaultdict(float), # spend per product_id (for qty-based promos)
            "order_items": [],  # list of {created_at, pid_spend, total_spend}
            "gift_applied_counts": defaultdict(int),  # gift_product_id → times Shopline auto-applied
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

        gift_applied = order_has_gift_applied(order, all_gift_product_ids)
        if gift_applied:
            gifts_already_applied += 1

        # Count how many times each specific gift was auto-applied by Shopline
        for item in order.get("subtotal_items", []):
            if item.get("item_type", "").lower() == "gift":
                pid = item.get("item_id")
                if pid and pid in all_gift_product_ids:
                    c["gift_applied_counts"][pid] += 1

        order_pid_spend = defaultdict(float)
        order_sku_prefixes = set()

        for item in order.get("subtotal_items", []):
            if item.get("item_type") != "Product":
                continue
            pid = item.get("item_id")
            qty = item.get("quantity", 0)
            # Track SKU prefix so calc_tier_spend can scope orders to the right session
            sku = item.get("sku") or ""
            pfx = extract_sku_prefix(sku)
            if pfx:
                order_sku_prefixes.add(pfx)
            # Use item_price (original unit price) — the top-level "price" field is often 0
            unit_price = (item.get("item_price") or {}).get("dollars", 0)
            line_total = unit_price * qty
            if pid and qty:
                if unfiltered or not gift_applied:
                    c["product_qty"][pid] += qty
                    c["product_spend"][pid] += line_total
                    order_pid_spend[pid] += line_total

        # Use the order's own total for all-product spend (matches client's GWS calculation).
        # Per-product spend (pid_spend) uses item_price for specific-product promos.
        order_total = (order.get("total") or {}).get("dollars", 0)

        # unfiltered: count all orders; filtered: skip orders where Shopline already gave a gift
        if order_total > 0 and (unfiltered or not gift_applied):
            c["order_items"].append({
                "created_at": order.get("created_at", ""),
                "pid_spend": dict(order_pid_spend),
                "total_spend": order_total,
                "sku_prefixes": order_sku_prefixes,
            })

    log.info(f"Unique customers: {len(customer_map)}")
    log.info(f"Orders with gift already applied by Shopline: {gifts_already_applied}")

    def calc_tier_spend(tier, customer, product_ids):
        """
        Sum session spend for a customer.

        Session scoping strategy (in priority order):
          1. If the promo has its own end_at → use start_at..end_at date window.
          2. If session_end_at is known (from the matching bundle_gift promo) → use it
             as the upper bound together with start_at.
          3. Fallback → start_at only (no upper bound).
        """
        start_at = tier.get("start_at") or ""
        end_at = tier.get("end_at") or tier.get("session_end_at") or ""

        relevant = [
            o for o in customer["order_items"]
            if (not start_at or o["created_at"] >= start_at)
            and (not end_at or o["created_at"] <= end_at)
        ]

        if tier["spend_all_products"]:
            return sum(o["total_spend"] for o in relevant)
        else:
            return sum(
                o["pid_spend"].get(pid, 0)
                for o in relevant
                for pid in product_ids
            )

    to_fulfill = []
    for product_ids_key, tiers in tier_groups.items():
        is_spend_promo = tiers[0]["min_spend"] > 0
        # Use whitelisted_product_ids directly from the tier (works for both spend & qty promos)
        product_ids = tiers[0]["whitelisted_product_ids"]

        for cid, c in customer_map.items():
            if is_spend_promo:
                # ── Greedy tier fill (client's formula) ──────────────────────
                # Sort tiers by threshold descending (e.g. $1080, $680)
                sorted_tiers = sorted(tiers, key=lambda t: t["min_spend"], reverse=True)
                min_threshold = sorted_tiers[-1]["min_spend"]  # lowest in session group

                # Total cumulative spend within date window, excluding single-order auto-applies.
                # All tiers in the same session group share the same date window, so we
                # use the first (highest) tier's window as the reference.
                ref_tier = sorted_tiers[0]
                total_spend = calc_tier_spend(ref_tier, c, product_ids)
                total_amount = total_spend  # same — no exclusions applied

                if total_spend < min_threshold:
                    continue

                # Greedy fill: fill $1080 blocks first, then $680 blocks with the remainder.
                # e.g. $3920 → 3×$1080 + 1×$680 → XLGIFT1=4, XLGIFT2=3, XLGIFT3=3
                unique_thresholds = sorted(set(t["min_spend"] for t in tiers), reverse=True)
                remaining = total_spend
                tier_blocks = {}  # threshold → number of complete blocks
                for threshold in unique_thresholds:
                    n = int(remaining // threshold)
                    tier_blocks[threshold] = n
                    remaining = remaining % threshold

                # Each tier's owed count = blocks filled at that threshold OR higher
                # (lower-threshold gifts are earned alongside higher-threshold blocks)
                tiers_to_check = []
                for t in sorted_tiers:
                    count = sum(v for thresh, v in tier_blocks.items() if thresh >= t["min_spend"])
                    if count > 0:
                        tiers_to_check.append((t, total_spend, count))

                if not tiers_to_check:
                    continue
            else:
                total_qty = sum(c["product_qty"].get(pid, 0) for pid in product_ids)
                if total_qty == 0:
                    continue
                best_qty_tiers = [
                    t for t in tiers
                    if total_qty >= t["min_item_count"] and t["min_item_count"] > 0
                ]
                if not best_qty_tiers:
                    continue
                tiers_to_check = [(best_qty_tiers[-1], total_qty, 1)]

            for tier, metric_value, owed in tiers_to_check:
                tkey = f"{cid}|{tier['promo_id']}"
                given = tracker.get(tkey, 0)
                to_give = owed - given
                if to_give > 0:
                    if is_spend_promo:
                        metric = (
                            f"spend=HK${metric_value:.0f} count={owed} "
                            f"(threshold HK${tier['min_spend']}) "
                            f"window={tier.get('start_at','')[:10]}~{tier.get('end_at','')[:10]}"
                        )
                    else:
                        metric = f"qty={metric_value} (min {tier['min_item_count']})"
                    log.info(
                        f"QUALIFY: {c['customer_name']} | {tier['title'][:40]} | {metric} owed={owed} given={given} → {to_give}"
                    )
                    to_fulfill.append(
                        {
                            "customer_id": cid,
                            "customer_name": c["customer_name"],
                            "customer_phone": c["customer_phone"],
                            "customer_email": c["customer_email"],
                            "total_qty": metric_value if not is_spend_promo else 0,
                            "total_amount": total_amount if is_spend_promo else 0,
                            "free_orders_to_create": to_give,
                            "owed": owed,
                            "tier": tier,
                            "tracker_key": tkey,
                            "last_order": c["last_order"],
                            "order_numbers": c["order_numbers"],
                            "gift_applied_counts": dict(c["gift_applied_counts"]),
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
        tracker_ws, promo_ws, gift_ws, unfiltered_ws = connect_sheets()
        tracker_records = tracker_ws.get_all_records(expected_headers=["tracker_key", "free_orders_given", "last_updated"])
        tracker_headers = tracker_ws.row_values(1)
        tracker = load_tracker(tracker_ws)

        automation_id = next_automation_id(promo_ws)
        log.info(f"Automation ID for this run: {automation_id}")

        gift_catalog = fetch_gift_catalog()
        tier_groups, xl_product_ids = fetch_bundle_gift_promotions(gift_catalog)
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

        # Filtered: excludes orders where Shopline already applied a gift → Qualified Promotions + Qualified Gifts
        to_fulfill = check_qualification(all_orders, tier_groups, tracker, unfiltered=False)
        # Unfiltered: counts all paid orders → Unfiltered Qualified customers tab
        to_fulfill_all = check_qualification(all_orders, tier_groups, tracker, unfiltered=True)

        written = []            # for job result summary
        promo_rows = []         # batch write to Qualified Promotions (qty-based)
        gift_rows = []          # batch write to Qualified Gifts (spend-based)
        unfiltered_rows = []    # batch write to Unfiltered Qualified customers (all combined)
        tracker_updates = {}

        # Group by customer_id + promo so each customer gets ONE row per promo
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
                    "total_amount": entry["total_amount"],
                    "tier": entry["tier"],
                    "last_order": entry["last_order"],
                    "tracker_key": tkey,
                    "order_numbers": entry["order_numbers"],
                    "free_orders_to_create": entry["free_orders_to_create"],
                    "owed": entry.get("owed", entry["free_orders_to_create"]),
                    "gift_applied_counts": entry.get("gift_applied_counts", {}),
                }
            else:
                grouped[group_key]["free_orders_to_create"] += entry["free_orders_to_create"]
                grouped[group_key]["owed"] = grouped[group_key].get("owed", 0) + entry.get("owed", entry["free_orders_to_create"])

        # Build rows — one per customer per promo, routed to correct sheet
        now = datetime.now(timezone.utc).isoformat()
        for group_key, e in grouped.items():
            if _stop_flag.is_set():
                log.info("Stop requested — halting")
                break

            tkey = e["tracker_key"]
            count = e["free_orders_to_create"]
            gift_qty_per_order = e["tier"]["gift_qty"]
            total_gift_qty = gift_qty_per_order * count
            is_spend_promo = e["tier"]["min_spend"] > 0
            created_order_numbers = []
            created_order_ids = []
            orders_created = 0

            if CREATE_ORDERS:
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
                        gift_catalog=gift_catalog,
                    )
                    if result_order:
                        on = result_order.get("order_number") or result_order.get("id", "?")
                        oid = result_order.get("id", "?")
                        created_order_numbers.append(str(on))
                        created_order_ids.append(str(oid))

                orders_created = len(created_order_numbers)
                log.info(
                    f"Customer {e['customer_name']} — {orders_created}/{count} orders created: {created_order_numbers}"
                )
                tracker[tkey] = tracker.get(tkey, 0) + orders_created
                tracker_updates[tkey] = tracker[tkey]
            else:
                log.info(
                    f"[{'GIFT' if is_spend_promo else 'PROMO'}] {e['customer_name']} — {count} free orders to log"
                )
                tracker[tkey] = tracker.get(tkey, 0) + count
                tracker_updates[tkey] = tracker[tkey]

            gift_pids = e["tier"]["gift_product_ids"]
            gift_names = ", ".join(gift_catalog.get(pid) or pid for pid in gift_pids)

            # gift_sku: extract the SKU code from the first gift name (e.g. "XLGIFT1")
            # Takes the first word of the first gift title — works for XLGIFT1, XMGIFT2, etc.
            first_gift_title = gift_catalog.get(gift_pids[0], "") if gift_pids else ""
            gift_sku = first_gift_title.split()[0] if first_gift_title else ", ".join(gift_pids)

            # session_date: date portion of promotion's start_at (e.g. "2026-03-30")
            session_date = (e["tier"].get("start_at") or "")[:10]

            # Common fields shared by both sheets
            common = [
                now,
                e["customer_id"],
                e["customer_name"],
                e["customer_phone"],
                e["customer_email"],
                e["tier"]["title"],
                session_date,
                gift_sku,
            ]
            tail = [
                gift_names,
                ", ".join(gift_pids),
                total_gift_qty,
                count,
                ", ".join(e.get("order_numbers", [])),
                e["last_order"].get("order_number", ""),
                tkey,
                automation_id,
                ", ".join(created_order_numbers),
                ", ".join(created_order_ids),
            ]

            if is_spend_promo:
                # Qualified Gifts: spend-based — show raw total_amount (HK$)
                amount_val = f"HK${e['total_amount']:.0f}"
                gift_rows.append(common + [amount_val] + tail)
            else:
                # Qualified Promotions: qty-based — show total_qty_bought
                amount_val = e["total_qty"]
                promo_rows.append(common + [amount_val] + tail)

            written.append(
                {
                    "customer_name": e["customer_name"],
                    "customer_phone": e["customer_phone"],
                    "customer_email": e["customer_email"],
                    "promo_title": e["tier"]["title"],
                    "type": "gift" if is_spend_promo else "promotion",
                    "free_orders_to_give": count,
                    "free_orders_created": orders_created,
                    "created_order_numbers": created_order_numbers,
                    "total_gift_qty": total_gift_qty,
                }
            )

        # Build unfiltered rows from to_fulfill_all (same shape but no gift_applied exclusion)
        grouped_all = {}
        for entry in to_fulfill_all:
            group_key = (entry["customer_id"], entry["tier"]["promo_id"])
            if group_key not in grouped_all:
                grouped_all[group_key] = {**entry}
            else:
                grouped_all[group_key]["free_orders_to_create"] += entry["free_orders_to_create"]
                grouped_all[group_key]["owed"] = grouped_all[group_key].get("owed", 0) + entry.get("owed", entry["free_orders_to_create"])

        for group_key, e in grouped_all.items():
            is_spend_promo = e["tier"]["min_spend"] > 0
            count = e["free_orders_to_create"]
            gift_qty_per_order = e["tier"]["gift_qty"]
            total_gift_qty = gift_qty_per_order * count
            gift_pids = e["tier"]["gift_product_ids"]
            gift_names = ", ".join(gift_catalog.get(pid) or pid for pid in gift_pids)
            first_gift_title = gift_catalog.get(gift_pids[0], "") if gift_pids else ""
            gift_sku = first_gift_title.split()[0] if first_gift_title else ", ".join(gift_pids)
            session_date = (e["tier"].get("start_at") or "")[:10]
            tkey = e["tracker_key"]
            row_type = "gift" if is_spend_promo else "promotion"
            amount_val = f"HK${e['total_amount']:.0f}" if is_spend_promo else e["total_qty"]
            owed_count = e.get("owed", count)
            applied = sum(e.get("gift_applied_counts", {}).get(pid, 0) for pid in gift_pids)
            to_add = max(0, owed_count - applied)
            unfiltered_rows.append([
                session_date,
                e["customer_id"],
                e["customer_name"],
                e["customer_phone"],
                amount_val,
                gift_sku,
                gift_names,
                owed_count, # 應計 — total earned (not reduced by tracker)
                applied,    # 已計 — auto-applied by Shopline
                to_add,     # 補回 — needs manual give (應計 - 已計)
                row_type,
                e["tier"]["title"],
                ", ".join(e.get("order_numbers", [])),
            ])

        # Batch write everything — separate tabs for promos and gifts, plus combined
        log.info(
            f"Batch writing {len(promo_rows)} promo rows, {len(gift_rows)} gift rows, "
            f"{len(unfiltered_rows)} unfiltered rows, {len(tracker_updates)} tracker updates..."
        )
        batch_write_qualified(promo_ws, promo_rows)
        batch_write_qualified(gift_ws, gift_rows)
        batch_write_qualified(unfiltered_ws, unfiltered_rows)
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
