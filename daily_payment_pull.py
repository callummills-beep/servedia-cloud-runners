#!/usr/bin/env python3
"""
daily_payment_pull.py — Servedia Payment Pipeline (OpenClaw VPS edition)
Pulls Whop + Fanbasis + Stripe → Supabase payment_logs / deposit_tracker / stripe_patient_deposits_daily
Pure stdlib — no pip installs needed.
"""
import base64
import json
import os
import sys
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ──────────────────────────────────────────────
# 0. Load credentials
# ──────────────────────────────────────────────

CFG = Path.home() / ".config" / "servedia"

def load_secrets():
    secrets_file = CFG / "payments-secrets-b64.txt"
    if not secrets_file.exists():
        fatal(f"secrets file not found: {secrets_file}")
    raw = secrets_file.read_text().strip()
    return json.loads(base64.b64decode(raw).decode())

def fatal(msg):
    print(f"[payments] FATAL: {msg}", file=sys.stderr)
    sys.exit(1)

def log(msg):
    print(f"[payments] {msg}", flush=True)

# ──────────────────────────────────────────────
# 1. HTTP helpers
# ──────────────────────────────────────────────

def http_get(url, headers=None, timeout=30):
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())

def http_post(url, data, headers=None, timeout=30):
    body = json.dumps(data).encode()
    h = {"Content-Type": "application/json"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, data=body, headers=h, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())

# ──────────────────────────────────────────────
# 2. Supabase SQL helper
# ──────────────────────────────────────────────

SUPABASE_PROJECT = "mvhqcfifnppxryfepspb"

def supabase_sql(sql, supabase_pat):
    url = f"https://api.supabase.com/v1/projects/{SUPABASE_PROJECT}/database/query"
    headers = {
        "Authorization": f"Bearer {supabase_pat}",
        "Content-Type": "application/json",
        "User-Agent": "servedia-openclaw-payment-pull/1.0",
        "Accept": "application/json",
    }
    req = urllib.request.Request(url, data=json.dumps({"query": sql}).encode(), headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        fatal(f"Supabase SQL error {e.code}: {body[:500]}")

# ──────────────────────────────────────────────
# 3. Whop pull
# ──────────────────────────────────────────────

COMPANIES = [
    {"name": "servedia",         "biz": "biz_kfkMqL5kyH1Zy3", "offer": "Servedia",       "key_field": "whop_servedia_key"},
    {"name": "servedia-limited", "biz": "biz_P1wRlQcV9zfgCu", "offer": "Servedia",       "key_field": "whop_servedia_limited_key"},
    {"name": "telemed-studios",  "biz": "biz_Fs5O7voBaTJj1I", "offer": "Telemed Studios", "key_field": "whop_telemed_key"},
]

SKIP_TITLES = {"Update Card Info", "Update Card Details"}

def whop_paginate(endpoint, api_key, params=""):
    results = []
    page = 1
    while True:
        sep = "&" if params else "?"
        url = f"https://api.whop.com{endpoint}?per=100&page={page}{sep}{params}"
        try:
            data = http_get(url, headers={"Authorization": f"Bearer {api_key}"})
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            fatal(f"Whop GET {endpoint} page {page}: {e.code} {body[:300]}")
        items = data.get("data", [])
        results.extend(items)
        meta = data.get("pagination", {})
        if not meta.get("next_page"):
            break
        page += 1
    return results

def whop_disputes_paginate(biz_id, api_key):
    results = []
    cursor = None
    while True:
        params = f"company_id={biz_id}&first=100"
        if cursor:
            params += f"&after={cursor}"
        url = f"https://api.whop.com/api/v1/disputes?{params}"
        try:
            data = http_get(url, headers={"Authorization": f"Bearer {api_key}"})
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            fatal(f"Whop disputes {biz_id}: {e.code} {body[:300]}")
        nodes = data.get("edges", [])
        results.extend([n.get("node", n) for n in nodes])
        page_info = data.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")
    return results

def build_whop_rows(secrets):
    rows = []

    for co in COMPANIES:
        api_key = secrets[co["key_field"]]
        biz_id = co["biz"]
        source = f"whop-{co['name']}"

        # Fetch products for title lookup
        products_raw = whop_paginate("/api/v2/products", api_key)
        product_map = {p["id"]: p.get("name", "") for p in products_raw}

        # Fetch memberships for email lookup
        memberships_raw = whop_paginate("/api/v2/memberships", api_key)
        membership_map = {m["id"]: m for m in memberships_raw}

        # Fetch payments
        payments_raw = whop_paginate("/api/v2/payments", api_key)
        for p in payments_raw:
            if p.get("status") != "paid":
                continue
            if not p.get("paid_at"):
                continue

            product_id = p.get("plan_id") or p.get("product_id") or ""
            product_title = product_map.get(product_id, p.get("product_title", ""))

            if product_title in SKIP_TITLES:
                continue

            # Classify
            if product_title == "Ad Spend":
                ptype = "ad spend re-billing"
            elif p.get("billing_reason") == "renewal":
                ptype = "backend"
            else:
                ptype = "frontend"

            # Email resolution
            membership_id = p.get("membership_id", "")
            membership = membership_map.get(membership_id, {})
            email = membership.get("email") or membership.get("user", {}).get("email", "")
            if not email:
                user = membership.get("user") or {}
                fn = (user.get("first_name") or "").lower().strip()
                ln = (user.get("last_name") or "").lower().strip()
                if fn or ln:
                    email = f"unknown-{fn}{ln}@no-email.local"
                else:
                    email = f"unknown-{p['id']}@no-email.local"

            paid_at = p.get("paid_at")
            if isinstance(paid_at, (int, float)):
                payment_date = datetime.fromtimestamp(paid_at, tz=timezone.utc).date().isoformat()
            else:
                payment_date = str(paid_at)[:10]

            rows.append({
                "source": source,
                "source_payment_id": p["id"],
                "payment_type": ptype,
                "payment_amount": float(p.get("final_amount", p.get("amount", 0)) or 0) / 100,
                "payment_date": payment_date,
                "refund_dispute_date": None,
                "client_email": email,
                "product_name": product_title,
                "offer_name": co["offer"],
                "notes": None,
            })

        # Disputes
        disputes_raw = whop_disputes_paginate(biz_id, api_key)
        for d in disputes_raw:
            email = d.get("buyer_email") or f"unknown-{d.get('id', 'x')}@no-email.local"
            dispute_date = (d.get("created_at") or "")[:10] or None
            rows.append({
                "source": source,
                "source_payment_id": d.get("id"),
                "payment_type": "dispute",
                "payment_amount": float(d.get("amount", 0) or 0) / 100,
                "payment_date": None,
                "refund_dispute_date": dispute_date,
                "client_email": email,
                "product_name": None,
                "offer_name": co["offer"],
                "notes": f"dispute status={d.get('status')}",
            })

    return rows

# ──────────────────────────────────────────────
# 4. Fanbasis pull
# ──────────────────────────────────────────────

def build_fanbasis_rows(secrets):
    api_key = secrets["fanbasis_key"]
    rows = []
    page = 1
    while True:
        url = f"https://www.fanbasis.com/public-api/checkout-sessions/transactions?page={page}&per_page=100"
        try:
            data = http_get(url, headers={"x-api-key": api_key})
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            fatal(f"Fanbasis page {page}: {e.code} {body[:300]}")
        transactions = data if isinstance(data, list) else data.get("data", data.get("transactions", []))
        if not transactions:
            break
        for t in transactions:
            fan = t.get("fan") or {}
            email = fan.get("email") or f"unknown-fb-{t.get('id','x')}@no-email.local"
            transaction_date = (t.get("transaction_date") or "")[:10]
            amount = float(t.get("amount", 0) or 0)

            rows.append({
                "source": "fanbasis",
                "source_payment_id": str(t.get("id", "")),
                "payment_type": "frontend",
                "payment_amount": amount,
                "payment_date": transaction_date,
                "refund_dispute_date": None,
                "client_email": email,
                "product_name": t.get("product_name") or t.get("offer_name"),
                "offer_name": "Fanbasis",
                "notes": None,
            })

            # Refunds
            for r in (t.get("refunds") or []):
                refund_date = (r.get("refund_date") or r.get("created_at") or "")[:10]
                rows.append({
                    "source": "fanbasis",
                    "source_payment_id": str(r.get("id", f"ref-{t.get('id')}")),
                    "payment_type": "refund",
                    "payment_amount": float(r.get("amount", 0) or 0),
                    "payment_date": transaction_date,
                    "refund_dispute_date": refund_date or None,
                    "client_email": email,
                    "product_name": t.get("product_name") or t.get("offer_name"),
                    "offer_name": "Fanbasis",
                    "notes": None,
                })

        # Pagination check
        if isinstance(data, list) or len(transactions) < 100:
            break
        page += 1

    return rows

# ──────────────────────────────────────────────
# 5. Upsert payment_logs in batches
# ──────────────────────────────────────────────

COLS = ["source", "source_payment_id", "payment_type", "payment_amount",
        "payment_date", "refund_dispute_date", "client_email", "product_name", "offer_name", "notes"]

def escape_sql(v):
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"

def upsert_payment_logs(rows, supabase_pat):
    if not rows:
        return 0
    total_inserted = 0
    batch_size = 200
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        values_parts = []
        for r in batch:
            vals = ", ".join(escape_sql(r.get(c)) for c in COLS)
            values_parts.append(f"({vals})")
        col_list = ", ".join(COLS)
        values_sql = ",\n".join(values_parts)
        sql = f"""
WITH ins AS (
  INSERT INTO public.payment_logs ({col_list})
  VALUES {values_sql}
  ON CONFLICT (source, source_payment_id, payment_type)
  WHERE source_payment_id IS NOT NULL
  DO NOTHING
  RETURNING id
)
SELECT COUNT(*) AS inserted FROM ins;
"""
        result = supabase_sql(sql, supabase_pat)
        inserted = 0
        if result:
            row = result[0] if isinstance(result, list) else result
            inserted = int(row.get("inserted", 0))
        total_inserted += inserted
    return total_inserted

# ──────────────────────────────────────────────
# 6. Rebuild deposit_tracker
# ──────────────────────────────────────────────

def rebuild_deposit_tracker(supabase_pat):
    sql = """
INSERT INTO public.deposit_tracker (date, total_deposit_revenue, total_deposits_refunded)
SELECT d.day::date,
  COALESCE(SUM(CASE WHEN payment_type IN ('frontend','backend','upsells','ad spend re-billing') THEN payment_amount ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN payment_type IN ('refund','dispute') THEN payment_amount ELSE 0 END), 0)
FROM (SELECT DISTINCT COALESCE(refund_dispute_date, payment_date) AS day FROM public.payment_logs) d
LEFT JOIN public.payment_logs pl ON COALESCE(pl.refund_dispute_date, pl.payment_date) = d.day
GROUP BY d.day
ON CONFLICT (date) DO UPDATE SET
  total_deposit_revenue = EXCLUDED.total_deposit_revenue,
  total_deposits_refunded = EXCLUDED.total_deposits_refunded,
  updated_at = now();
"""
    supabase_sql(sql, supabase_pat)

# ──────────────────────────────────────────────
# 7. Stripe pull
# ──────────────────────────────────────────────

STRIPE_ACCOUNTS = [
    {"name": "medspa",  "key_field": "stripe_medspa_key"},
    {"name": "agency",  "key_field": "stripe_agency_key"},
]

def stripe_b64_auth(api_key):
    return base64.b64encode(f"{api_key}:".encode()).decode()

def stripe_paginate(endpoint, api_key, extra_params=""):
    results = []
    cursor = None
    auth = stripe_b64_auth(api_key)
    while True:
        url = f"https://api.stripe.com/v1/{endpoint}?limit=100{extra_params}"
        if cursor:
            url += f"&starting_after={cursor}"
        try:
            data = http_get(url, headers={"Authorization": f"Basic {auth}"})
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            fatal(f"Stripe GET {endpoint}: {e.code} {body[:300]}")
        items = data.get("data", [])
        results.extend(items)
        if not data.get("has_more"):
            break
        if items:
            cursor = items[-1]["id"]
    return results

def pull_stripe_yesterday(secrets):
    now_utc = datetime.now(timezone.utc)
    yesterday = now_utc.date() - timedelta(days=1)
    yest_start = int(datetime(yesterday.year, yesterday.month, yesterday.day, tzinfo=timezone.utc).timestamp())
    yest_end   = yest_start + 86400
    date_str = yesterday.isoformat()

    summary = {}
    for acct in STRIPE_ACCOUNTS:
        api_key = secrets[acct["key_field"]]
        name = acct["name"]

        time_params = f"&created[gte]={yest_start}&created[lt]={yest_end}"

        charges  = stripe_paginate("charges",  api_key, time_params)
        refunds  = stripe_paginate("refunds",  api_key, time_params)
        disputes = stripe_paginate("disputes", api_key, time_params)

        gross    = sum(c.get("amount", 0) for c in charges  if c.get("status") == "succeeded") / 100
        ref_amt  = sum(r.get("amount", 0) for r in refunds  if r.get("status") == "succeeded") / 100
        disp_amt = sum(d.get("amount", 0) for d in disputes) / 100
        ch_count = len([c for c in charges if c.get("status") == "succeeded"])
        rf_count = len([r for r in refunds if r.get("status") == "succeeded"])

        summary[name] = {
            "gross": gross, "refunds": ref_amt, "disputes": disp_amt,
            "charges_count": ch_count, "refunds_count": rf_count,
        }

    return date_str, summary

def upsert_stripe_daily(date_str, summary, supabase_pat):
    m = summary.get("medspa", {})
    a = summary.get("agency", {})
    sql = f"""
INSERT INTO public.stripe_patient_deposits_daily
  (date, medspa_gross, medspa_refunds, medspa_disputes, agency_gross, agency_refunds, agency_disputes, charges_count, refunds_count)
VALUES (
  '{date_str}',
  {m.get('gross', 0)}, {m.get('refunds', 0)}, {m.get('disputes', 0)},
  {a.get('gross', 0)}, {a.get('refunds', 0)}, {a.get('disputes', 0)},
  {m.get('charges_count', 0) + a.get('charges_count', 0)},
  {m.get('refunds_count', 0) + a.get('refunds_count', 0)}
)
ON CONFLICT (date) DO UPDATE SET
  medspa_gross = EXCLUDED.medspa_gross,
  medspa_refunds = EXCLUDED.medspa_refunds,
  medspa_disputes = EXCLUDED.medspa_disputes,
  agency_gross = EXCLUDED.agency_gross,
  agency_refunds = EXCLUDED.agency_refunds,
  agency_disputes = EXCLUDED.agency_disputes,
  charges_count = EXCLUDED.charges_count,
  refunds_count = EXCLUDED.refunds_count,
  updated_at = now();
"""
    supabase_sql(sql, supabase_pat)

# ──────────────────────────────────────────────
# 8. Main
# ──────────────────────────────────────────────

def main():
    log(f"=== payments pipeline start @ {datetime.now(timezone.utc).isoformat()} ===")

    secrets = load_secrets()
    supabase_pat = secrets["supabase_pat"]

    # ── Whop ──
    try:
        whop_rows = build_whop_rows(secrets)
    except SystemExit:
        raise
    except Exception as e:
        fatal(f"whop pull failed: {e}")
    log(f"whop: {len(whop_rows)} rows from 3 companies")

    # ── Fanbasis ──
    try:
        fb_rows = build_fanbasis_rows(secrets)
    except SystemExit:
        raise
    except Exception as e:
        fatal(f"fanbasis pull failed: {e}")
    log(f"fanbasis: {len(fb_rows)} rows")

    # ── Upsert payment_logs ──
    all_rows = whop_rows + fb_rows
    try:
        inserted = upsert_payment_logs(all_rows, supabase_pat)
    except SystemExit:
        raise
    except Exception as e:
        fatal(f"payment_logs upsert failed: {e}")
    log(f"payment_logs upsert: {inserted} new rows inserted")

    # ── Rebuild deposit_tracker ──
    try:
        rebuild_deposit_tracker(supabase_pat)
    except SystemExit:
        raise
    except Exception as e:
        fatal(f"deposit_tracker rebuild failed: {e}")
    log("deposit_tracker rebuilt")

    # ── Stripe ──
    try:
        date_str, stripe_summary = pull_stripe_yesterday(secrets)
    except SystemExit:
        raise
    except Exception as e:
        fatal(f"stripe pull failed: {e}")

    m = stripe_summary.get("medspa", {})
    a = stripe_summary.get("agency", {})
    log(f"stripe: medspa=${m.get('gross', 0):.2f} refunds=${m.get('refunds', 0):.2f}  agency=${a.get('gross', 0):.2f} refunds=${a.get('refunds', 0):.2f}")

    try:
        upsert_stripe_daily(date_str, stripe_summary, supabase_pat)
    except SystemExit:
        raise
    except Exception as e:
        fatal(f"stripe upsert failed: {e}")
    log(f"stripe_patient_deposits_daily upserted for {date_str}")

    log("=== done ===")

if __name__ == "__main__":
    main()
