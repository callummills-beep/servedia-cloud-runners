#!/usr/bin/env python3
"""
Cloud-only single-file Windsor pipeline.

Designed to be embedded verbatim in a Claude Code RemoteTrigger prompt
(no local files, no module imports beyond stdlib). Reads secrets from
environment variables when present, otherwise from the well-known
~/.config/servedia/ paths (for local testing parity).

Does in one shot:
  1. Fetch Windsor.ai facebook ad data (both workspaces) for an adaptive
     date window: max(last_1d, gap+1 if MAX("Date") in Ads Data is behind,
     30 cap).
  2. Upsert into public."Ads Data" via Supabase PostgREST on (Ad ID, Date).
  3. Fetch Hub active+paused Client Management rows for audit.
  4. Compute audit (ok / add / missing_id / unmatched) + upsert
     windsor_sync_audit append-only.
  5. Send HTML summary email to callum.mills@servedia.co via Gmail API.

No Playwright. The Windsor SYNC (checking accounts in the dashboard) stays
local on launchd — only daily-critical because of new client onboarding,
which is a separate manual or weekly job.

Retry policy: 3 attempts with exponential backoff (5s/15s/45s) on transient
network errors + HTTP 429/5xx. 4xx + RuntimeErrors propagate.
"""

from __future__ import annotations

import base64
import json
import os
import socket
import sys
import time
from datetime import date, datetime, timedelta, timezone
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

# ---------- Config ----------------------------------------------------------

SUPABASE_URL = "https://mvhqcfifnppxryfepspb.supabase.co"
SUPABASE_TABLE = "Ads Data"
CONFLICT_COLS = "Ad ID,Date"
WINDSOR_BASE = "https://connectors.windsor.ai/facebook"
WINDSOR_FIELDS = [
    "date", "account_id", "account_name",
    "campaign_id", "campaign",
    "adset_id", "adset_name",
    "ad_id", "ad_name",
    "spend", "actions_lead", "link_clicks",
]
MAX_ROWS = 50000
RECIPIENT = "callum.mills@servedia.co"
HUB_ACTIVE_STATUSES = ("Active (First Contract)", "Active (Past Initial Contract)", "Paused")
USER_AGENT = "servedia-windsor-cloud/1.0"


# ---------- Secrets loader -------------------------------------------------

def load_secrets() -> dict:
    """Load secrets. Cloud path: single env var SECRETS_B64 = base64 of a JSON
    dict {windsor_keys:[...], supabase_svc:..., google_token:{...}}.
    Local dev path: read from ~/.config/servedia/ files."""
    env = os.environ
    if b64 := env.get("SECRETS_B64"):
        return json.loads(base64.b64decode(b64).decode("utf-8"))

    config_dir = Path.home() / ".config" / "servedia"
    return {
        "windsor_keys": [
            (config_dir / "windsor-api-key-1").read_text().strip(),
            (config_dir / "windsor-api-key-2").read_text().strip(),
        ],
        "supabase_svc": (config_dir / "supabase-hub-service-role-key").read_text().strip(),
        "google_token": json.loads((config_dir / "google-oauth-token.json").read_text()),
    }


# ---------- Retry helper ---------------------------------------------------

_RETRYABLE_EXCEPTIONS = (ConnectionResetError, ConnectionError, socket.timeout, TimeoutError)
_RETRYABLE_HTTP_CODES = {429, 500, 502, 503, 504}


def with_retry(fn: Callable, *, attempts: int = 3, base_backoff: float = 5.0, label: str = "http"):
    last_exc: BaseException | None = None
    for i in range(attempts):
        try:
            return fn()
        except HTTPError as e:
            if e.code not in _RETRYABLE_HTTP_CODES:
                raise
            last_exc = e
        except URLError as e:
            reason = getattr(e, "reason", None)
            if not (isinstance(reason, _RETRYABLE_EXCEPTIONS) or "timed out" in str(reason).lower()):
                raise
            last_exc = e
        except _RETRYABLE_EXCEPTIONS as e:
            last_exc = e
        if i == attempts - 1:
            break
        wait = base_backoff * (3 ** i)
        print(f"[retry] {label}: {type(last_exc).__name__}: {last_exc} -> sleep {wait:.0f}s "
              f"then attempt {i + 2}/{attempts}", file=sys.stderr)
        time.sleep(wait)
    assert last_exc is not None
    raise last_exc


def http_json(req: Request, timeout: int = 300) -> Any:
    def _do() -> Any:
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            return json.loads(body.decode("utf-8")) if body else None
    return with_retry(_do, label=req.full_url[:60])


# ---------- Step 1: pick window ---------------------------------------------

def fetch_latest_ads_date(svc_key: str) -> date | None:
    url = (f"{SUPABASE_URL}/rest/v1/{quote(SUPABASE_TABLE)}"
           f"?select=Date&order=Date.desc&limit=1")
    req = Request(url, headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}",
                                "User-Agent": USER_AGENT})
    rows = http_json(req, timeout=30)
    if not rows:
        return None
    try:
        return date.fromisoformat(rows[0]["Date"])
    except Exception:
        return None


def choose_window(svc_key: str) -> tuple[int, str]:
    latest = fetch_latest_ads_date(svc_key)
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    if latest is None:
        return 30, "Ads Data is empty - full 30-day backfill"
    gap = (yesterday - latest).days
    if gap <= 0:
        return 1, f"caught up (latest {latest} = yesterday)"
    n = min(30, gap + 1)
    return n, f"GAP DETECTED ({gap} day(s) behind latest {latest})"


# ---------- Step 2: Windsor pull -------------------------------------------

def pull_windsor_workspace(api_key: str, date_preset: str) -> list[dict]:
    params = {
        "api_key": api_key,
        "date_preset": date_preset,
        "fields": ",".join(WINDSOR_FIELDS),
        "_max_rows": str(MAX_ROWS),
        "_renderer": "json",
    }
    url = f"{WINDSOR_BASE}?{urlencode(params)}"
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        payload = http_json(req, timeout=300)
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            raise RuntimeError(f"Windsor HTTP {e.code}: {body[:300]}") from e
        if "No facebook account" in str(parsed.get("error", "")):
            return []
        raise RuntimeError(f"Windsor HTTP {e.code}: {parsed.get('error', body[:300])}") from e
    if isinstance(payload, dict) and payload.get("error"):
        if "No facebook account" in payload["error"]:
            return []
        raise RuntimeError(f"Windsor API error: {payload['error']}")
    return payload.get("data", []) if isinstance(payload, dict) else []


def to_supabase_payload(rows: list[dict]) -> list[dict]:
    out = []
    for r in rows:
        ad_id = r.get("ad_id")
        d = r.get("date")
        if not ad_id or not d:
            continue
        out.append({
            "Date": str(d),
            "ad_account_id": str(r.get("account_id") or "").lstrip("act_"),
            "Campaign ID": str(r["campaign_id"]) if r.get("campaign_id") is not None else None,
            "Campaign": r.get("campaign"),
            "AdSet ID": str(r["adset_id"]) if r.get("adset_id") is not None else None,
            "AdSet Name": r.get("adset_name"),
            "Ad ID": str(ad_id),
            "Ad Name": r.get("ad_name"),
            "Amount Spent": f"{float(r.get('spend') or 0.0):.2f}",
            "Leads": int(r.get("actions_lead") or 0),
            "Link Clicks": int(float(r.get("link_clicks") or 0)),
        })
    # Dedupe by (Ad ID, Date) - Windsor sometimes overlaps on day-of-today
    by_key: dict[tuple[str, str], dict] = {(r["Ad ID"], r["Date"]): r for r in out}
    return list(by_key.values())


def upsert_ads_data(rows: list[dict], svc_key: str, batch: int = 1000) -> int:
    table_enc = quote(SUPABASE_TABLE)
    conflict = quote(CONFLICT_COLS)
    url = f"{SUPABASE_URL}/rest/v1/{table_enc}?on_conflict={conflict}"
    headers = {
        "apikey": svc_key,
        "Authorization": f"Bearer {svc_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
        "User-Agent": USER_AGENT,
    }
    total = 0
    for i in range(0, len(rows), batch):
        chunk = rows[i:i + batch]
        body = json.dumps(chunk).encode("utf-8")

        def _do() -> None:
            req = Request(url, data=body, headers=headers, method="POST")
            with urlopen(req, timeout=60) as resp:
                _ = resp.read()

        try:
            with_retry(_do, label=f"supabase upsert chunk {i // batch + 1}")
            total += len(chunk)
        except HTTPError as e:
            err_body = e.read().decode("utf-8", errors="replace")[:500]
            raise RuntimeError(f"Supabase HTTP {e.code}: {err_body}") from e
    return total


# ---------- Step 3: Audit ---------------------------------------------------

def fetch_hub_active(svc_key: str) -> list[dict]:
    table = quote("Client Management")
    fields = quote('"Client Name",mb_ad_account_id,"Status",ad_status,ad_account_name')
    in_clause = "(" + ",".join(f'"{s}"' for s in HUB_ACTIVE_STATUSES) + ")"
    url = (f"{SUPABASE_URL}/rest/v1/{table}?select={fields}"
           f"&Status=in.{quote(in_clause, safe='()')}")
    req = Request(url, headers={
        "apikey": svc_key, "Authorization": f"Bearer {svc_key}", "User-Agent": USER_AGENT,
    })
    return http_json(req, timeout=30)


def fetch_windsor_connections(keys: list[str]) -> list[dict]:
    out = []
    for i, key in enumerate(keys, start=1):
        url = (f"https://onboard.windsor.ai/api/common/ds-accounts"
               f"?{urlencode({'datasource': 'all', 'api_key': key})}")
        req = Request(url, headers={"User-Agent": USER_AGENT})
        try:
            data = http_json(req, timeout=30) or []
        except HTTPError as e:
            print(f"[audit] ds-accounts wkspace {i} failed: HTTP {e.code}", file=sys.stderr)
            continue
        for x in data:
            out.append({
                "workspace": f"windsor-{i}",
                "account_id": str(x.get("account_id", "")).replace("facebook__", "").replace("act_", "").strip(),
                "account_name": x.get("account_name"),
                "datasource": x.get("datasource", "unknown"),
            })
    return out


def compute_audit_rows(windsor: list[dict], hub_active: list[dict]) -> list[dict]:
    """Active-only diff. Churned not consulted (active-only policy)."""
    hub_by_id = {}
    for h in hub_active:
        acc = (h.get("mb_ad_account_id") or "").replace("act_", "").strip()
        if acc:
            hub_by_id[acc] = h

    rows = []
    workspaces_seen = sorted({w["workspace"] for w in windsor}) or ["windsor-1"]

    # Pass 1: every Windsor connection -> ok / unmatched
    for w in windsor:
        h = hub_by_id.get(w["account_id"])
        if h:
            rows.append({
                "workspace": w["workspace"], "action": "ok",
                "ad_account_id": w["account_id"], "ad_account_name": w["account_name"],
                "client_name": h.get("Client Name"), "client_status": h.get("Status"),
                "hub_ad_status": h.get("ad_status"), "notes": None,
            })
        else:
            rows.append({
                "workspace": w["workspace"], "action": "unmatched",
                "ad_account_id": w["account_id"], "ad_account_name": w["account_name"],
                "client_name": None, "client_status": None, "hub_ad_status": None,
                "notes": "In Windsor, no active-Hub match (could be churned, repurposed, secondary, or backup).",
            })

    # Pass 2: every active Hub client not in any Windsor -> add
    all_w = {w["account_id"] for w in windsor}
    for h in hub_active:
        acc = (h.get("mb_ad_account_id") or "").replace("act_", "").strip()
        if acc and acc not in all_w:
            rows.append({
                "workspace": workspaces_seen[0], "action": "add",
                "ad_account_id": acc, "ad_account_name": None,
                "client_name": h.get("Client Name"), "client_status": h.get("Status"),
                "hub_ad_status": h.get("ad_status"),
                "notes": "Active in Hub but not connected to any Windsor workspace.",
            })

    # Pass 3: active Hub clients with no mb_ad_account_id -> missing_id
    for h in hub_active:
        acc = (h.get("mb_ad_account_id") or "").strip()
        if not acc:
            rows.append({
                "workspace": "-", "action": "missing_id",
                "ad_account_id": "-", "ad_account_name": h.get("ad_account_name"),
                "client_name": h.get("Client Name"), "client_status": h.get("Status"),
                "hub_ad_status": h.get("ad_status"),
                "notes": "Hub profile missing mb_ad_account_id - fill in to enable Windsor sync.",
            })

    return rows


def insert_audit(rows: list[dict], svc_key: str) -> int:
    url = f"{SUPABASE_URL}/rest/v1/windsor_sync_audit"
    headers = {
        "apikey": svc_key, "Authorization": f"Bearer {svc_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal", "User-Agent": USER_AGENT,
    }
    BATCH = 500
    total = 0
    for i in range(0, len(rows), BATCH):
        chunk = rows[i:i + BATCH]
        body = json.dumps(chunk).encode("utf-8")

        def _do() -> None:
            req = Request(url, data=body, headers=headers, method="POST")
            with urlopen(req, timeout=30) as resp:
                _ = resp.read()

        try:
            with_retry(_do, label=f"audit insert chunk {i // BATCH + 1}")
            total += len(chunk)
        except HTTPError as e:
            err = e.read().decode("utf-8", errors="replace")[:500]
            raise RuntimeError(f"audit insert HTTP {e.code}: {err}") from e
    return total


# ---------- Step 4: Email ---------------------------------------------------

def gmail_access_token(tok: dict) -> str:
    body = urlencode({
        "client_id": tok["client_id"], "client_secret": tok["client_secret"],
        "refresh_token": tok["refresh_token"], "grant_type": "refresh_token",
    }).encode("utf-8")
    req = Request(tok["token_uri"], data=body,
                  headers={"Content-Type": "application/x-www-form-urlencoded",
                           "User-Agent": USER_AGENT},
                  method="POST")
    resp = http_json(req, timeout=20)
    return resp["access_token"]


def build_html(audit_rows: list[dict], pull_summary: dict) -> tuple[str, str]:
    from collections import Counter
    by_action = Counter(r["action"] for r in audit_rows)
    summary_line = " | ".join(f"{by_action.get(k, 0)} {k}"
                              for k in ("ok", "add", "missing_id", "unmatched"))
    date_str = datetime.now(timezone.utc).date().isoformat()
    subject = (f"[CLOUD] Windsor <-> Hub audit - {date_str} - {summary_line} "
               f"(pull: {pull_summary.get('rows_upserted', 0)} rows, "
               f"window {pull_summary.get('window', '?')})")

    def section(title: str, action: str) -> str:
        items = [r for r in audit_rows if r["action"] == action]
        if not items:
            return f"<h3>{title}: none</h3>"
        body = "<table border='1' cellpadding='6' cellspacing='0' style='border-collapse:collapse;font-size:13px'>"
        body += "<tr><th>Client</th><th>Status</th><th>Ad account ID</th><th>Notes</th></tr>"
        for r in items:
            body += "<tr>"
            body += f"<td>{(r['client_name'] or r['ad_account_name'] or '-')}</td>"
            body += f"<td>{(r['client_status'] or '')}</td>"
            body += f"<td><code>{r['ad_account_id']}</code></td>"
            body += f"<td>{(r['notes'] or '')[:140]}</td>"
            body += "</tr>"
        body += "</table>"
        return f"<h3>{title}: {len(items)}</h3>{body}"

    html = f"""<html><body style="font-family:system-ui,-apple-system,sans-serif;color:#222">
<p><b>{summary_line}</b> &middot; {date_str} &middot; Pull: {pull_summary.get('rows_upserted', 0)} rows
across {pull_summary.get('distinct_accounts', 0)} accounts ({pull_summary.get('window', '?')}).</p>
{section("[MISSING ID] active client, no mb_ad_account_id in Hub", "missing_id")}
{section("[ADD] active in Hub, not in Windsor - needs Grant Facebook Ads Access", "add")}
{section("[UNMATCHED] in Windsor, no active-Hub match - informational only", "unmatched")}
<p style="color:#888;font-size:12px;margin-top:24px">
Running in cloud via RemoteTrigger (no local Mac needed). Active-only policy: never uncheck.<br>
Source of truth: <code>public.windsor_sync_audit</code> (Supabase mvhqcfifnppxryfepspb).
</p>
</body></html>"""
    return subject, html


def send_email(subject: str, html: str, tok: dict) -> None:
    access = gmail_access_token(tok)
    msg = MIMEText(html, "html")
    msg["To"] = RECIPIENT
    msg["From"] = RECIPIENT
    msg["Subject"] = subject
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    payload = json.dumps({"raw": raw}).encode("utf-8")
    req = Request("https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
                  data=payload,
                  headers={"Authorization": f"Bearer {access}",
                           "Content-Type": "application/json", "User-Agent": USER_AGENT},
                  method="POST")

    def _do() -> None:
        with urlopen(req, timeout=30) as resp:
            _ = resp.read()
    try:
        with_retry(_do, label="gmail send")
    except HTTPError as e:
        raise RuntimeError(f"Gmail send HTTP {e.code}: "
                           f"{e.read().decode('utf-8', 'replace')[:400]}") from e


# ---------- Main -----------------------------------------------------------

def main() -> int:
    started = time.time()
    print(f"[cloud] === Windsor cloud pipeline start @ "
          f"{datetime.now(timezone.utc).isoformat(timespec='seconds')} ===")

    secrets = load_secrets()
    svc_key = secrets["supabase_svc"]

    # Step 1: pick adaptive window
    n_days, reason = choose_window(svc_key)
    preset = f"last_{n_days}d"
    print(f"[cloud] window: {preset} - {reason}")

    # Step 2: pull both Windsor workspaces
    workspace_counts: dict[str, int] = {}
    all_raw: list[dict] = []
    for i, k in enumerate(secrets["windsor_keys"], start=1):
        try:
            rows = pull_windsor_workspace(k, preset)
        except RuntimeError as e:
            print(f"[cloud] FATAL windsor-{i}: {e}", file=sys.stderr)
            return 2
        workspace_counts[f"windsor-{i}"] = len(rows)
        all_raw.extend(rows)
    print(f"[cloud] windsor workspaces: {workspace_counts}")

    payload = to_supabase_payload(all_raw)
    if payload:
        distinct_accounts = len({r["ad_account_id"] for r in payload})
        dates = sorted({r["Date"] for r in payload})
        upserted = upsert_ads_data(payload, svc_key)
        print(f"[cloud] upserted {upserted} rows; "
              f"accounts={distinct_accounts}; "
              f"date_range=[{dates[0]}..{dates[-1]}]")
    else:
        distinct_accounts = 0
        upserted = 0
        print("[cloud] no rows to upsert")

    pull_summary = {
        "rows_upserted": upserted,
        "distinct_accounts": distinct_accounts,
        "window": preset,
        "workspaces": workspace_counts,
    }

    # Step 3: audit
    try:
        windsor_conns = fetch_windsor_connections(secrets["windsor_keys"])
        hub_active = fetch_hub_active(svc_key)
        audit_rows = compute_audit_rows(windsor_conns, hub_active)
        inserted = insert_audit(audit_rows, svc_key)
        from collections import Counter
        action_counts = Counter(r["action"] for r in audit_rows)
        print(f"[cloud] audit: {dict(action_counts)} ({inserted} rows inserted)")
    except Exception as e:
        print(f"[cloud] audit failed (non-fatal): {e}", file=sys.stderr)
        audit_rows = []

    # Step 4: email
    if audit_rows:
        try:
            subject, html = build_html(audit_rows, pull_summary)
            send_email(subject, html, secrets["google_token"])
            print(f"[cloud] email sent: {subject}")
        except Exception as e:
            print(f"[cloud] email failed (non-fatal): {e}", file=sys.stderr)

    elapsed = time.time() - started
    print(f"[cloud] === done in {elapsed:.1f}s ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
