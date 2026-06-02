#!/usr/bin/env python3
"""
Hyros B2B Performance Sync — daily canonical pull.
VPS / OpenClaw edition — reads all credentials from ~/.config/servedia/

How it works:
  Playwright persistent context navigates to Hyros /reporting/performance/scientific,
  intercepts the sourceboardV2/table API response (canonical Scientific attribution),
  clicks Today→Yesterday for single-day data, upserts to Supabase.

Session: H-REFRESH-TOKEN-PROD cookie lasts ~30 days. On expiry: auto-login via
  email+password + 6-char hex Gmail verification code + "Trust device 30 days".

Credential files (written by bootstrap):
  ~/.config/servedia/supabase-hub-service-role-key
  ~/.config/servedia/google-oauth-token.json
  ~/.config/servedia/google-oauth-client.json
  ~/.config/servedia/hyros-email
  ~/.config/servedia/hyros-password

Usage:
  python3 hyros_b2b_pull.py              # yesterday in Europe/Zurich
  python3 hyros_b2b_pull.py 2026-05-30   # specific date (YYYY-MM-DD)
  python3 hyros_b2b_pull.py --dry-run    # print without upserting
"""

from __future__ import annotations
import argparse, asyncio, base64, json, os, re, sys, time, urllib.request, urllib.error
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# ── Credential paths ───────────────────────────────────────────────────────────
CFG          = Path.home() / ".config/servedia"
PROFILE_DIR  = CFG / "hyros-browser-data"
SUPA_KEY     = (CFG / "supabase-hub-service-role-key").read_text().strip()
SUPA_REST    = "https://mvhqcfifnppxryfepspb.supabase.co/rest/v1"
OAUTH_TOKEN  = CFG / "google-oauth-token.json"
OAUTH_CLIENT = CFG / "google-oauth-client.json"
HYROS_EMAIL  = (CFG / "hyros-email").read_text().strip()
HYROS_PASS   = (CFG / "hyros-password").read_text().strip()

METRIC_QL     = 37367   # Stage: Qualified Leads
METRIC_INTROS = 37375   # Stage: Intro Calls Booked

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[hyros] {msg}", flush=True)


def zurich_yesterday() -> date:
    """Yesterday in Europe/Zurich (correct DST via TZ env var)."""
    from subprocess import run
    out = run(
        ["date", "-d", "yesterday", "+%Y-%m-%d"]
        if sys.platform != "darwin"
        else ["/bin/date", "-v-1d", "+%Y-%m-%d"],
        env={**os.environ, "TZ": "Europe/Zurich"},
        capture_output=True, text=True, check=True
    )
    return date.fromisoformat(out.stdout.strip())


def dd_mm_yyyy(d: date) -> str:
    return d.strftime("%d-%m-%Y")


# ── Gmail verification code ────────────────────────────────────────────────────

def get_verification_code(min_epoch: int) -> str | None:
    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
    except ImportError:
        log("WARN: google-auth not installed — cannot read Gmail for verification code")
        return None

    creds_data  = json.loads(OAUTH_TOKEN.read_text())
    client_data = json.loads(OAUTH_CLIENT.read_text())["installed"]
    creds = Credentials(
        token=creds_data["token"], refresh_token=creds_data["refresh_token"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_data["client_id"], client_secret=client_data["client_secret"]
    )
    svc = build("gmail", "v1", credentials=creds)

    def extract_body(payload: dict) -> str:
        if payload.get("body", {}).get("data"):
            return base64.urlsafe_b64decode(payload["body"]["data"] + "==").decode("utf-8", errors="replace")
        for part in payload.get("parts", []):
            t = extract_body(part)
            if t: return t
        return ""

    for attempt in range(18):
        results = svc.users().messages().list(
            userId="me",
            q=f'from:noreply@hyros.com subject:"Verification code" after:{min_epoch}',
            maxResults=3
        ).execute()
        for ref in results.get("messages", []):
            msg  = svc.users().messages().get(userId="me", id=ref["id"], format="full").execute()
            body = extract_body(msg["payload"])
            codes = re.findall(r"authentication code[:\s]+([a-f0-9]{6})\b", body, re.IGNORECASE)
            if not codes:
                codes = re.findall(r":\s*([a-f0-9]{6})\s", body)
            if codes:
                log(f"Gmail: code found (attempt {attempt+1})")
                return codes[0]
        log(f"Gmail: attempt {attempt+1}/18 — waiting 5s…")
        time.sleep(5)
    return None


# ── Playwright pull ────────────────────────────────────────────────────────────

async def pull_day(target: date) -> dict:
    from playwright.async_api import async_playwright

    target_ddmm = dd_mm_yyyy(target)
    log(f"pulling {target.isoformat()} ({target_ddmm})")

    captured: list[dict] = []

    async with async_playwright() as pw:
        ctx = await pw.chromium.launch_persistent_context(
            str(PROFILE_DIR), headless=True,
            viewport={"width": 1400, "height": 900},
            args=["--no-sandbox", "--disable-dev-shm-usage"]  # needed on Linux VPS
        )
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()

        async def on_response(resp):
            if "/sourceboardV2/table" not in resp.url or resp.request.method != "POST":
                return
            try:
                data     = await resp.json()
                req_body = json.loads(resp.request.post_data or "{}")
                req_date = req_body.get("statsRequest", {}).get("start", "")
                total    = next((r for r in data.get("rows", []) if r.get("rowType") == "TOTAL"), None)
                if total and req_date:
                    cm = {x["metricDTO"]["id"]: x["result"]
                          for x in total.get("customMetricResultDTOS", [])}
                    captured.append({
                        "req_date": req_date,
                        "cost":  total.get("cost"),
                        "leads": total.get("leads"),
                        "ql":    cm.get(METRIC_QL),
                        "intros": cm.get(METRIC_INTROS),
                    })
                    log(f"  intercepted req_date={req_date} cost={total.get('cost')} "
                        f"leads={total.get('leads')} ql={cm.get(METRIC_QL)} intros={cm.get(METRIC_INTROS)}")
            except Exception as e:
                log(f"  response parse error: {e}")

        page.on("response", on_response)

        # ── Login if session expired ──────────────────────────────────────────
        await page.goto("https://app.hyros.com/dashboard", wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(2)
        log(f"dashboard URL: {page.url}")

        if "login" in page.url:
            log("session expired — running login flow")
            await page.wait_for_selector('input[name="username"]', timeout=20000)
            await page.fill('input[name="username"]', HYROS_EMAIL)
            await page.fill('input[name="password"]', HYROS_PASS)
            await page.click('button[type="submit"]')
            await asyncio.sleep(3)

            body_text = await page.inner_text("body")
            if "send" in body_text.lower() or "security email" in body_text.lower():
                log("security email page — clicking Send")
                send_ts = int(time.time()) - 60
                await page.click('button:has-text("Send")')
                await asyncio.sleep(2)

                code = get_verification_code(send_ts)
                if not code:
                    raise RuntimeError("could not retrieve Gmail verification code")

                await asyncio.sleep(1)
                first_input = await page.query_selector("input")
                if first_input:
                    await first_input.click()
                    await page.keyboard.type(code, delay=120)
                else:
                    raise RuntimeError("OTP input not found on verification page")

                try:
                    for inp in await page.query_selector_all("input"):
                        if await inp.get_attribute("type") == "checkbox":
                            await inp.check(); break
                except Exception:
                    pass

                await page.click('button:has-text("Continue")')
                await page.wait_for_url("**/dashboard**", timeout=25000)
                log(f"logged in → {page.url}")

        # ── Navigate to performance report ────────────────────────────────────
        log("navigating to performance report…")
        await page.goto(
            "https://app.hyros.com/reporting/performance/scientific",
            wait_until="domcontentloaded", timeout=40000
        )

        for _ in range(12):
            if captured: break
            await asyncio.sleep(1)

        log("forcing Yesterday single-day view…")
        for label in ["Today", "Yesterday"]:
            await page.evaluate(f'''
                () => {{
                    const el = Array.from(document.querySelectorAll("*"))
                        .find(e => e.children.length === 0 && e.textContent.trim() === "{label}");
                    if (el) el.click();
                }}
            ''')
            await asyncio.sleep(3)

        for _ in range(20):
            if any(c["req_date"] == target_ddmm for c in captured):
                break
            await asyncio.sleep(1)

        await ctx.close()

    matches = [c for c in captured if c["req_date"] == target_ddmm]
    if not matches:
        if captured:
            log(f"WARN: no response for {target_ddmm}, using last captured ({captured[-1]['req_date']})")
            matches = [captured[-1]]
        else:
            raise RuntimeError(f"no data captured for {target_ddmm}")

    row = matches[-1]
    return {
        "date":            target.isoformat(),
        "ad_spend":        round(float(row["cost"] or 0), 2),
        "leads":           int(row["leads"] or 0),
        "qualified_leads": int(row["ql"] or 0),
        "intros_booked":   int(row["intros"] or 0),
        "notes":           f"Canonical Hyros Reporting API (Scientific, meta). Playwright intercept. req_date={row['req_date']}.",
    }


# ── Supabase upsert ────────────────────────────────────────────────────────────

def supabase_upsert(row: dict) -> None:
    body = json.dumps([row]).encode()
    req  = urllib.request.Request(
        f"{SUPA_REST}/marketing_daily_performance?on_conflict=date",
        data=body, method="POST",
        headers={
            "apikey": SUPA_KEY, "Authorization": f"Bearer {SUPA_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            log(f"supabase upsert → HTTP {r.status} ✅")
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"supabase upsert failed: HTTP {e.code} — {e.read().decode()}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("date", nargs="?", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    target = date.fromisoformat(args.date) if args.date else zurich_yesterday()
    log(f"=== Hyros B2B sync start — target={target.isoformat()} dry_run={args.dry_run} ===")

    row = asyncio.run(pull_day(target))
    log(f"captured: {json.dumps({k: v for k, v in row.items() if k != 'notes'})}")

    if args.dry_run:
        log("dry-run — skipping upsert")
        return

    supabase_upsert(row)
    log(f"=== done ===")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"FATAL: {type(e).__name__}: {e}")
        import traceback; traceback.print_exc(file=sys.stderr)
        sys.exit(1)
