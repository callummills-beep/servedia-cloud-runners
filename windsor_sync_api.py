#!/usr/bin/env python3
"""
Windsor connection sync — PURE HTTP (no Playwright, no reCAPTCHA, no 2Captcha).

Discovered 2026-06-05: Windsor's account-selection is a plain REST API keyed
by the same api_key as the data pull:

  GET    api-onboard.windsor.ai/facebook/accounts?api_key=     -> available accounts (account_id, account_name, credentials_id)
  GET    api-onboard.windsor.ai/api/ds/accounts/facebook?api_key=  -> connected accounts (internal id, account_id, ...)
  POST   api-onboard.windsor.ai/api/ds/accounts/facebook?api_key=  body {account_id, account_name, credentials_id}  -> ADD/select
  DELETE api-onboard.windsor.ai/api/ds/accounts/facebook?api_key=  body {id}                                         -> REMOVE/deselect

Policy (set by Callum):
  - ADD every Active+Paused Hub client whose ad account is reachable (in the
    available list / OAuth scope) but not yet connected.
  - REMOVE connected accounts whose ONLY Hub owner is Churned — BUT only after a
    collision check: never remove an account_id that ANY active/paused client
    also uses (ad accounts get repurposed).
  - Active clients whose account is NOT in the available list need a manual
    "Grant Facebook Ads Access" in Windsor (new BM) — reported, not actioned.

Runs anywhere with the api_key + Supabase service key. No browser. No Mac.
"""
from __future__ import annotations
import argparse, json, sys, time
from pathlib import Path
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
import socket

CONFIG = Path.home()/".config"/"servedia"
SUPA = "https://mvhqcfifnppxryfepspb.supabase.co"
ONB = "https://api-onboard.windsor.ai"
UA = "servedia-windsor-sync-api/1.0"
ACTIVE = ("Active (First Contract)","Active (Past Initial Contract)","Paused")
CHURNED = ("Churned",)

def secrets():
    import os
    if b64 := os.environ.get("SECRETS_B64"):
        import base64; return json.loads(base64.b64decode(b64))
    return {
        "windsor_keys":[(CONFIG/"windsor-api-key-1").read_text().strip(),
                        (CONFIG/"windsor-api-key-2").read_text().strip()],
        "supabase_svc":(CONFIG/"supabase-hub-service-role-key").read_text().strip(),
    }

_RETRY=(ConnectionResetError,ConnectionError,socket.timeout,TimeoutError)
def http(method, url, headers=None, body=None, attempts=3):
    last=None
    for i in range(attempts):
        try:
            req=Request(url, data=(json.dumps(body).encode() if body is not None else None),
                        headers={"User-Agent":UA, **(headers or {})}, method=method)
            with urlopen(req, timeout=60) as r:
                raw=r.read()
                return r.status, (json.loads(raw) if raw else None)
        except HTTPError as e:
            if e.code in (429,500,502,503,504) and i<attempts-1: last=e; time.sleep(5*(3**i)); continue
            raw=e.read().decode("utf-8","replace")
            return e.code, raw
        except _RETRY as e:
            last=e
            if i<attempts-1: time.sleep(5*(3**i)); continue
            raise
    raise last

def windsor_available(key):
    s,d=http("GET", f"{ONB}/facebook/accounts?{urlencode({'api_key':key})}")
    accts = d.get("accounts",[]) if isinstance(d,dict) else (d if isinstance(d,list) else [])
    # map account_id -> {name, credentials_id} (first wins if dup across creds)
    out={}
    for a in accts:
        aid=str(a.get("account_id"))
        if aid and aid not in out:
            out[aid]={"name":a.get("account_name"),"cred":a.get("credentials_id")}
    return out

def windsor_connected(key):
    s,d=http("GET", f"{ONB}/api/ds/accounts/facebook?{urlencode({'api_key':key})}")
    # connected endpoint wraps the list under "data"; available uses "accounts"
    accts = d if isinstance(d,list) else (d.get("data") or d.get("accounts") or []) if isinstance(d,dict) else []
    # map account_id -> internal id
    return {str(a.get("account_id")): a.get("id") for a in accts if a.get("account_id")}

def hub_clients(svc):
    statuses = ACTIVE + CHURNED
    inq="("+",".join(f'"{s}"' for s in statuses)+")"
    fields=quote('"Client Name",mb_ad_account_id,"Status"')
    url=f'{SUPA}/rest/v1/{quote("Client Management")}?select={fields}&Status=in.{quote(inq,safe="()")}&mb_ad_account_id=not.is.null'
    s,rows=http("GET",url,headers={"apikey":svc,"Authorization":f"Bearer {svc}"})
    active, churned = {}, {}
    for r in rows:
        acc=(r.get("mb_ad_account_id") or "").replace("act_","").strip()
        if not acc: continue
        if r["Status"] in ACTIVE: active[acc]=r["Client Name"]
        elif r["Status"] in CHURNED: churned.setdefault(acc, r["Client Name"])
    return active, churned

def add_account(key, account_id, name, cred):
    return http("POST", f"{ONB}/api/ds/accounts/facebook?{urlencode({'api_key':key})}",
                headers={"Content-Type":"application/json"},
                body={"account_id":account_id,"account_name":name,"credentials_id":cred})

def remove_account(key, internal_id):
    return http("DELETE", f"{ONB}/api/ds/accounts/facebook?{urlencode({'api_key':key})}",
                headers={"Content-Type":"application/json"}, body={"id":internal_id})

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--workspace", type=int, default=1)
    args=ap.parse_args()
    sec=secrets()
    key=sec["windsor_keys"][args.workspace-1]
    svc=sec["supabase_svc"]

    available=windsor_available(key)
    connected=windsor_connected(key)
    active, churned = hub_clients(svc)
    print(f"available={len(available)} connected={len(connected)} hub_active={len(active)} hub_churned={len(churned)}")

    # ADD: active, reachable, not connected
    to_add=[]
    needs_grant=[]
    for acc, cname in active.items():
        if acc in connected: continue
        if acc in available: to_add.append((acc, cname, available[acc]))
        else: needs_grant.append((acc, cname))

    # REMOVE: connected, owner churned, NO active collision
    to_remove=[]
    for acc, internal_id in connected.items():
        if acc in active: continue                 # active owner → keep
        if acc in churned:                          # churned owner, and not active → safe remove
            to_remove.append((acc, churned[acc], internal_id))

    print(f"\nPLAN: +{len(to_add)} add | -{len(to_remove)} remove | {len(needs_grant)} need Grant-Access")
    for acc,c,info in to_add: print(f"  ADD     {acc:<20} {c}")
    for acc,c,iid in to_remove: print(f"  REMOVE  {acc:<20} {c} (churned)")
    for acc,c in needs_grant: print(f"  GRANT?  {acc:<20} {c} (not in OAuth scope)")

    if args.dry_run:
        print("\n(dry-run; no changes)"); return 0

    done_add=done_rm=0
    for acc,c,info in to_add:
        st,resp=add_account(key, acc, info["name"] or c, info["cred"])
        ok = st in (200,201)
        print(f"  {'✅' if ok else '❌'} ADD {acc} ({c}) -> {st}")
        done_add += ok
        time.sleep(0.4)
    for acc,c,iid in to_remove:
        st,resp=remove_account(key, iid)
        ok = st in (200,204)
        print(f"  {'✅' if ok else '❌'} REMOVE {acc} ({c}) -> {st}")
        done_rm += ok
        time.sleep(0.4)
    print(f"\nDONE: added {done_add}/{len(to_add)}, removed {done_rm}/{len(to_remove)}")
    if needs_grant:
        print(f"⚠️  {len(needs_grant)} still need manual Grant Facebook Ads Access in Windsor (new BMs).")
    return 0

if __name__=="__main__":
    sys.exit(main())
