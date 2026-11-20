#!/usr/bin/env python3
"""
Discord Mass Joiner + Account Generator with a local web dashboard.

Flow:
  1. python app.py            -> starts local server on http://127.0.0.1:8000
  2. (optional) press "Generate" to pre-create accounts into the pool
  3. paste invite link, set amount (0 = unlimited), press RUN
  4. accounts are generated -> joined -> logged live in the browser
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import random
import re
import string
import sys
import threading
import time
import uuid
import webbrowser
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Optional
from urllib.parse import unquote

import aiohttp
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse

ROOT = Path(__file__).parent
WEB_DIR = ROOT / "web"

API = "https://discord.com/api/v9"
REGISTER_URL = f"{API}/auth/register"
HCAPTCHA_SITEKEY = "a5f74b19-9e45-40e0-b45d-47ff91b7a6c2"
CLIENT_BUILD = 337000

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

UA_LOWER = string.ascii_lowercase + string.digits


# ----------------------------------------------------------------------------- #
#  small helpers
# ----------------------------------------------------------------------------- #
def rnd_username() -> str:
    return "".join(random.choice(UA_LOWER) for _ in range(random.randint(9, 13)))


def rnd_password(n: int = 15) -> str:
    chars = string.ascii_letters + string.digits + "!@#$%^&*"
    return "".join(random.choice(chars) for _ in range(n))


def super_properties(ua: str) -> str:
    props = {
        "os": "Windows" if "Windows" in ua else ("Mac OS X" if "Mac" in ua else "Linux"),
        "browser": "Chrome",
        "device": "",
        "system_locale": "en-US",
        "browser_user_agent": ua,
        "browser_version": "126.0.0.0",
        "os_version": "10",
        "referrer": "",
        "referring_domain": "",
        "referrer_current": "",
        "referring_domain_current": "",
        "release_channel": "stable",
        "client_build_number": CLIENT_BUILD,
        "client_event_source": None,
        "client_heartbeat_session_id": str(uuid.uuid4()),
        "client_app_state": "focused",
    }
    return base64.b64encode(json.dumps(props, separators=(",", ":")).encode()).decode()


def parse_invite(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    m = re.search(r"(?:discord(?:app)?\.com/invite|discord\.gg|discord\.me)/([A-Za-z0-9\-_]+)", raw)
    if m:
        return m.group(1)
    m = re.search(r"([A-Za-z0-9\-_]{2,})", raw)
    return m.group(1) if m else ""


def extract_verify_token(html: str) -> Optional[str]:
    if not html:
        return None
    html = html.replace("&amp;", "&")
    m = re.search(r"verify#token=([A-Za-z0-9_\-]+)", html)
    if m:
        return m.group(1)
    for cand in re.findall(r"[?&]upn=([A-Za-z0-9_\-%=]+)", html):
        raw = unquote(cand)
        try:
            dec = base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4)).decode("utf-8", "ignore")
        except Exception:
            continue
        m2 = re.search(r"verify#token=([A-Za-z0-9_\-]+)", dec) or re.search(r"[?&]token=([A-Za-z0-9_\-]+)", dec)
        if m2:
            return m2.group(1)
    return None


# ----------------------------------------------------------------------------- #
#  websocket hub
# ----------------------------------------------------------------------------- #
class Hub:
    def __init__(self) -> None:
        self.clients: set[WebSocket] = set()
        self.loop: Optional[asyncio.AbstractEventLoop] = None

    async def register(self, ws: WebSocket) -> None:
        await ws.accept()
        self.clients.add(ws)

    def unregister(self, ws: WebSocket) -> None:
        self.clients.discard(ws)

    def broadcast(self, payload: dict) -> None:
        if not self.clients:
            return
        data = json.dumps(payload, default=str)
        try:
            loop = self.loop or asyncio.get_event_loop()
        except RuntimeError:
            return
        for ws in list(self.clients):
            loop.create_task(self._send(ws, data))

    async def _send(self, ws: WebSocket, data: str) -> None:
        try:
            await ws.send_text(data)
        except Exception:
            self.unregister(ws)


# ----------------------------------------------------------------------------- #
#  config
# ----------------------------------------------------------------------------- #
@dataclass
class RunConfig:
    invite: str = ""
    amount: int = 10              # 0 = unlimited
    gen_concurrency: int = 3
    join_concurrency: int = 2
    join_delay: float = 1.0
    retries: int = 3
    mode: str = "generate"        # generate | tokens
    tokens: str = ""
    email_mode: str = "alias"     # alias | list | mailtm
    email_base: str = ""
    email_list: str = ""
    verify_email: bool = False
    solver: str = "none"          # none | capsolver | 2captcha
    solver_key: str = ""
    proxies: str = ""
    password: str = ""


# ----------------------------------------------------------------------------- #
#  engine
# ----------------------------------------------------------------------------- #
class Engine:
    def __init__(self, hub: Hub) -> None:
        self.hub = hub
        self.cfg = RunConfig()
        self.running = False
        self.paused = False
        self.phase = "idle"
        self.task: Optional[asyncio.Task] = None
        self.queue: Optional[asyncio.Queue] = None

        self.pool: list[dict] = []
        self.accounts: list[dict] = []

        self.gen_started = 0
        self.generated = 0
        self.joined = 0
        self.failed = 0
        self.retried = 0
        self.ratelimited = 0
        self.verified = 0
        self.t0 = 0.0

        self._seq = 0
        self._token_pool: list[str] = []
        self._proxy_pool: list[Optional[str]] = []
        self._proxy_idx = 0
        self._gen_lock = asyncio.Lock()
        self._last_stats_push = 0.0
        self._global_until = 0.0

    # ---------------- logging ----------------
    COLORS = {"debug": "\033[90m", "info": "\033[36m", "success": "\033[92m",
              "warn": "\033[93m", "error": "\033[91m"}

    def log(self, level: str, msg: str) -> None:
        c = self.COLORS.get(level, "")
        print(f"{c}[{time.strftime('%H:%M:%S')}] {level.upper():7}\033[0m {msg}", flush=True)
        self.hub.broadcast({"type": "log", "level": level, "msg": msg, "ts": time.time()})

    def push_stats(self, force: bool = False) -> None:
        now = time.time()
        if not force and now - self._last_stats_push < 0.25:
            return
        self._last_stats_push = now
        elapsed = max(0.001, now - self.t0) if self.t0 else 0.001
        self.hub.broadcast({
            "type": "stats",
            "generated": self.generated,
            "joined": self.joined,
            "failed": self.failed,
            "retried": self.retried,
            "ratelimited": self.ratelimited,
            "verified": self.verified,
            "pool": len(self.pool),
            "running": self.running,
            "paused": self.paused,
            "phase": self.phase,
            "elapsed": round(elapsed, 1),
            "speed": round(self.joined / elapsed * 60, 1) if self.t0 else 0.0,
            "amount": self.cfg.amount,
            "gen_started": self.gen_started,
        })

    def push_account(self, acct: dict) -> None:
        safe = {k: v for k, v in acct.items() if k != "mail_token"}
        self.hub.broadcast({"type": "account", "account": safe})

    # ---------------- account record ----------------
    def new_account(self) -> dict:
        self._seq += 1
        return {
            "id": self._seq,
            "username": "", "email": "", "password": "", "token": "",
            "status": "queued", "guild": "", "error": "", "proxy": "",
            "created": time.time(), "mail_token": "",
        }

    # ---------------- config ingestion ----------------
    def apply_config(self, payload: dict) -> None:
        c = self.cfg
        c.invite = parse_invite(payload.get("invite", ""))
        c.amount = int(payload.get("amount") or 0)
        c.gen_concurrency = max(1, min(20, int(payload.get("gen_concurrency") or 3)))
        c.join_concurrency = max(1, min(20, int(payload.get("join_concurrency") or 2)))
        c.join_delay = max(0.0, float(payload.get("join_delay") or 0))
        c.retries = max(0, min(10, int(payload.get("retries") or 3)))
        c.mode = payload.get("mode") or "generate"
        c.tokens = payload.get("tokens") or ""
        c.email_mode = payload.get("email_mode") or "alias"
        c.email_base = (payload.get("email_base") or "").strip()
        c.email_list = payload.get("email_list") or ""
        c.verify_email = bool(payload.get("verify_email"))
        c.solver = payload.get("solver") or "none"
        c.solver_key = (payload.get("solver_key") or "").strip()
        c.proxies = payload.get("proxies") or ""
        c.password = (payload.get("password") or "").strip()

        self._proxy_pool = []
        for line in re.split(r"[\s,;]+", c.proxies.strip()):
            line = line.strip()
            if not line:
                continue
            if "://" not in line:
                line = "http://" + line
            self._proxy_pool.append(line)

        self._token_pool = []
        for line in re.split(r"[\s,;]+", c.tokens.strip()):
            line = line.strip()
            if line:
                self._token_pool.append(line)
        if c.mode == "tokens" and c.amount == 0:
            c.amount = len(self._token_pool)

    def next_proxy(self) -> Optional[str]:
        if not self._proxy_pool:
            return None
        p = self._proxy_pool[self._proxy_idx % len(self._proxy_pool)]
        self._proxy_idx += 1
        return p

    # ---------------- lifecycle ----------------
    def start(self, payload: dict) -> None:
        if self.running:
            return
        self.apply_config(payload)
        self.reset_counters()
        self.running = True
        self.paused = False
        self.phase = "starting"
        self.t0 = time.time()
        self.queue = asyncio.Queue(maxsize=max(4, self.cfg.gen_concurrency * 4))
        self.task = asyncio.create_task(self._run())
        self.push_stats(True)

    def start_prepare(self, payload: dict, n: int) -> None:
        if self.running:
            return
        self.apply_config(payload)
        self.running = True
        self.phase = "preparing"
        self.task = asyncio.create_task(self._prepare(n))
        self.push_stats(True)

    def stop(self) -> None:
        if not self.running:
            return
        self.running = False
        self.paused = False
        self.log("warn", "Stop requested — finishing in-flight work...")

    def toggle_pause(self) -> None:
        self.paused = not self.paused
        self.log("warn", "Paused." if self.paused else "Resumed.")
        self.push_stats(True)

    def reset_counters(self) -> None:
        self.gen_started = self.generated = self.joined = 0
        self.failed = self.retried = self.ratelimited = self.verified = 0
        self._global_until = 0.0

    async def _wait_if_paused(self) -> None:
        while self.paused and self.running:
            await asyncio.sleep(0.2)

    # ---------------- phases ----------------
    async def _prepare(self, n: int) -> None:
        """Generate n accounts into the idle pool, no joining."""
        try:
            self.log("info", f"Pre-generating {n} account(s) with {self.cfg.gen_concurrency} workers...")
            tasks = [asyncio.create_task(self._prepare_one()) for _ in range(n)]
            done = 0
            for fut in asyncio.as_completed(tasks):
                acct = await fut
                done += 1
                if acct:
                    self.pool.append(acct)
                self.phase = f"preparing {done}/{n}"
                self.push_stats(True)
            self.log("success", f"Pool ready: {len(self.pool)} account(s) waiting for an invite.")
        except asyncio.CancelledError:
            pass
        finally:
            self.running = False
            self.phase = "idle"
            self.push_stats(True)
            self.hub.broadcast({"type": "done", "phase": "prepare"})

    async def _prepare_one(self) -> Optional[dict]:
        try:
            return await self.create_account()
        except Exception as e:
            self.log("error", f"generate failed: {e}")
            return None

    async def _run(self) -> None:
        cfg = self.cfg
        try:
            if not cfg.invite:
                self.log("error", "No valid invite link supplied.")
                return

            self.log("info", "Resolving invite...")
            info = await self.resolve_invite(cfg.invite)
            if info:
                self.log("success", f"Target: {info.get('name')} — {info.get('members')} members")
            else:
                self.log("warn", "Could not resolve invite metadata (continuing anyway).")

            # seed queue from the pre-generated pool
            if self.pool:
                seed = list(self.pool)
                self.pool.clear()
                self.log("info", f"Using {len(seed)} pre-generated account(s) from the pool.")
                for a in seed:
                    self.generated += 1
                    self.gen_started += 1
                    await self.queue.put(a)

            self.phase = "running"
            self.push_stats(True)

            workers = [asyncio.create_task(self._consumer(i)) for i in range(cfg.join_concurrency)]
            producer = asyncio.create_task(self._producer())
            await asyncio.gather(producer, *workers)

            self.phase = "finished"
            self.log("success",
                     f"Run complete — joined {self.joined}, failed {self.failed}, "
                     f"generated {self.generated} in {time.time() - self.t0:.0f}s")
        except asyncio.CancelledError:
            self.log("warn", "Run cancelled.")
        except Exception as e:
            self.log("error", f"fatal: {e!r}")
        finally:
            self.running = False
            self.phase = "idle"
            self.push_stats(True)
            self.hub.broadcast({"type": "done"})

    # ---------------- producer ----------------
    async def _producer(self) -> None:
        cfg = self.cfg
        pending: set[asyncio.Task] = set()
        while self.running:
            await self._wait_if_paused()
            can_spawn = (cfg.amount == 0) or (self.gen_started < cfg.amount)
            if can_spawn and len(pending) < max(1, cfg.gen_concurrency):
                self.gen_started += 1
                t = asyncio.create_task(self._gen_one())
                pending.add(t)
                t.add_done_callback(pending.discard)
                continue
            if pending:
                await asyncio.wait(list(pending), return_when=asyncio.FIRST_COMPLETED)
                continue
            if not can_spawn:
                break
            await asyncio.sleep(0.05)

        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        for _ in range(max(1, cfg.join_concurrency)):
            await self.queue.put(None)

    async def _gen_one(self) -> None:
        try:
            acct = await self.create_account()
        except Exception as e:
            self.log("error", f"generation failed: {e}")
            if self.cfg.amount:
                self.gen_started -= 1
            self.push_stats(True)
            return
        self.generated += 1
        self.push_stats()
        await self.queue.put(acct)

    # ---------------- consumer ----------------
    async def _consumer(self, wid: int) -> None:
        while True:
            item = await self.queue.get()
            if item is None:
                await self.queue.put(None)
                return
            await self._wait_if_paused()
            if not self.running and item.get("status") == "queued":
                pass
            await self.join_account(item, wid)

    # ---------------- generation ----------------
    async def create_account(self) -> dict:
        cfg = self.cfg
        acct = self.new_account()
        acct["proxy"] = self.next_proxy() or ""

        if cfg.mode == "tokens":
            async with self._gen_lock:
                if not self._token_pool:
                    raise RuntimeError("token list exhausted")
                token = self._token_pool.pop(0)
            acct["token"] = token
            acct["status"] = "queued"
            self.push_account(acct)
            return acct

        email, mail_token = await self._next_email()
        acct["email"] = email
        acct["mail_token"] = mail_token or ""
        acct["username"] = rnd_username()
        acct["password"] = cfg.password or rnd_password()
        acct["status"] = "generating"
        self.push_account(acct)

        ua = random.choice(USER_AGENTS)
        body = {
            "email": email,
            "password": acct["password"],
            "username": acct["username"],
            "consent": True,
            "device_fingerprint": None,
            "unique_username_registration": True,
            "gift_code_sku_id": None,
            "captcha_key": None,
        }
        if cfg.invite:
            body["invite"] = cfg.invite

        headers = self._headers(ua, register=True)

        data, status = await self._request("POST", REGISTER_URL, body, headers, acct["proxy"])

        if status == 429:
            self.ratelimited += 1
            await asyncio.sleep(float(data.get("retry_after", 2)) if isinstance(data, dict) else 2)
            data, status = await self._request("POST", REGISTER_URL, body, headers, acct["proxy"])

        if isinstance(data, dict) and "captcha_key" in data and not data.get("token"):
            if cfg.solver == "none":
                raise RuntimeError("captcha required — add a CapSolver/2Captcha key")
            self.log("debug", f"solving hCaptcha for {email}...")
            solved = await self.solve_captcha(
                data.get("captcha_sitekey", HCAPTCHA_SITEKEY),
                data.get("captcha_rqdata"),
                ua,
                acct["proxy"],
            )
            if not solved:
                raise RuntimeError("captcha solve failed")
            body["captcha_key"] = solved
            if data.get("captcha_rqtoken"):
                body["captcha_rqtoken"] = data["captcha_rqtoken"]
            data, status = await self._request("POST", REGISTER_URL, body, headers, acct["proxy"])

        if not isinstance(data, dict) or "token" not in data:
            err = self._pretty_error(data, status)
            if "USERNAME_TAKEN" in json.dumps(data or {})[:200]:
                raise RuntimeError("username taken")
            raise RuntimeError(err)

        acct["token"] = data["token"]
        acct["status"] = "queued"
        self.push_account(acct)
        self.log("success", f"generated {acct['email']}  ({acct['username']})")

        if cfg.verify_email and acct["mail_token"]:
            await self.verify_account(acct)

        return acct

    # ---------------- email providers ----------------
    async def _next_email(self) -> tuple[str, Optional[str]]:
        cfg = self.cfg
        if cfg.email_mode == "list":
            async with self._gen_lock:
                lines = [l.strip() for l in re.split(r"[\s,;]+", cfg.email_list) if l.strip()]
                if not lines:
                    raise RuntimeError("email list empty")
                # pop deterministic by index
                idx = min(self.generated, len(lines) - 1)
                if self.generated < len(lines):
                    return lines[self.generated], None
                return lines[idx], None

        if cfg.email_mode == "mailtm":
            return await self._mailtm_create()

        # alias mode
        base = cfg.email_base.strip()
        if not base:
            raise RuntimeError("email_base required in alias mode")
        if "@" not in base:
            base += "@gmail.com"
        local, domain = base.split("@", 1)
        if self.generated == 0 and not re.search(r"\+", local):
            addr = base
        else:
            addr = f"{local}+d{random.randint(100000, 999999)}@{domain}"
        return addr, None

    async def _mailtm_create(self) -> tuple[str, str]:
        base = "https://api.mail.tm"
        async with aiohttp.ClientSession(connector=self._connector()) as s:
            r = await s.get(f"{base}/domains")
            domains = (await r.json()).get("hydra:member", [])
            if not domains:
                raise RuntimeError("mail.tm has no domains")
            domain = random.choice(domains)["domain"]
            address = f"{rnd_username()}{random.randint(10,99)}@{domain}"
            password = rnd_password(16)
            r = await s.post(f"{base}/accounts", json={"address": address, "password": password})
            if r.status not in (200, 201):
                raise RuntimeError(f"mail.tm account failed ({r.status})")
            r = await s.post(f"{base}/token", json={"address": address, "password": password})
            tok = (await r.json()).get("token")
            if not tok:
                raise RuntimeError("mail.tm auth failed")
            return address, tok

    async def verify_account(self, acct: dict) -> None:
        base = "https://api.mail.tm"
        headers = {"Authorization": f"Bearer {acct['mail_token']}"}
        async with aiohttp.ClientSession(connector=self._connector()) as s:
            deadline = time.time() + 90
            while time.time() < deadline and self.running:
                await asyncio.sleep(4)
                try:
                    r = await s.get(f"{base}/messages", headers=headers)
                    if r.status != 200:
                        continue
                    for msg in (await r.json()).get("hydra:member", []):
                        r2 = await s.get(f"{base}/messages/{msg['id']}", headers=headers)
                        if r2.status != 200:
                            continue
                        payload = await r2.json()
                        blob = " ".join(payload.get("html", []) or []) + (payload.get("text") or "")
                        tok = extract_verify_token(blob)
                        if tok:
                            await self._request(
                                "POST", f"{API}/auth/verify",
                                {"token": tok, "captcha_key": None},
                                self._headers(random.choice(USER_AGENTS), auth=acct["token"]),
                                acct["proxy"],
                            )
                            self.verified += 1
                            self.log("info", f"email verified: {acct['email']}")
                            return
                except Exception:
                    continue
            self.log("warn", f"verification timeout for {acct['email']}")

    # ---------------- joining ----------------
    async def join_account(self, acct: dict, wid: int) -> None:
        if not self.running:
            acct["status"] = "skipped"
            self.push_account(acct)
            return

        acct["status"] = "joining"
        self.push_account(acct)

        attempt = 0
        while attempt <= self.cfg.retries and self.running:
            await self._wait_if_paused()
            if attempt:
                self.retried += 1
                self.push_stats()

            await self._global_gate()

            hdrs = self._headers(random.choice(USER_AGENTS), auth=acct["token"])
            data, status = await self._request(
                "POST", f"{API}/invites/{self.cfg.invite}",
                {"session_id": str(uuid.uuid4())}, hdrs, acct["proxy"] or None,
            )

            if status in (200, 201, 204):
                guild = ""
                if isinstance(data, dict):
                    guild = (data.get("guild") or {}).get("name", "")
                acct["status"] = "joined"
                acct["guild"] = guild or acct["guild"]
                acct["error"] = ""
                self.joined += 1
                self.log("success", f"[w{wid}] joined -> {acct['email'] or acct['token'][:12]}")
                self.push_account(acct)
                self.push_stats(True)
                if self.cfg.join_delay:
                    await asyncio.sleep(self.cfg.join_delay)
                return

            if status == 429:
                self.ratelimited += 1
                ra = float(data.get("retry_after", 1.5)) if isinstance(data, dict) else 1.5
                if isinstance(data, dict) and data.get("global"):
                    self._global_until = time.time() + ra
                    self.log("warn", f"global rate limit — pausing {ra:.1f}s")
                await asyncio.sleep(ra + random.uniform(0, 0.4))
                attempt += 1
                continue

            if status == 401:
                acct["status"] = "invalid"
                acct["error"] = "invalid token"
                self.failed += 1
                self.push_account(acct)
                self.push_stats(True)
                self.log("error", f"invalid token ({acct['email'] or acct['token'][:12]})")
                return

            if status >= 500:
                await asyncio.sleep(2 + attempt)
                attempt += 1
                continue

            err = self._pretty_error(data, status)
            low = err.lower()
            if "already" in low:
                acct["status"] = "joined"
                acct["error"] = "already a member"
                self.joined += 1
                self.push_account(acct)
                self.push_stats(True)
                self.log("info", f"already a member: {acct['email'] or acct['token'][:12]}")
                return

            acct["status"] = "failed"
            acct["error"] = err
            self.failed += 1
            self.push_account(acct)
            self.push_stats(True)
            self.log("error", f"join failed ({acct['email'] or acct['token'][:12]}): {err}")
            return

        if acct["status"] == "joining":
            acct["status"] = "failed"
            acct["error"] = "retries exhausted"
            self.failed += 1
            self.push_account(acct)
            self.push_stats(True)

    async def _global_gate(self) -> None:
        while time.time() < self._global_until:
            await asyncio.sleep(0.25)

    # ---------------- http plumbing ----------------
    def _connector(self) -> aiohttp.TCPConnector:
        return aiohttp.TCPConnector(limit=0, ttl_dns_cache=300, ssl=False)

    def _headers(self, ua: str, *, auth: str = "", register: bool = False) -> dict:
        h = {
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/json",
            "Origin": "https://discord.com",
            "Referer": "https://discord.com/register" if register else "https://discord.com/",
            "User-Agent": ua,
            "X-Super-Properties": super_properties(ua),
            "X-Discord-Locale": "en-US",
            "X-Discord-Timezone": "Asia/Kolkata",
            "X-Debug-Options": "bugReporterEnabled",
            "Cookie": f"__dcfduid={os.urandom(16).hex()}; __sdcfduid={os.urandom(16).hex()}",
        }
        if auth:
            h["Authorization"] = auth
        if register:
            h["X-Fingerprint"] = str(uuid.uuid4())
        return h

    async def _request(self, method: str, url: str, body: dict | None,
                       headers: dict, proxy: Optional[str]) -> tuple[Any, int]:
        try:
            async with aiohttp.ClientSession(connector=self._connector()) as s:
                kwargs = {"headers": headers}
                if proxy:
                    kwargs["proxy"] = proxy
                if body is not None:
                    kwargs["json"] = body
                async with s.request(method, url, **kwargs) as r:
                    txt = await r.text()
                    try:
                        data = json.loads(txt) if txt else {}
                    except Exception:
                        data = {"raw": txt[:300]}
                    return data, r.status
        except asyncio.TimeoutError:
            return {"message": "timeout"}, 408
        except aiohttp.ClientError as e:
            return {"message": f"network: {e.__class__.__name__}"}, 599

    @staticmethod
    def _pretty_error(data: Any, status: int) -> str:
        if isinstance(data, dict):
            if data.get("message"):
                return f"{status} {data['message']}"
            keys = [k for k in data.keys() if k not in ("raw", "captcha_key")]
            if keys:
                return f"{status} " + ", ".join(f"{k}: {data[k]}" for k in keys[:3])
            if data.get("raw"):
                return f"{status} {data['raw'][:120]}"
        return f"HTTP {status}"

    async def resolve_invite(self, code: str) -> Optional[dict]:
        url = f"{API}/invites/{code}?with_counts=true&with_expiration=true&inputValue=discord.gg%2F{code}"
        data, status = await self._request("GET", url, None, self._headers(USER_AGENTS[0]), None)
        if status != 200 or not isinstance(data, dict):
            return None
        g = data.get("guild") or {}
        icon = g.get("icon")
        return {
            "name": g.get("name", "unknown"),
            "id": g.get("id", ""),
            "members": data.get("approximate_member_count", "?"),
            "online": data.get("approximate_presence_count", "?"),
            "channel": (data.get("channel") or {}).get("name", ""),
            "icon": f"https://cdn.discordapp.com/icons/{g.get('id')}/{icon}.png?size=128" if icon else "",
            "expires": data.get("expires_at"),
        }

    # ---------------- captcha ----------------
    async def solve_captcha(self, sitekey: str, rqdata: Optional[str],
                            ua: str, proxy: Optional[str]) -> Optional[str]:
        cfg = self.cfg
        if not cfg.solver_key:
            return None
        try:
            if cfg.solver == "capsolver":
                return await self._capsolver(sitekey, rqdata, ua, proxy)
            if cfg.solver == "2captcha":
                return await self._twocaptcha(sitekey, rqdata, ua, proxy)
        except Exception as e:
            self.log("error", f"captcha error: {e}")
        return None

    async def _capsolver(self, sitekey, rqdata, ua, proxy) -> Optional[str]:
        task = {
            "type": "HCaptchaTaskProxyLess",
            "websiteURL": "https://discord.com/register",
            "websiteKey": sitekey,
            "userAgent": ua,
        }
        if rqdata:
            task["enterprisePayload"] = {"rqdata": rqdata}
        async with aiohttp.ClientSession() as s:
            r = await s.post("https://api.capsolver.com/createTask",
                             json={"clientKey": self.cfg.solver_key, "task": task})
            j = await r.json()
            tid = j.get("taskId")
            if not tid:
                self.log("error", f"capsolver: {j.get('errorDescription') or j}")
                return None
            deadline = time.time() + 150
            while time.time() < deadline and self.running:
                await asyncio.sleep(3)
                r = await s.post("https://api.capsolver.com/getTaskResult",
                                 json={"clientKey": self.cfg.solver_key, "taskId": tid})
                j = await r.json()
                if j.get("status") == "ready":
                    return (j.get("solution") or {}).get("gRecaptchaResponse")
                if j.get("status") == "failed":
                    self.log("error", f"capsolver failed: {j.get('errorDescription')}")
                    return None
        return None

    async def _twocaptcha(self, sitekey, rqdata, ua, proxy) -> Optional[str]:
        params = {
            "key": self.cfg.solver_key,
            "method": "hcaptcha",
            "sitekey": sitekey,
            "pageurl": "https://discord.com/register",
            "userAgent": ua,
            "json": 1,
            "enterprise": 1,
        }
        if rqdata:
            params["data"] = rqdata
        async with aiohttp.ClientSession() as s:
            r = await s.post("https://2captcha.com/in.php", data=params)
            j = await r.json()
            if j.get("status") != 1:
                self.log("error", f"2captcha: {j.get('request')}")
                return None
            cid = j["request"]
            deadline = time.time() + 180
            while time.time() < deadline and self.running:
                await asyncio.sleep(5)
                r = await s.get("https://2captcha.com/res.php",
                                params={"key": self.cfg.solver_key, "action": "get",
                                        "id": cid, "json": 1})
                j = await r.json()
                if j.get("status") == 1:
                    return j["request"]
                if j.get("request") not in ("CAPCHA_NOT_READY", "CAPTCHA_NOT_READY"):
                    self.log("error", f"2captcha failed: {j.get('request')}")
                    return None
        return None

    # ---------------- snapshot ----------------
    def snapshot(self) -> dict:
        elapsed = max(0.001, time.time() - self.t0) if self.t0 else 0.001
        return {
            "running": self.running,
            "paused": self.paused,
            "phase": self.phase,
            "stats": {
                "generated": self.generated, "joined": self.joined, "failed": self.failed,
                "retried": self.retried, "ratelimited": self.ratelimited,
                "verified": self.verified, "pool": len(self.pool),
                "elapsed": round(elapsed, 1),
                "speed": round(self.joined / elapsed * 60, 1) if self.t0 else 0.0,
                "amount": self.cfg.amount, "gen_started": self.gen_started,
            },
            "accounts": [{k: v for k, v in a.items() if k != "mail_token"}
                         for a in self.accounts[-1500:]],
        }

    def export(self, kind: str) -> str:
        rows = self.accounts
        if kind == "joined":
            rows = [a for a in rows if a["status"] == "joined"]
        if kind == "tokens":
            return "\n".join(a["token"] for a in rows if a.get("token"))
        out = []
        for a in rows:
            out.append(f"{a.get('email','')}|{a.get('password','')}|{a.get('token','')}|"
                       f"{a.get('status','')}|{a.get('guild','')}|{a.get('error','')}")
        return "\n".join(out)


# ----------------------------------------------------------------------------- #
#  app
# ----------------------------------------------------------------------------- #
hub = Hub()
engine = Engine(hub)
app = FastAPI(title="Discord Joiner")


@app.on_event("startup")
async def _startup() -> None:
    hub.loop = asyncio.get_running_loop()


@app.get("/")
async def index():
    return FileResponse(WEB_DIR / "index.html")


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await hub.register(ws)
    try:
        await ws.send_text(json.dumps({"type": "snapshot", **engine.snapshot()}))
        while True:
            msg = await ws.receive_text()
            if msg == "ping":
                await ws.send_text(json.dumps({"type": "pong"}))
    except WebSocketDisconnect:
        hub.unregister(ws)
    except Exception:
        hub.unregister(ws)


@app.post("/api/start")
async def api_start(payload: dict):
    if engine.running:
        return {"ok": False, "error": "already running"}
    engine.start(payload)
    return {"ok": True}


@app.post("/api/prepare")
async def api_prepare(payload: dict):
    if engine.running:
        return {"ok": False, "error": "already running"}
    n = int(payload.get("prepare_amount") or payload.get("amount") or 5)
    engine.start_prepare(payload, max(1, n))
    return {"ok": True}


@app.post("/api/stop")
async def api_stop():
    engine.stop()
    return {"ok": True}


@app.post("/api/pause")
async def api_pause():
    engine.toggle_pause()
    return {"ok": True, "paused": engine.paused}


@app.get("/api/state")
async def api_state():
    return engine.snapshot()


@app.get("/api/invite")
async def api_invite(url: str):
    code = parse_invite(url)
    if not code:
        return JSONResponse({"ok": False, "error": "could not parse invite"}, status_code=400)
    info = await engine.resolve_invite(code)
    if not info:
        return JSONResponse({"ok": False, "error": "invite invalid or expired"}, status_code=404)
    return {"ok": True, "code": code, **info}


@app.get("/api/export")
async def api_export(kind: str = "all"):
    data = engine.export(kind)
    return PlainTextResponse(
        data or "",
        headers={"Content-Disposition": f'attachment; filename="{kind}.txt"'},
    )


# ----------------------------------------------------------------------------- #
def main() -> None:
    ap = argparse.ArgumentParser(description="Discord Mass Joiner + dashboard")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--no-browser", action="store_true")
    args = ap.parse_args()

    url = f"http://{args.host}:{args.port}"
    print(f"\n  \033[95m◆\033[0m Discord Joiner dashboard  ->  \033[96m{url}\033[0m\n")

    if not args.no_browser:
        threading.Timer(1.2, lambda: webbrowser.open(url)).start()

    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()