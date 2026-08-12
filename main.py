# ==========================================
# PRODUCTION-GRADE TELETHON MANAGER BOT v2.0
# Optimized for 500+ Accounts with MongoDB
# Ultra-Fast Parallel Processing Engine
# Enhanced: Error Handling, Auto-Recovery, Smart Notifications, Render Keep-Alive
# ==========================================

import asyncio
import logging
import os
import sys
import time
import re
import traceback
import json
import random
import psutil
import urllib.request
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from collections import defaultdict

from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError, PhoneCodeInvalidError, FloodWaitError,
    UnauthorizedError, ChatWriteForbiddenError, UserAlreadyParticipantError,
    UserBannedInChannelError, InviteHashExpiredError, InviteHashInvalidError,
    ChannelPrivateError, AuthKeyUnregisteredError, AuthKeyDuplicatedError,
    ChatAdminRequiredError, SlowModeWaitError, PeerIdInvalidError,
    ChannelsTooMuchError, UserDeactivatedBanError, PhoneNumberBannedError,
    RPCError, QueryIdInvalidError
)

from telethon.tl.functions.channels import JoinChannelRequest, GetParticipantRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.functions.chatlists import CheckChatlistInviteRequest, JoinChatlistInviteRequest
from telethon.tl.functions.account import ReportPeerRequest
from telethon.tl.functions.messages import ReportRequest
from telethon.tl.types import (
    InputReportReasonSpam, InputReportReasonFake, 
    InputReportReasonViolence, InputReportReasonPornography, 
    InputReportReasonOther, DocumentAttributeSticker, InputStickerSetEmpty
)

from motor.motor_asyncio import AsyncIOMotorClient

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ==========================================
# CONFIGURATION (From Environment Variables)
# ==========================================
API_ID = int(os.getenv("API_ID", "38174429"))
API_HASH = os.getenv("API_HASH", "45f03a04bfd3ce9d12c877b4295cf785")
BOT_TOKEN = os.getenv("BOT_TOKEN", "8959839207:AAHRisdYogaRgsyabtGQGZWa-vW_rDB-y9I")
ADMIN_ID = int(os.getenv("ADMIN_ID", "7929802589"))
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb+srv://alishalyfbuisness_db_user:aWwRhpw3NrcbSvaZ@cluster0.texmdkv.mongodb.net/?appName=Cluster0")
PORT = int(os.getenv("PORT", "8080"))

# Multi-API Support
API_CONFIGS_JSON = os.getenv("API_CONFIGS", "")
try:
    if API_CONFIGS_JSON and API_CONFIGS_JSON.strip():
        API_CONFIGS = json.loads(API_CONFIGS_JSON)
    else:
        API_CONFIGS = [{"api_id": API_ID, "api_hash": API_HASH}]
except Exception:
    API_CONFIGS = [{"api_id": API_ID, "api_hash": API_HASH}]

import math
def get_api_for_account(acc_index: int, total_accs: int) -> Dict:
    """Smart API distribution: 10 acc + 2 APIs = 5 each"""
    if not API_CONFIGS:
        return {"api_id": API_ID, "api_hash": API_HASH}
    num_apis = len(API_CONFIGS)
    per_api = math.ceil(total_accs / num_apis) if num_apis > 0 else 1
    idx = min(acc_index // per_api, num_apis - 1)
    return API_CONFIGS[idx]

# Folders
os.makedirs("downloads", exist_ok=True)
os.makedirs("logs", exist_ok=True)

# ==========================================
# ADVANCED LOGGING SYSTEM
# ==========================================
logger = logging.getLogger("MultiAccountManager")
logger.setLevel(logging.DEBUG)
log_formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

file_handler = RotatingFileHandler("logs/bot_logs.txt", maxBytes=10*1024*1024, backupCount=3)
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

# ==========================================
# DUMMY WEB SERVER & KEEP-ALIVE SYSTEM
# ==========================================
async def handle_ping_request(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Responds to web health checks to keep Render/Web services alive"""
    try:
        await reader.read(1024)
        response = "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 12\r\n\r\nBot is Alive"
        writer.write(response.encode('utf-8'))
        await writer.drain()
    except Exception:
        pass
    finally:
        writer.close()
        await writer.wait_closed()

async def start_dummy_web_server():
    """Starts a native asyncio HTTP server on $PORT for deployment platforms"""
    try:
        server = await asyncio.start_server(handle_ping_request, "0.0.0.0", PORT)
        logger.info(f"🌐 Dummy Web Server running on port {PORT}")
        async with server:
            await server.serve_forever()
    except Exception as e:
        logger.error(f"❌ Dummy Web Server Error: {e}")

async def keep_alive_ping_loop():
    """Periodic self-ping loop to prevent free-tier instances from going to sleep"""
    await asyncio.sleep(10)
    ping_url = os.getenv("RENDER_EXTERNAL_URL") or os.getenv("PING_URL")
    if not ping_url:
        ping_url = f"http://127.0.0.1:{PORT}"

    logger.info(f"🔄 Keep-Alive loop started. Target URL: {ping_url}")
    
    while True:
        try:
            await asyncio.sleep(300)  # Ping every 5 minutes
            loop = asyncio.get_running_loop()
            
            def _ping():
                req = urllib.request.Request(ping_url, headers={'User-Agent': 'KeepAlivePing/1.0'})
                with urllib.request.urlopen(req, timeout=10) as response:
                    return response.status

            status = await loop.run_in_executor(None, _ping)
            logger.info(f"💓 Keep-alive ping sent to {ping_url} | Status: {status}")
        except Exception as e:
            logger.warning(f"⚠️ Keep-alive ping attempt failed: {e}")

# ==========================================
# ERROR TRACKING & SMART NOTIFICATION SYSTEM
# ==========================================
@dataclass
class ErrorRecord:
    """Tracks unique errors to prevent spam notifications"""
    error_type: str
    phone: str
    chat_id: Optional[str] = None
    count: int = 1
    last_seen: float = field(default_factory=time.time)
    notified: bool = False
    
class ErrorTracker:
    """Manages error deduplication and smart notifications"""
    def __init__(self):
        self.errors: Dict[str, ErrorRecord] = {}
        self.cleanup_interval = 3600  # Clean up old errors every 1 hour
        
    def get_key(self, error_type: str, phone: str, chat_id: Optional[str] = None) -> str:
        return f"{phone}:{error_type}:{chat_id or 'global'}"
    
    def add_error(self, error_type: str, phone: str, chat_id: Optional[str] = None) -> bool:
        key = self.get_key(error_type, phone, chat_id)
        if key in self.errors:
            self.errors[key].count += 1
            self.errors[key].last_seen = time.time()
            return False
        
        self.errors[key] = ErrorRecord(
            error_type=error_type,
            phone=phone,
            chat_id=chat_id,
            notified=False
        )
        return True
    
    def mark_notified(self, error_type: str, phone: str, chat_id: Optional[str] = None):
        key = self.get_key(error_type, phone, chat_id)
        if key in self.errors:
            self.errors[key].notified = True
    
    async def cleanup_old_errors(self):
        current_time = time.time()
        old_keys = [
            k for k, v in self.errors.items() 
            if current_time - v.last_seen > self.cleanup_interval
        ]
        for k in old_keys:
            del self.errors[k]

error_tracker = ErrorTracker()

# ==========================================
# MONGODB SETUP
# ==========================================
if not MONGODB_URI:
    logger.error("CRITICAL ERROR: MONGODB_URI environment variable is missing.")
    sys.exit(1)
if API_ID <= 0 or not API_HASH or not BOT_TOKEN:
    logger.error("CRITICAL ERROR: API_ID, API_HASH, and BOT_TOKEN must be configured via environment variables.")
    sys.exit(1)

mongo_client = AsyncIOMotorClient(MONGODB_URI)
db = mongo_client['telethon_manager']

accounts_col = db['accounts']
ad_config_col = db['ad_config']
bot_stats_col = db['bot_stats']
auth_users_col = db['auth_users']
error_log_col = db['error_logs']
smart_requirements_col = db['smart_join_requirements']

async def connect_to_mongodb():
    max_retries = 3
    retry_delay = 5
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Connecting to MongoDB (Attempt {attempt}/{max_retries})...")
            await mongo_client.server_info()
            logger.info("✅ Successfully connected to MongoDB!")
            return True
        except Exception as e:
            logger.error(f"❌ MongoDB connection failed on attempt {attempt}: {e}")
            if attempt < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
            else:
                logger.error("CRITICAL ERROR: Failed to connect to MongoDB after 3 attempts.")
                sys.exit(1)

def normalize_broadcast_config(config: dict) -> dict:
    config = dict(config or {})
    mode = str(config.get("broadcast_mode", "parallel")).lower().strip()
    if mode not in {"sequential", "parallel"}:
        mode = "parallel"
    try:
        account_delay = max(5, min(int(config.get("account_delay", 120)), 86400))
    except (TypeError, ValueError):
        account_delay = 120
    try:
        interval = max(10, min(int(config.get("interval", 300)), 86400))
    except (TypeError, ValueError):
        interval = 300
    try:
        acc_cooldown = max(0, min(int(config.get("acc_cooldown", 5)), 3600))
    except (TypeError, ValueError):
        acc_cooldown = 5
    config.update({"broadcast_mode": mode, "account_delay": account_delay, "interval": interval, "acc_cooldown": acc_cooldown})
    return config

async def setup_db():
    try:
        if not await ad_config_col.find_one({"_id": 1}):
            await ad_config_col.insert_one({
                "_id": 1,
                "status": "paused",
                "msgs": [],
                "interval": 300,
                "last_run": 0,
                "target_type": "all",
                "target_ids": [],
                "acc_cooldown": 5,
                "broadcast_mode": "parallel",
                "account_delay": 120
            })
        
        if not await bot_stats_col.find_one({"_id": 1}):
            await bot_stats_col.insert_one({
                "_id": 1,
                "total_sent": 0,
                "total_failed": 0,
                "total_joined": 0,
                "errors_today": 0,
                "success_rate": 0.0
            })
        
        if not await auth_users_col.find_one({"_id": ADMIN_ID}):
            await auth_users_col.insert_one({"_id": ADMIN_ID})
        
        raw_ad_config = await ad_config_col.find_one({"_id": 1}) or {}
        if raw_ad_config.get("broadcast_mode") == "sequential" and not raw_ad_config.get("broadcast_mode_explicit", False):
            raw_ad_config["broadcast_mode"] = "parallel"
        existing_ad_config = normalize_broadcast_config(raw_ad_config)
        await ad_config_col.update_one({"_id": 1}, {"$set": {
            "broadcast_mode": existing_ad_config["broadcast_mode"],
            "account_delay": existing_ad_config["account_delay"],
            "interval": existing_ad_config["interval"],
            "acc_cooldown": existing_ad_config["acc_cooldown"]
        }}, upsert=True)

        if not existing_ad_config or "smart_join" not in existing_ad_config:
            await ad_config_col.update_one(
                {"_id": 1},
                {"$set": {"smart_join": {"enabled": True, "cache_ttl": 900}}},
                upsert=True
            )
        await smart_requirements_col.create_index(
            [("group_id", 1), ("source_message_id", 1), ("channel_key", 1)],
            name="smart_requirement_dedupe"
        )
        await smart_requirements_col.create_index("expires_at", expireAfterSeconds=0, name="smart_requirement_ttl")

        await accounts_col.create_index("status")
        await error_log_col.create_index("timestamp", expireAfterSeconds=604800)
        
        logger.info("✅ MongoDB Database setup verified successfully.")
    except Exception as e:
        logger.error(f"❌ MongoDB Setup Error: {e}")

# ==========================================
# GLOBALS & STATE MANAGEMENT
# ==========================================
bot = TelegramClient('master_bot', API_ID, API_HASH)
active_clients: Dict[str, TelegramClient] = {}
active_session_keys = set()
client_lifecycle_lock = asyncio.Lock()
bot_username = "@Tecxo"
dialog_cache: Dict[str, List] = {}
last_cache_update: Dict[str, float] = {}
cache_ttl = 300

BROADCAST_CONCURRENCY = max(1, int(os.getenv("BROADCAST_CONCURRENCY", "50")))
BROADCAST_SEMAPHORE = asyncio.Semaphore(BROADCAST_CONCURRENCY)
BROADCAST_CHAT_CONCURRENCY = max(1, int(os.getenv("BROADCAST_CHAT_CONCURRENCY", "15")))
BROADCAST_CHAT_SEMAPHORE = asyncio.Semaphore(BROADCAST_CHAT_CONCURRENCY)
JOIN_SEMAPHORE = asyncio.Semaphore(30)
REPORT_SEMAPHORE = asyncio.Semaphore(25)
broadcast_cycle_lock = asyncio.Lock()
broadcast_cycle_task: Optional[asyncio.Task] = None
_background_tasks: set[asyncio.Task] = set()

def create_supervised_task(coro, *, name: str):
    task = asyncio.create_task(coro, name=name)
    _background_tasks.add(task)

    def _finish(done_task: asyncio.Task):
        _background_tasks.discard(done_task)
        if done_task.cancelled():
            logger.info("Background task cancelled: %s", name)
            return
        try:
            error = done_task.exception()
            if error:
                logger.error("Background task failed: %s", name, exc_info=(type(error), error, error.__traceback__))
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Failed to inspect background task: %s", name)

    task.add_done_callback(_finish)
    return task

# ==========================================
# DATABASE HELPERS
# ==========================================
async def update_stats(sent=0, failed=0, joined=0):
    try:
        await bot_stats_col.update_one(
            {"_id": 1},
            {"$inc": {"total_sent": sent, "total_failed": failed, "total_joined": joined, "errors_today": failed}}
        )
    except Exception as e:
        logger.error(f"❌ Failed to update stats: {e}")

async def log_error(phone: str, error_type: str, details: str, chat_id: Optional[str] = None):
    try:
        await error_log_col.insert_one({
            "phone": phone,
            "error_type": error_type,
            "details": details,
            "chat_id": chat_id,
            "timestamp": datetime.now(timezone.utc)
        })
    except Exception as e:
        logger.error(f"❌ Failed to log error: {e}")

async def is_authorized(user_id):
    try:
        user = await auth_users_col.find_one({"_id": user_id})
        return bool(user)
    except Exception as e:
        logger.error(f"❌ Authorization check failed: {e}")
        return False

# ==========================================
# AUTO-RECOVERY SYSTEM
# ==========================================
async def reconnect_client(phone: str) -> bool:
    client = active_clients.get(phone)
    if client is None:
        return False
    async with client_lifecycle_lock:
        if client.is_connected():
            return True
        for attempt in range(3):
            try:
                await asyncio.wait_for(client.connect(), timeout=20)
                if client.is_connected():
                    logger.info("Reconnected client: %s", phone)
                    return True
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Reconnect failed for %s attempt=%d: %s", phone, attempt + 1, type(exc).__name__)
                if attempt < 2:
                    await asyncio.sleep(min(2 ** attempt, 5))
    logger.error("Reconnect exhausted for %s", phone)
    return False

async def health_check_clients():
    while True:
        try:
            await asyncio.sleep(300)
            for phone, client in list(active_clients.items()):
                try:
                    if not client.is_connected():
                        if not await reconnect_client(phone):
                            await accounts_col.update_one({"_id": phone}, {"$set": {"status": "dead"}})
                except Exception as e:
                    logger.error(f"❌ Health check failed for {phone}: {e}")
        except Exception as e:
            logger.error(f"❌ Health check loop error: {e}")

# ==========================================
# DIALOG CACHING & OPTIMIZATION
# ==========================================
async def get_cached_dialogs(client: TelegramClient, phone: str, force_refresh: bool = False) -> List:
    try:
        current_time = time.time()
        if phone in dialog_cache and phone in last_cache_update:
            cache_age = current_time - last_cache_update[phone]
            old_count = len(dialog_cache.get(phone, []))
            
            if cache_age < cache_ttl and not force_refresh:
                logger.debug(f"[Cache] Using cached dialogs for {phone} ({old_count} chats)")
                return dialog_cache[phone]
        
        logger.info(f"[Cache] Fetching fresh dialogs for {phone}")
        dialogs = await client.get_dialogs()
        new_count = len(dialogs)
        
        if phone in dialog_cache:
            old_count = len(dialog_cache[phone])
            diff = new_count - old_count
            if abs(diff) > 0:
                logger.info(f"[Cache] Dialog count changed: {old_count} → {new_count} ({diff:+d} chats)")
        
        dialog_cache[phone] = dialogs
        last_cache_update[phone] = current_time
        logger.debug(f"[Cache] Cache updated for {phone}: {new_count} dialogs")
        
        return dialogs
    except Exception as e:
        logger.error(f"Failed to get dialogs for {phone}: {type(e).__name__}: {str(e)[:100]}")
        return dialog_cache.get(phone, [])

# ==========================================
# SMART JOIN DETECTION AND AUTO-JOIN PIPELINE
# ==========================================
SMART_FORCE_WORDS = ("join", "subscribe", "sub", "member", "channel", "group", "required", "must", "access", "continue")
SMART_BUTTON_WORDS = ("join", "subscribe", "i've joined", "ive joined", "check", "continue")
SMART_LINK_RE = re.compile(r"https?://(?:www\.)?(?:t\.me|telegram\.me)/(?:joinchat/|\+|addlist/)?[A-Za-z0-9_+\-/]+", re.I)
SMART_USERNAME_RE = re.compile(r"(?<![A-Za-z0-9_])@[A-Za-z][A-Za-z0-9_]{3,31}")
smart_join_locks = defaultdict(asyncio.Lock)
smart_join_suppressed_groups = defaultdict(set)
smart_join_target_sources = defaultdict(set)

def _smart_url(value: str) -> Optional[dict]:
    value = value.strip().rstrip(".,);]>")
    match = SMART_LINK_RE.fullmatch(value)
    if not match:
        return None
    raw = match.group(0)
    path = re.split(r"(?:t\.me|telegram\.me)/", raw, flags=re.I)[-1].split("?", 1)[0].strip("/")
    if not path or path.lower().endswith("bot") or path.lower().startswith(("share", "addstickers")):
        return None
    if path.startswith("joinchat/") or path.startswith("+"):
        return {"key": raw.lower(), "link": raw, "kind": "invite", "target": path.split("/", 1)[-1].lstrip("+")}
    if path.startswith("addlist/"):
        return {"key": raw.lower(), "link": raw, "kind": "chatlist", "target": path[8:]}
    if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{3,31}", path):
        return {"key": path.lower(), "link": raw, "kind": "public", "target": path}
    return None

def _smart_candidates(message) -> List[dict]:
    values = []
    text = getattr(message, "raw_text", None) or getattr(message, "message", "") or ""
    values.extend(SMART_LINK_RE.findall(text))
    values.extend(SMART_USERNAME_RE.findall(text))
    for row in getattr(message, "buttons", None) or []:
        for button in row:
            url = getattr(button, "url", None)
            if url:
                values.append(url)
            label = getattr(button, "text", "") or ""
            if any(word in label.lower() for word in SMART_BUTTON_WORDS):
                values.append(label)
    results, seen = [], set()
    for value in values:
        parsed = _smart_url(value) if value.startswith(("http://", "https://")) else _smart_url("https://t.me/" + value[1:]) if value.startswith("@") else None
        if parsed and parsed["key"] not in seen:
            seen.add(parsed["key"])
            results.append(parsed)
    return results

def _smart_score(message, candidates: List[dict]) -> Tuple[int, List[str]]:
    text = (getattr(message, "raw_text", None) or getattr(message, "message", "") or "").lower()
    reasons = [word for word in SMART_FORCE_WORDS if word in text]
    button_text = " ".join(getattr(button, "text", "") or "" for row in (getattr(message, "buttons", None) or []) for button in row).lower()
    button_hits = [word for word in SMART_BUTTON_WORDS if word in button_text]
    score = min(40, len(reasons) * 6) + min(35, len(button_hits) * 18)
    if getattr(message, "sender", None) and getattr(message.sender, "bot", False):
        score += 15
    if candidates:
        score += 10
    return min(score, 100), reasons + button_hits

async def _smart_membership(client, target) -> str:
    try:
        entity = await client.get_input_entity(target)
        await client(GetParticipantRequest(entity, await client.get_me()))
        return "member"
    except UserAlreadyParticipantError:
        return "member"
    except Exception as exc:
        name = type(exc).__name__.lower()
        if "participant" in name and "left" in name:
            return "left"
        if "banned" in name:
            return "banned"
        return "unknown"

async def _smart_join_one(client, phone: str, candidate: dict) -> str:
    for attempt in range(2):
        try:
            if candidate["kind"] == "invite":
                await client(ImportChatInviteRequest(candidate["target"]))
            elif candidate["kind"] == "chatlist":
                check = await client(CheckChatlistInviteRequest(candidate["target"]))
                if not getattr(check, "peers", None):
                    return "invalid"
                await client(JoinChatlistInviteRequest(slug=candidate["target"], peers=check.peers))
            else:
                await client(JoinChannelRequest(candidate["target"]))
            state = await _smart_membership(client, candidate["target"])
            return "member" if state == "member" else state
        except UserAlreadyParticipantError:
            return "member"
        except FloodWaitError as exc:
            logger.warning(f"[SMART JOIN] phone={phone} link={candidate['link']} flood_wait={exc.seconds}")
            if attempt == 0:
                await asyncio.sleep(min(max(exc.seconds, 1), 60))
        except (InviteHashExpiredError, InviteHashInvalidError, ChannelPrivateError):
            return "invalid"
        except UserBannedInChannelError:
            return "banned"
        except Exception as exc:
            logger.info(f"[SMART JOIN] phone={phone} link={candidate['link']} action_error={type(exc).__name__}")
            return "error"
    return "flood_wait"

async def smart_join_handler(event):
    try:
        config = await ad_config_col.find_one({"_id": 1}) or {}
        smart_config = config.get("smart_join", {})
        if smart_config.get("enabled", True) is False:
            return
        message = event.message
        if not (getattr(message, "sender", None) and getattr(message.sender, "bot", False)):
            return
        candidates = _smart_candidates(message)
        score, reasons = _smart_score(message, candidates)
        if score < 55 or not candidates:
            return
        phone = getattr(event.client, "_phone", "unknown")
        group_id = int(getattr(event, "chat_id", 0) or 0)
        source_id = int(getattr(message, "id", 0) or 0)

        if group_id in smart_join_suppressed_groups[phone]:
            return
        async with smart_join_locks[f"{phone}:{group_id}"]:
            if group_id in smart_join_suppressed_groups[phone]:
                return
            valid = []
            for candidate in candidates:
                await smart_requirements_col.update_one(
                    {"group_id": group_id, "source_message_id": source_id, "channel_key": candidate["key"]},
                    {"$setOnInsert": {"group_id": group_id, "source_message_id": source_id, "channel_key": candidate["key"], "link": candidate["link"], "kind": candidate["kind"], "target": candidate["target"], "detected_at": datetime.now(timezone.utc), "expires_at": datetime.now(timezone.utc) + timedelta(minutes=15), "reasons": reasons, "enabled": True}},
                    upsert=True
                )
                valid.append(candidate)
            results = []
            for candidate in valid:
                result = await _smart_join_one(event.client, phone, candidate)
                results.append((candidate, result))
                logger.info(f"[SMART JOIN] group={group_id} user={phone} source_message={source_id} link={candidate['link']} reason={','.join(reasons)} membership={result} action=auto_join")
                if result == "member":
                    aliases = {candidate["key"].lower(), str(candidate.get("target", "")).lower()}
                    try:
                        entity = await event.client.get_entity(candidate["target"])
                        if getattr(entity, "id", None) is not None:
                            aliases.add(str(entity.id))
                        if getattr(entity, "username", None):
                            aliases.add(entity.username.lower())
                    except Exception:
                        pass
                    for alias in filter(None, aliases):
                        smart_join_target_sources[(phone, alias)].add(group_id)
                    await smart_requirements_col.update_one({"group_id": group_id, "source_message_id": source_id, "channel_key": candidate["key"]}, {"$set": {"membership": result, "joined_at": datetime.now(timezone.utc)}})
            failed = [candidate for candidate, result in results if result != "member"]
            if failed:
                logger.info(f"[SMART JOIN] verification remains available for phone={phone} group={group_id} failed={len(failed)}")
    except Exception as exc:
        logger.info(f"[SMART JOIN] fail_open error={type(exc).__name__}")

auto_join_handler = smart_join_handler
joined_accounts = set()

# ==========================================
# CLIENT LOADER & VERIFICATION
# ==========================================
async def load_and_verify_clients():
    global bot_username
    try:
        me = await bot.get_me()
        bot_username = f"@{me.username}" if me.username else "@Tecxo"
    except Exception as e:
        logger.warning(f"⚠️ Failed to fetch bot username: {e}")
    
    cursor = accounts_col.find({})
    accounts = await cursor.to_list(length=None)
    logger.info(f"📱 Initializing {len(accounts)} accounts from MongoDB...")
    
    loaded, dead = 0, 0
    
    for acc_index, acc in enumerate(accounts):
        if not isinstance(acc, dict):
            logger.error("Skipping malformed account record: %s", type(acc).__name__)
            dead += 1
            continue
        phone = acc.get('_id')
        if not phone:
            logger.warning("Skipping account without _id")
            continue
        
        session_str = acc.get('session_string')
        if not session_str:
            logger.warning(f"⚠️ Skipping {phone}: no session_string in database")
            await log_error(phone, "MISSING_SESSION", "Account has no session_string field")
            dead += 1
            continue
        
        if acc.get('joined_first_link', False):
            joined_accounts.add(phone)
        
        client = None
        session_key = (phone, session_str)
        try:
            async with client_lifecycle_lock:
                if phone in active_clients:
                    logger.warning(f"Skipping duplicate client for {phone}")
                    continue
                if session_key in active_session_keys:
                    logger.warning(f"Skipping duplicate StringSession for {phone}")
                    continue
                active_session_keys.add(session_key)

            account_api_id = acc.get('api_id')
            account_api_hash = acc.get('api_hash')
            
            if account_api_id and account_api_hash:
                client = TelegramClient(StringSession(session_str), account_api_id, account_api_hash)
            else:
                api_config = get_api_for_account(acc_index, len(accounts))
                client = TelegramClient(StringSession(session_str), api_config['api_id'], api_config['api_hash'])
            
            await asyncio.wait_for(client.connect(), timeout=30)
            
            if await asyncio.wait_for(client.is_user_authorized(), timeout=20):
                await asyncio.wait_for(client.get_me(), timeout=20)
                active_clients[phone] = client
                client.add_event_handler(auto_join_handler, events.NewMessage(incoming=True))
                await accounts_col.update_one({"_id": phone}, {"$set": {"status": "active", "last_connected": time.time()}})
                loaded += 1
                logger.info(f"✅ Loaded and verified: {phone}")
            else:
                raise AuthKeyUnregisteredError("Session no longer authorized")
        
        except (AuthKeyUnregisteredError, AuthKeyDuplicatedError, UnauthorizedError) as e:
            if client is not None and client.is_connected():
                await client.disconnect()
            active_session_keys.discard(session_key)
            logger.warning(f"❌ Dead session for {phone}: {type(e).__name__}")
            await accounts_col.update_one({"_id": phone}, {"$set": {"status": "dead"}})
            await log_error(phone, "DEAD_SESSION", str(e))
            dead += 1
        except Exception as e:
            if client is not None:
                try:
                    if client.is_connected():
                        await client.disconnect()
                except Exception:
                    logger.exception(f"Failed to disconnect replacement client for {phone}")
            active_session_keys.discard(session_key)
            logger.error(f"❌ Failed to load {phone}: {type(e).__name__}: {str(e)[:100]}")
            await accounts_col.update_one({"_id": phone}, {"$set": {"status": "dead"}})
            await log_error(phone, "LOAD_ERROR", str(e))
            dead += 1
    
    logger.info(f"📊 Initialization complete. Active: {loaded}, Dead: {dead}")

# ==========================================
# ULTRA-OPTIMIZED PARALLEL BROADCAST ENGINE
# ==========================================
def _is_disconnected_error(exc: BaseException) -> bool:
    text = str(exc).lower()
    return isinstance(exc, ConnectionError) or "cannot send requests while disconnected" in text or "disconnected" in text

async def _reconnect_once(phone: str, client: TelegramClient) -> bool:
    logger.warning("[ACCOUNT] %s attempting reconnect", phone)
    try:
        if client.is_connected():
            return True
        await asyncio.wait_for(client.connect(), timeout=20)
        if client.is_connected():
            logger.info("[ACCOUNT] %s reconnected", phone)
            return True
    except Exception as exc:
        logger.warning("[ACCOUNT] %s reconnect error: %s", phone, type(exc).__name__)
    logger.error("[ACCOUNT] %s reconnect failed - stopping worker", phone)
    return False

async def process_broadcast_for_account(client: TelegramClient, phone: str, messages: List, target_type: str, specific_ids: set, config: dict) -> Tuple[str, int, int, str]:
    started_at = time.monotonic()
    messages = tuple(messages or ())
    specific_ids = set(specific_ids or ())
    targets = []
    stop_reason = "completed"
    reconnect_attempted = False
    account_stop_event = asyncio.Event()

    async def ensure_connected() -> bool:
        nonlocal reconnect_attempted
        if account_stop_event.is_set():
            return False
        if client.is_connected():
            return True
        logger.warning("[ACCOUNT] %s connection lost", phone)
        if reconnect_attempted:
            account_stop_event.set()
            return False
        reconnect_attempted = True
        reconnected = await _reconnect_once(phone, client)
        if not reconnected:
            account_stop_event.set()
        return reconnected

    try:
        if not await ensure_connected():
            stop_reason = "disconnected"
            return phone, 0, len(messages), stop_reason
        dialogs = await get_cached_dialogs(client, phone)
        for dialog in dialogs:
            try:
                is_bot = dialog.is_user and getattr(dialog.entity, "bot", False)
                matches = (
                    target_type == "groups" and dialog.is_group or
                    target_type == "channels" and dialog.is_channel and not dialog.is_group or
                    target_type == "private" and dialog.is_user and not is_bot or
                    target_type == "all" and not is_bot or
                    target_type == "specific_groups" and dialog.is_group and dialog.id in specific_ids or
                    target_type == "specific_channels" and dialog.is_channel and not dialog.is_group and dialog.id in specific_ids
                )
                if matches:
                    targets.append(dialog)
            except Exception:
                logger.exception("[%s] Dialog filter failed", phone)

        logger.info("[%s] Broadcast start: messages=%d chats=%d", phone, len(messages), len(targets))

        async def send_message_once(dialog, msg):
            if not await ensure_connected():
                raise ConnectionError("client disconnected")
            if msg.get("media"):
                attributes = []
                if msg.get("is_sticker"):
                    attributes.append(DocumentAttributeSticker(alt="✅", stickerset=InputStickerSetEmpty()))
                await client.send_file(dialog.id, msg["media"], caption=msg.get("text") if not msg.get("is_sticker") else None, attributes=attributes or None)
            else:
                await client.send_message(dialog.id, msg.get("text", ""))

        async def send_to_chat(dialog):
            dialog_title = getattr(dialog.entity, "title", getattr(dialog.entity, "first_name", "Unknown"))
            sent = failed = 0
            async with BROADCAST_CHAT_SEMAPHORE:
                for msg in messages:
                    if account_stop_event.is_set():
                        return sent, failed, "disconnected"
                    try:
                        await send_message_once(dialog, msg)
                        sent += 1

                        aliases = {str(dialog.id).lower()}
                        username = getattr(dialog.entity, "username", None)
                        if username:
                            aliases.add(username.lower())
                        suppressed = set()
                        for alias in aliases:
                            suppressed.update(smart_join_target_sources.get((phone, alias), ()))
                        if suppressed:
                            smart_join_suppressed_groups[phone].update(suppressed)
                    except SlowModeWaitError as exc:
                        failed += len(messages) - sent
                        logger.warning("[%s] SlowMode %ss on chat %s; skipping chat", phone, getattr(exc, "seconds", 0), dialog_title)
                        await log_error(phone, "SLOW_MODE", f"Chat: {dialog_title}, wait: {getattr(exc, 'seconds', 0)}s")
                        break
                    except FloodWaitError as exc:
                        remaining = len(messages) - sent
                        failed += remaining
                        logger.warning("[%s] FloodWait %ss on chat %s; isolating worker", phone, exc.seconds, dialog_title)
                        await asyncio.sleep(exc.seconds)
                        await log_error(phone, "FLOOD_WAIT", f"Chat: {dialog_title}, wait: {exc.seconds}s")
                        break
                    except (ChatWriteForbiddenError, ChatAdminRequiredError, UserBannedInChannelError, ChannelPrivateError) as exc:
                        failed += len(messages) - sent
                        await log_error(phone, type(exc).__name__, f"Chat: {dialog_title}")
                        break
                    except (UnauthorizedError, AuthKeyUnregisteredError) as exc:
                        failed += len(messages) - sent
                        await accounts_col.update_one({"_id": phone}, {"$set": {"status": "dead"}})
                        await log_error(phone, "SESSION_EXPIRED", str(exc))
                        break
                    except Exception as exc:
                        if _is_disconnected_error(exc):
                            logger.warning("[ACCOUNT] %s connection lost", phone)
                            if not reconnect_attempted:
                                reconnect_attempted = True
                                if await _reconnect_once(phone, client):
                                    try:
                                        await send_message_once(dialog, msg)
                                        sent += 1
                                        continue
                                    except Exception as retry_exc:
                                        exc = retry_exc
                            stop_reason = "disconnected"
                            failed += len(messages) - sent
                            account_stop_event.set()
                            logger.error("[ACCOUNT] %s worker stopped: disconnected", phone)
                            return sent, failed, "disconnected"
                        failed += 1
                        logger.error("[%s] Send failed for chat %s: %s", phone, dialog_title, type(exc).__name__)
            return sent, failed, "disconnected" if account_stop_event.is_set() else "completed"

        results = await asyncio.gather(*(send_to_chat(dialog) for dialog in targets), return_exceptions=True)
        total_sent = total_failed = 0
        for result in results:
            if isinstance(result, BaseException):
                total_failed += len(messages)
                if isinstance(result, asyncio.CancelledError):
                    account_stop_event.set()
                    stop_reason = "cancelled"
                logger.error("[%s] Chat worker failed: %s", phone, type(result).__name__)
                continue
            sent, failed, worker_reason = result
            total_sent += sent
            total_failed += failed
            if worker_reason != "completed":
                stop_reason = worker_reason
        logger.info("[%s] Broadcast end: sent=%d failed=%d duration=%.1fs", phone, total_sent, total_failed, time.monotonic() - started_at)
        return phone, total_sent, total_failed, stop_reason
    except asyncio.CancelledError:
        account_stop_event.set()
        logger.warning("[ACCOUNT] %s worker stopped: cancelled", phone)
        return phone, 0, len(messages), "cancelled"
    except Exception as exc:
        if _is_disconnected_error(exc):
            account_stop_event.set()
            logger.error("[ACCOUNT] %s worker stopped: disconnected", phone)
            return phone, 0, len(messages), "disconnected"
        logger.exception("[%s] Broadcast account failed", phone)
        return phone, 0, len(messages), "exception"

async def run_broadcast_cycle(config: dict, accounts_list: list[tuple[str, TelegramClient]]):
    cycle_start = time.time()
    messages = list(config.get("msgs", []))
    target_type = config.get("target_type", "all")
    specific_ids = set(config.get("target_ids", []))
    mode = config.get("broadcast_mode", "parallel")
    logger.info(f"Broadcast START: accounts={len(accounts_list)} messages={len(messages)} mode={mode}")

    async def run_account(phone, client):
        async with BROADCAST_SEMAPHORE:
            try:
                return await process_broadcast_for_account(client, phone, messages, target_type, specific_ids, config)
            except asyncio.CancelledError:
                logger.warning("[ACCOUNT] %s worker stopped: cancelled", phone)
                return phone, 0, len(messages), "cancelled"
            except BaseException as exc:
                logger.exception("[ACCOUNT] %s worker failed: %s", phone, type(exc).__name__)
                return phone, 0, len(messages), "exception"

    try:
        results = []
        if mode == "sequential":
            account_delay = config.get("account_delay", 120)
            for index, (phone, client) in enumerate(accounts_list):
                results.append(await run_account(phone, client))
                if index < len(accounts_list) - 1:
                    await asyncio.sleep(account_delay)
        else:
            results = await asyncio.gather(*(run_account(phone, client) for phone, client in accounts_list), return_exceptions=True)

        total_sent = total_failed = failed_accounts = 0
        for result in results:
            if isinstance(result, BaseException):
                failed_accounts += 1
                logger.error("Broadcast worker error: %s", type(result).__name__)
                continue
            phone, sent, failed, stopped_reason = result
            total_sent += sent
            total_failed += failed
            if failed or stopped_reason != "completed":
                failed_accounts += 1
            if stopped_reason != "completed":
                logger.warning("[ACCOUNT] %s worker stopped: %s", phone, stopped_reason)
        duration = time.time() - cycle_start
        await update_stats(sent=total_sent, failed=total_failed)
        await ad_config_col.update_one({"_id": 1}, {"$set": {"last_run": cycle_start}})
        logger.info(f"Broadcast END: sent={total_sent} failed={total_failed} failed_accounts={failed_accounts} duration={duration:.1f}s")
    except asyncio.CancelledError:
        logger.warning("Broadcast cycle cancelled")
        raise
    except Exception:
        logger.exception("Broadcast cycle failed before completion")
    finally:
        logger.debug("Broadcast cycle cleanup complete")

async def spammer_engine():
    global broadcast_cycle_task
    logger.info("Broadcast Engine Started")
    consecutive_errors = 0
    last_check = 0.0

    while True:
        try:
            now = time.time()
            if now - last_check < 1:
                await asyncio.sleep(0.1)
                continue
            last_check = now
            config = normalize_broadcast_config(await ad_config_col.find_one({"_id": 1}) or {})
            messages = config.get("msgs", [])
            last_run = config.get("last_run", 0)
            interval = config.get("interval", 300)
            should_run = config.get("status") == "active" and messages and active_clients and (not last_run or now >= last_run + interval)

            if should_run and (broadcast_cycle_task is None or broadcast_cycle_task.done()):
                async with broadcast_cycle_lock:
                    if broadcast_cycle_task is None or broadcast_cycle_task.done():
                        accounts_list = list(active_clients.items())
                        broadcast_cycle_task = create_supervised_task(run_broadcast_cycle(config, accounts_list), name="broadcast-cycle")
                        consecutive_errors = 0
        except Exception:
            consecutive_errors += 1
            logger.exception("Broadcast engine error")
            if consecutive_errors > 10:
                consecutive_errors = 0
                await asyncio.sleep(5)
        await asyncio.sleep(0.1)

# ==========================================
# DASHBOARD & UI
# ==========================================
async def safe_callback_answer(event, *args, **kwargs):
    try:
        await event.answer(*args, **kwargs)
    except (QueryIdInvalidError, RPCError):
        logger.debug("Ignoring expired callback query")
    except Exception:
        logger.debug("Callback answer failed", exc_info=True)

async def get_dashboard_text():
    try:
        total_hosted = await accounts_col.count_documents({})
        active_accs = len(active_clients)
        dead_accs = total_hosted - active_accs
        
        config = await ad_config_col.find_one({"_id": 1}) or {}
        status = config.get('status', 'paused')
        msgs = config.get('msgs', [])
        interval = config.get('interval', 300)
        acc_cooldown = config.get('acc_cooldown', 5)
        broadcast_mode = config.get('broadcast_mode', 'parallel')
        account_delay = config.get('account_delay', 120) if broadcast_mode == 'sequential' else 0
        
        stats = await bot_stats_col.find_one({"_id": 1}) or {}
        tot_sent = stats.get('total_sent', 0)
        tot_failed = stats.get('total_failed', 0)
        tot_joined = stats.get('total_joined', 0)
        errors_today = stats.get('errors_today', 0)
        
        total_ops = tot_sent + tot_failed
        success_rate = (tot_sent / total_ops * 100) if total_ops > 0 else 0
        
        ram_usage = psutil.virtual_memory().percent
        cpu_usage = psutil.cpu_percent(interval=0.1)
        
        is_set = "Set 🟢" if msgs else "Not Set 🔴"
        ad_status = "Active ▶️" if status == 'active' else "Paused ⏸"
        mode_icon = "⚡" if broadcast_mode == 'parallel' else "📱"
        
        text = (
            f"╰_╯ **{bot_username} Ads DASHBOARD v2.0** ❞\n\n"
            f"**📊 Server Analytics (MongoDB):**\n"
            f"• Hosted Accounts: `{total_hosted}/500`\n"
            f"• Online Accounts: `{active_accs}` 🟢\n"
            f"• Dead Sessions: `{dead_accs}` 🔴\n"
            f"• Success Rate: `{success_rate:.1f}%` 📈\n\n"
            f"**⚙️ Ad Configuration:**\n"
            f"• Ad Message: **{is_set}**\n"
            f"• Cycle Interval: `{interval}s`\n"
            f"• Message Gap: `{acc_cooldown}s`\n"
            f"• Broadcast Mode: **{broadcast_mode.upper()} {mode_icon}**\n"
            f"• Account Delay: `{account_delay}s`\n"
            f"• Advertising Status: **{ad_status}**\n\n"
            f"**📈 Global Statistics:**\n"
            f"• Total Sent Ads: `{tot_sent}`\n"
            f"• Failed Ads: `{tot_failed}`\n"
            f"• Successful Joins: `{tot_joined}`\n"
            f"• Errors Today: `{errors_today}`\n\n"
            f"**💻 System Resources:**\n"
            f"• RAM Usage: `{ram_usage:.1f}%`\n"
            f"• CPU Usage: `{cpu_usage:.1f}%`\n\n"
            f"╰_╯ Choose an action below ❞"
        )
        return text
    except Exception as e:
        logger.error(f"❌ Dashboard generation error: {e}")
        return "❌ Error loading dashboard."

def dashboard_buttons():
    return [
        [Button.inline("Add Account ➕", b"add_account"), Button.inline("My Accounts 📋", b"my_accounts")],
        [Button.inline("Set Ad Message", b"set_ad"), Button.inline("Set Time Interval", b"set_time")],
        [Button.inline("Start Ads ▶️", b"start_ads"), Button.inline("Stop Ads ⏸", b"stop_ads")],
        [Button.inline("Broadcast Mode ⚡", b"broadcast_mode"), Button.inline("Account Delay ⏱", b"set_account_delay")],
        [Button.inline("Delete Accounts", b"del_accounts"), Button.inline("Join Link 🔗", b"join_all")],
        [Button.inline("Mass Report ⚠️", b"mass_report"), Button.inline("Acc Cooldown ⏳", b"set_acc_cooldown")],
        [Button.inline("Download DB 💾", b"download_db"), Button.inline("Logs 📋", b"view_logs")],
        [Button.inline("Smart Join Detection 🧠", b"smart_join_menu")],
        [Button.inline("Manage Access 🔐", b"manage_auth"), Button.inline("Reset DB 🔄", b"reset_db_confirm")],
        [Button.inline("Close ❌", b"close_menu")]
    ]

# ==========================================
# BOT COMMANDS & CALLBACKS
# ==========================================

@bot.on(events.NewMessage(pattern='/start'))
async def start_handler(event):
    try:
        if not await is_authorized(event.sender_id):
            return await event.respond("🚫 **You are not authorized to use this bot.**")
        msg = await get_dashboard_text()
        await event.respond(msg, buttons=dashboard_buttons())
    except Exception as e:
        logger.error(f"❌ Start command error: {e}")

@bot.on(events.CallbackQuery(data=b"main_menu"))
async def back_to_main(event):
    try:
        if not await is_authorized(event.sender_id):
            return await safe_callback_answer(event, "🚫 Unauthorized!", alert=True)
        msg = await get_dashboard_text()
        await event.edit(msg, buttons=dashboard_buttons())
    except Exception as e:
        logger.error(f"❌ Main menu callback error: {e}")

@bot.on(events.CallbackQuery(data=b"smart_join_menu"))
async def smart_join_menu(event):
    if not await is_authorized(event.sender_id):
        return await safe_callback_answer(event, "🚫 Unauthorized!", alert=True)
    config = await ad_config_col.find_one({"_id": 1}) or {}
    enabled = config.get("smart_join", {}).get("enabled", True)
    count = await smart_requirements_col.count_documents({})
    text = ("🧠 **SMART JOIN DETECTION**\n\n"
            f"Status: **{'Enabled' if enabled else 'Disabled'}**\n"
            f"Cached requirements: `{count}`\n\n"
            "High-confidence force-join responses are detected per group and joined with bounded retries.")
    buttons = [
        [Button.inline("Disable" if enabled else "Enable", b"smart_join_toggle")],
        [Button.inline("View Detected Requirements", b"smart_join_view")],
        [Button.inline("Clear Detected Requirements", b"smart_join_clear")],
        [Button.inline("Re-scan Requirements", b"smart_join_rescan")],
        [Button.inline("Back 🔙", b"main_menu")]
    ]
    await event.edit(text, buttons=buttons)

@bot.on(events.CallbackQuery(data=b"smart_join_toggle"))
async def smart_join_toggle(event):
    if not await is_authorized(event.sender_id):
        return await safe_callback_answer(event, "🚫 Unauthorized!", alert=True)
    config = await ad_config_col.find_one({"_id": 1}) or {}
    enabled = not config.get("smart_join", {}).get("enabled", True)
    await ad_config_col.update_one({"_id": 1}, {"$set": {"smart_join.enabled": enabled}})
    await safe_callback_answer(event, f"Smart Join {'enabled' if enabled else 'disabled'}.", alert=True)
    await smart_join_menu(event)

@bot.on(events.CallbackQuery(data=b"smart_join_view"))
async def smart_join_view(event):
    if not await is_authorized(event.sender_id):
        return await safe_callback_answer(event, "🚫 Unauthorized!", alert=True)
    docs = await smart_requirements_col.find({}).sort("detected_at", -1).to_list(length=20)
    if not docs:
        text = "🧠 **SMART JOIN REQUIREMENTS**\n\nNo active requirements found."
    else:
        lines = []
        for doc in docs:
            lines.append(f"• Group `{doc.get('group_id')}` — {doc.get('link')}\n  `{doc.get('membership', 'pending')}`")
        text = "🧠 **SMART JOIN REQUIREMENTS**\n\n" + "\n".join(lines)
    await event.edit(text[:4000], buttons=[[Button.inline("Back", b"smart_join_menu")]])

@bot.on(events.CallbackQuery(data=b"smart_join_clear"))
async def smart_join_clear(event):
    if not await is_authorized(event.sender_id):
        return await safe_callback_answer(event, "🚫 Unauthorized!", alert=True)
    result = await smart_requirements_col.delete_many({})
    await safe_callback_answer(event, f"Cleared {result.deleted_count} requirement(s).", alert=True)
    await smart_join_menu(event)

@bot.on(events.CallbackQuery(data=b"smart_join_rescan"))
async def smart_join_rescan(event):
    if not await is_authorized(event.sender_id):
        return await safe_callback_answer(event, "🚫 Unauthorized!", alert=True)
    docs = await smart_requirements_col.find({"membership": {"$ne": "member"}}).to_list(length=100)
    checked = 0
    for doc in docs:
        for phone, client in list(active_clients.items()):
            result = await _smart_join_one(client, phone, {"kind": doc.get("kind", "public"), "target": doc.get("target", doc.get("channel_key")), "link": doc.get("link"), "key": doc.get("channel_key")})
            checked += 1
            if result == "member":
                await smart_requirements_col.update_one({"_id": doc["_id"]}, {"$set": {"membership": result, "rechecked_at": datetime.now(timezone.utc)}})
                break
    await safe_callback_answer(event, f"Re-scan checked {checked} account(s).", alert=True)
    await smart_join_menu(event)

@bot.on(events.CallbackQuery(pattern=rb"smart_verify:(-?\d+):(\d+)"))
async def smart_verify(event):
    if not await is_authorized(event.sender_id):
        return await safe_callback_answer(event, "🚫 Unauthorized!", alert=True)
    try:
        _, group_id, source_id = event.data.decode().split(":")
        docs = await smart_requirements_col.find({"group_id": int(group_id), "source_message_id": int(source_id)}).to_list(length=20)
        verified = 0
        for doc in docs:
            for phone, client in list(active_clients.items()):
                candidate = {"kind": doc.get("kind", "public"), "target": doc.get("target", doc.get("channel_key")), "link": doc.get("link"), "key": doc.get("channel_key")}
                if await _smart_membership(client, candidate["target"]) == "member":
                    await smart_requirements_col.update_one({"_id": doc["_id"]}, {"$set": {"membership": "member", "verified_at": datetime.now(timezone.utc)}})
                    verified += 1
                    break
        await safe_callback_answer(event, f"Verified {verified}/{len(docs)} requirement(s).", alert=True)
    except Exception as exc:
        logger.info(f"[SMART JOIN] verify error={type(exc).__name__}")
        await safe_callback_answer(event, "Verification unavailable; try again later.", alert=True)

@bot.on(events.CallbackQuery(data=b"reset_db_confirm"))
async def reset_db_confirm(event):
    sender = event.sender_id
    await event.delete()
    
    try:
        async with bot.conversation(sender, timeout=60) as conv:
            text = (
                "⚠️ **WARNING: RESET DATABASE** ⚠️\n\n"
                "This will DELETE ALL:\n"
                "• All hosted accounts\n"
                "• All ad messages\n"
                "• All settings\n"
                "• Everything!\n\n"
                "Type `RESET` to confirm (case-sensitive):"
            )
            await conv.send_message(text, buttons=[[Button.inline("Cancel", b"main_menu")]])
            
            resp = await conv.get_response()
            if resp.text.strip() == "RESET":
                await accounts_col.delete_many({})
                await ad_config_col.delete_many({})
                await bot_stats_col.delete_many({})
                
                active_clients.clear()
                joined_accounts.clear()
                
                await ad_config_col.insert_one({
                    "_id": 1,
                    "status": "paused",
                    "msgs": [],
                    "interval": 300,
                    "last_run": 0,
                    "target_type": "all",
                    "target_ids": [],
                    "acc_cooldown": 5
                })
                
                logger.warning("⚠️ DATABASE COMPLETELY RESET! All state cleared.")
                await conv.send_message("✅ **Database completely reset!**\n\nBot restarted fresh.", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
            else:
                await conv.send_message("❌ Reset cancelled.", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
    except Exception as e:
        logger.error(f"❌ Reset DB error: {e}")

@bot.on(events.CallbackQuery(data=b"close_menu"))
async def close_menu(event):
    try:
        await event.delete()
    except Exception as e:
        logger.error(f"❌ Close menu error: {e}")

# --- 1. ADD ACCOUNT ---
@bot.on(events.CallbackQuery(data=b"add_account"))
async def add_account_handler(event):
    sender = event.sender_id
    await event.delete()
    
    temp_client = None
    try:
        async with bot.conversation(sender, timeout=300) as conv:
            text = (
                "╰_╯ **HOST NEW ACCOUNT** ❞\n\n"
                "Secure Account Hosting via MongoDB\n\n"
                "Enter your phone number with country code:\n\n"
                "`Example: +1234567890 ❞`\n\n"
                "Your data is encrypted and secure"
            )
            await conv.send_message(text, buttons=[[Button.inline("Back 🔙", b"main_menu")]])
            
            resp = await conv.get_response()
            if resp.text == '/start' or resp.text.lower() == 'back': return
            
            phone = resp.text.strip().replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
            if not phone.startswith('+') and phone.isdigit():
                phone = '+' + phone
            
            total_accs = await accounts_col.count_documents({})
            api_config = get_api_for_account(total_accs, total_accs + 1)
            
            temp_client = TelegramClient(StringSession(), api_config["api_id"], api_config["api_hash"])
            await temp_client.connect()
            
            send_code = await temp_client.send_code_request(phone)
            await conv.send_message(f"✅ OTP sent to `{phone}`.\n\n**Enter OTP:** (If OTP is 12345, type it with spaces like `1 2 3 4 5`).")
            otp = (await conv.get_response()).text.replace(" ", "").strip()
            
            try:
                await temp_client.sign_in(phone, otp, phone_code_hash=send_code.phone_code_hash)
            except SessionPasswordNeededError:
                await conv.send_message("🔐 Two-Step Verification is active. **Enter Password:**")
                pw = (await conv.get_response()).text.strip()
                await temp_client.sign_in(password=pw)
            
            session_string = temp_client.session.save()
            await accounts_col.update_one(
                {"_id": phone},
                {"$set": {
                    "session_string": session_string, 
                    "status": "active", 
                    "added_date": datetime.now(timezone.utc),
                    "api_id": api_config["api_id"],
                    "api_hash": api_config["api_hash"]
                }},
                upsert=True
            )
            
            active_clients[phone] = temp_client
            temp_client.add_event_handler(auto_join_handler, events.NewMessage(incoming=True))
            
            logger.info(f"✅ New account hosted: {phone}")
            await conv.send_message(f"🎉 **Account {phone} successfully hosted!**", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
    
    except Exception as e:
        logger.error(f"❌ Add account failed: {e}")
        if temp_client:
            try:
                await temp_client.disconnect()
            except Exception:
                pass
        try:
            await bot.send_message(sender, f"❌ **Error:** {str(e)}", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
        except Exception:
            pass

# --- 2. MY ACCOUNTS ---
@bot.on(events.CallbackQuery(data=b"my_accounts"))
async def my_accounts_handler(event):
    try:
        accounts = await accounts_col.find({}).to_list(length=None)
        if not accounts:
            text = "╰_╯ **NO ACCOUNTS HOSTED** ❞\n\nAdd an account to start broadcasting!"
            return await event.edit(text, buttons=[[Button.inline("Add Account 📱", b"add_account"), Button.inline("Back 🔙", b"main_menu")]])
        
        text = "╰_╯ **HOSTED ACCOUNTS** ❞\n\n"
        for i, acc in enumerate(accounts, 1):
            phone = acc['_id']
            status = acc.get('status', 'dead')
            icon = "🟢" if status == 'active' else "🔴"
            text += f"`{i}.` 📱 {phone} [{icon}]\n"
        
        text += "\n*(🟢 = Online, 🔴 = Dead/Needs Relogin)*"
        await event.edit(text, buttons=[[Button.inline("Back 🔙", b"main_menu")]])
    except Exception as e:
        logger.error(f"❌ My accounts error: {e}")

# --- 3. SET AD MESSAGE ---
@bot.on(events.CallbackQuery(data=b"set_ad"))
async def set_ad_handler(event):
    sender = event.sender_id
    await event.delete()
    
    try:
        config = await ad_config_col.find_one({"_id": 1}) or {}
        msgs = config.get('msgs', [])
        current_ad = msgs[0]['text'][:30] + "..." if msgs and msgs[0].get('text') else ("Media Ad" if msgs else "No message set yet.")
        
        async with bot.conversation(sender, timeout=600) as conv:
            text = (
                "╰_╯ **SET YOUR AD MESSAGE** ❞\n\n"
                f"**Current Ad Message:** ❞\n`{current_ad}`\n\n"
                "`Send your ad message now (Text, Photo, or Sticker): ❞`"
            )
            await conv.send_message(text, buttons=[[Button.inline("Cancel", b"main_menu")]])
            
            m = await conv.get_response()
            
            media_path = None
            is_sticker = False
            if m.media:
                msg_loader = await conv.send_message("⏳ Downloading media, please wait...")
                media_path = await m.download_media(file="downloads/")
                await msg_loader.delete()
                
                if m.document and getattr(m.document, 'mime_type', '') in ['image/webp', 'application/x-tgsticker', 'video/webm']:
                    is_sticker = True
                elif media_path and (media_path.endswith('.webp') or media_path.endswith('.tgs') or media_path.endswith('.webm')):
                    is_sticker = True
            
            saved_messages = [{"text": m.text if m.text else "", "media": media_path, "is_sticker": is_sticker}]
            
            tgt_text = "🎯 **Choose Target Audience:**"
            tgt_btns = [
                [Button.inline("All Groups", b"tg_groups"), Button.inline("All Channels", b"tg_channels")],
                [Button.inline("Specific Groups", b"tg_spcgroups"), Button.inline("Everything", b"tg_all")]
            ]
            tgt_msg = await conv.send_message(tgt_text, buttons=tgt_btns)
            tgt_resp = await conv.wait_event(events.CallbackQuery())
            choice = tgt_resp.data.decode().split('_')[1]
            await tgt_msg.delete()
            
            target_type = choice
            target_ids = []
            
            if choice == 'spcgroups':
                target_type = 'specific_groups'
                client = list(active_clients.values())[0] if active_clients else None
                if client:
                    dialogs = [d async for d in client.iter_dialogs() if d.is_group]
                    if dialogs:
                        msg_text = "🎯 **Select Groups:**\n\n"
                        inline_buttons = []
                        row = []
                        for i, d in enumerate(dialogs[:50], 1):
                            msg_text += f"`{i}.` {d.title[:30]}\n"
                            row.append(Button.inline(str(i), f"sel_{d.id}"))
                            if len(row) == 5:
                                inline_buttons.append(row)
                                row = []
                        if row: inline_buttons.append(row)
                        inline_buttons.append([Button.inline("✅ DONE", b"sel_done")])
                        
                        sel_msg = await conv.send_message(msg_text, buttons=inline_buttons)
                        selected_specific_ids = set()
                        while True:
                            s_resp = await conv.wait_event(events.CallbackQuery())
                            if s_resp.data == b"sel_done":
                                await s_resp.answer("Saved!")
                                await sel_msg.delete()
                                break
                            elif s_resp.data.startswith(b"sel_"):
                                d_id = int(s_resp.data.decode().split('_')[1])
                                if d_id in selected_specific_ids:
                                    selected_specific_ids.remove(d_id)
                                    await s_resp.answer("Removed!", alert=False)
                                else:
                                    selected_specific_ids.add(d_id)
                                    await s_resp.answer("Added! ✅", alert=False)
                        target_ids = list(selected_specific_ids)
            
            await ad_config_col.update_one(
                {"_id": 1},
                {"$set": {"msgs": saved_messages, "target_type": target_type, "target_ids": target_ids, "last_ad_update": time.time()}}
            )
            
            logger.info("✅ Ad message updated.")
            await conv.send_message("✅ **Ad Message Set!**", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
    except Exception as e:
        logger.error(f"❌ Set ad error: {traceback.format_exc()}")
        await bot.send_message(sender, "❌ Error setting ad.", buttons=[[Button.inline("Back 🔙", b"main_menu")]])

# --- 4. SET TIME INTERVALS ---
@bot.on(events.CallbackQuery(pattern=b"set_time|set_acc_cooldown"))
async def set_times_handler(event):
    sender = event.sender_id
    action = event.data.decode()
    await event.delete()
    
    try:
        config = await ad_config_col.find_one({"_id": 1}) or {}
        if action == "set_time":
            current_val = config.get('interval', 300)
            col = "interval"
            text = f"╰_╯ **SET INTERVAL** ❞\n\n__Current:__ `{current_val}`s\n\n`Send number in seconds:`"
        else:
            current_val = config.get('acc_cooldown', 5)
            col = "acc_cooldown"
            text = f"╰_╯ **SET COOLDOWN** ❞\n\n__Current:__ `{current_val}`s\n\n`Send number in seconds:`"
        
        async with bot.conversation(sender, timeout=120) as conv:
            msg = await conv.send_message(text, buttons=[[Button.inline("Cancel", b"main_menu")]])
            m = await conv.get_response()
            try:
                new_val = int(m.text.strip())
                await ad_config_col.update_one({"_id": 1}, {"$set": {col: new_val}})
                await msg.delete()
                await conv.send_message(f"✅ Updated to **{new_val} seconds**.", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
            except ValueError:
                await conv.send_message("❌ Invalid number.", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
    except Exception as e:
        logger.error(f"❌ Set time error: {e}")

# --- 4B. BROADCAST MODE ---
@bot.on(events.CallbackQuery(data=b"broadcast_mode"))
async def broadcast_mode_handler(event):
    sender = event.sender_id
    await event.delete()
    
    try:
        config = await ad_config_col.find_one({"_id": 1}) or {}
        current_mode = config.get('broadcast_mode', 'parallel')
        
        text = "╰_╯ **SELECT BROADCAST MODE** ❞\n\n⚡ **Parallel Mode:** All accounts broadcast simultaneously (fast, but risk of rate limits)\n\n📱 **Sequential Mode:** One account at a time with delay between them (slower, but recovers faster)\n\nWhich mode do you want?"
        buttons = [
            [Button.inline("⚡ Parallel", b"mode_parallel"), Button.inline("📱 Sequential", b"mode_sequential")],
            [Button.inline("Back 🔙", b"main_menu")]
        ]
        await bot.send_message(sender, text, buttons=buttons)
    except Exception as e:
        logger.error(f"❌ Mode selection error: {e}")

@bot.on(events.CallbackQuery(pattern=b"mode_parallel|mode_sequential"))
async def set_broadcast_mode(event):
    try:
        new_mode = 'parallel' if event.data == b"mode_parallel" else 'sequential'
        await ad_config_col.update_one({"_id": 1}, {"$set": {"broadcast_mode": new_mode, "broadcast_mode_explicit": True}})
        mode_name = "⚡ PARALLEL" if new_mode == 'parallel' else "📱 SEQUENTIAL"
        await safe_callback_answer(event, f"✅ Mode set to {mode_name}!", alert=True)
        await back_to_main(event)
    except Exception as e:
        logger.error(f"❌ Set broadcast mode error: {e}")

# --- 4C. ACCOUNT DELAY (for sequential mode) ---
@bot.on(events.CallbackQuery(data=b"set_account_delay"))
async def set_account_delay_handler(event):
    sender = event.sender_id
    await event.delete()
    
    try:
        config = await ad_config_col.find_one({"_id": 1}) or {}
        current_delay = config.get('account_delay', 120)
        
        text = f"╰_╯ **SET ACCOUNT DELAY** ❞\n\nThis is the wait time between sequential broadcasts.\n\n__Current:__ `{current_delay}`s\n\n`Send delay in seconds (e.g., 120 = 2 min):`"
        
        async with bot.conversation(sender, timeout=120) as conv:
            msg = await conv.send_message(text, buttons=[[Button.inline("Cancel", b"main_menu")]])
            m = await conv.get_response()
            try:
                new_delay = int(m.text.strip())
                if new_delay < 5:
                    raise ValueError("Minimum 5 seconds")
                await ad_config_col.update_one({"_id": 1}, {"$set": {"account_delay": new_delay}})
                await msg.delete()
                await conv.send_message(f"✅ Account delay set to **{new_delay} seconds** ({new_delay/60:.1f} min).", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
            except ValueError as e:
                await conv.send_message(f"❌ Invalid value: {str(e)}", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
    except Exception as e:
        logger.error(f"❌ Set delay error: {e}")

# --- 5. START / STOP ADS ---
@bot.on(events.CallbackQuery(pattern=b"start_ads|stop_ads"))
async def toggle_ads(event):
    try:
        new_status = 'active' if event.data == b"start_ads" else 'paused'
        await ad_config_col.update_one({"_id": 1}, {"$set": {"status": new_status}})
        await safe_callback_answer(event, f"✅ Ads {new_status.title()}!", alert=True)
        await back_to_main(event)
    except Exception as e:
        logger.error(f"❌ Toggle ads error: {e}")

# --- 6. DELETE ACCOUNTS ---
@bot.on(events.CallbackQuery(data=b"del_accounts"))
async def del_accounts_handler(event):
    try:
        accounts = await accounts_col.find({}).to_list(length=None)
        if not accounts:
            return await safe_callback_answer(event, "No accounts!", alert=True)
        
        text = "╰_╯ **DELETE ACCOUNTS** ❞\nClick on an account to safely remove."
        buttons = []
        for acc in accounts:
            phone = acc['_id']
            icon = "🟢" if acc.get('status') == 'active' else "🔴"
            buttons.append([Button.inline(f"🗑 {phone} [{icon}]", f"rmacc_{phone}".encode())])
        buttons.append([Button.inline("Back 🔙", b"main_menu")])
        await event.edit(text, buttons=buttons)
    except Exception as e:
        logger.error(f"❌ Delete menu error: {e}")

@bot.on(events.CallbackQuery(pattern=rb"rmacc_(.*)"))
async def process_delete(event):
    try:
        phone = event.data.decode().replace('rmacc_', '')
        
        if phone in active_clients:
            try:
                await active_clients[phone].log_out()
                await active_clients[phone].disconnect()
            except Exception as e:
                logger.error(f"❌ Error logging out {phone}: {e}")
            finally:
                del active_clients[phone]
                if phone in dialog_cache:
                    del dialog_cache[phone]
                if phone in last_cache_update:
                    del last_cache_update[phone]
        
        await accounts_col.delete_one({"_id": phone})
        await safe_callback_answer(event, "✅ Account Deleted!", alert=True)
        await del_accounts_handler(event)
    except Exception as e:
        logger.error(f"❌ Delete process error: {e}")

# --- 7. ULTIMATE GLOBAL JOIN LINK ---
@bot.on(events.CallbackQuery(data=b"join_all"))
async def join_all_handler(event):
    sender = event.sender_id
    await event.delete()
    
    if not active_clients:
        return await bot.send_message(sender, "❌ No active accounts hosted.", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
    
    async with bot.conversation(sender, timeout=300) as conv:
        await conv.send_message(
            "╰_╯ **GLOBAL JOIN LINK** ❞\n\nAll accounts will join simultaneously.\n`Send links separated by spaces/newlines: ❞`",
            buttons=[[Button.inline("Cancel", b"main_menu")]]
        )
        resp = await conv.get_response()
        if resp.text == '/start' or resp.text.lower() == 'back': return
        
        raw_links = [l.strip() for l in resp.text.split() if l.strip()]
        links = [l for l in raw_links if 't.me/' in l or 'telegram.me/' in l]
        
        if not links:
            return await conv.send_message("❌ No valid links found.", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
        
        state = {"total_links": len(links), "total_accs": len(active_clients), "processed_accs": 0, "success": 0, "already": 0, "failed": 0, "running": True}
        ui_msg = await conv.send_message(f"🚀 **Initializing Join Engine...**")
        
        async def ui_updater():
            while state["running"]:
                try:
                    pct = int((state["processed_accs"] / state["total_accs"]) * 100) if state["total_accs"] > 0 else 0
                    bar = "█" * (pct // 10) + "░" * (10 - (pct // 10))
                    text = (
                        f"╰_╯ **LIVE JOIN PROGRESS** ❞\n\n"
                        f"**Progress:** [{bar}] {pct}%\n"
                        f"• Accounts Processed: `{state['processed_accs']}/{state['total_accs']}`\n"
                        f"• Successful Joins: `✅ {state['success']}`\n"
                        f"• Failed/Limits: `❌ {state['failed']}`\n\n"
                        f"*(Processing at Hyper Speed...)*"
                    )
                    await ui_msg.edit(text)
                except Exception:
                    pass
                await asyncio.sleep(2)
        
        ui_task = create_supervised_task(ui_updater(), name="join-progress-ui")
        
        async def process_account(client, phone):
            for link in links:
                try:
                    if 'addlist/' in link:
                        slug = link.split('addlist/')[-1].split('?')[0].replace('/', '')
                        check = await client(CheckChatlistInviteRequest(slug))
                        if hasattr(check, 'peers'):
                            await client(JoinChatlistInviteRequest(slug=slug, peers=check.peers))
                    elif '+' in link or 'joinchat' in link:
                        h_code = link.split('/')[-1].replace('+', '').split('?')[0]
                        await client(ImportChatInviteRequest(h_code))
                    else:
                        uname = link.split('/')[-1].replace('@', '').split('?')[0]
                        await client(JoinChannelRequest(uname))
                    
                    state["success"] += 1
                    await asyncio.sleep(0.2)
                
                except UserAlreadyParticipantError:
                    state["already"] += 1
                except FloodWaitError as e:
                    remaining = state["total_links"] - (state["success"] + state["already"] + state["failed"])
                    state["failed"] += remaining
                    break
                except Exception as e:
                    state["failed"] += 1
                    logger.debug(f"[{phone}] Join error: {type(e).__name__}")
            
            state["processed_accs"] += 1
        
        async def bounded_process(client, phone):
            async with JOIN_SEMAPHORE:
                await process_account(client, phone)
        
        tasks = [bounded_process(c, p) for p, c in active_clients.items()]
        await asyncio.gather(*tasks)
        
        state["running"] = False
        await ui_task
        await update_stats(joined=state["success"])
        
        final_text = (
            f"╰_╯ **JOIN PROCESS COMPLETED** ❞\n\n"
            f"📊 **Final Report:**\n"
            f"✅ Joined: `{state['success']}` | ⏭️ Skipped: `{state['already']}` | ❌ Failed: `{state['failed']}`\n"
        )
        await ui_msg.edit(final_text, buttons=[[Button.inline("Back 🔙", b"main_menu")]])

# --- 8. MASS REPORT ENGINE ⚠️ ---
@bot.on(events.CallbackQuery(data=b"mass_report"))
async def mass_report_handler(event):
    sender = event.sender_id
    await event.delete()
    
    if not active_clients:
        return await bot.send_message(sender, "❌ No active accounts available to report from.", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
    
    async with bot.conversation(sender, timeout=300) as conv:
        await conv.send_message(
            "╰_╯ **ADVANCED MASS REPORT ENGINE** ❞\n\n"
            "⚠️ **Warning:** Abuse of this feature may lead to account bans.\n\n"
            "`Send the Target (Username, Chat ID, or Message Link):\n"
            "(e.g., @Scammer, -1001234567, or https://t.me/karanerainfo/45735) ❞`",
            buttons=[[Button.inline("Cancel", b"main_menu")]]
        )
        
        resp = await conv.get_response()
        if resp.text == '/start' or resp.text.lower() == 'back': return
        
        target_input = resp.text.strip()
        clean_target = target_input.split('?')[0].replace('https://', '').replace('http://', '').replace('t.me/', '').replace('telegram.me/', '').strip()
        
        logger.info(f"Mass report target: {clean_target}")
        
        msg_id = None
        if '/' in clean_target:
            parts = clean_target.split('/')
            if parts[0] == 'c' and len(parts) >= 3:
                target_entity = int('-100' + parts[1])
                msg_id = int(parts[2])
            else:
                target_entity = parts[0]
                msg_id = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
        else:
            target_entity = clean_target
        
        check_target = str(target_entity).replace('@', '').lower()
        protected_targets = ['dorabita007', '8653737174']
        
        if check_target in protected_targets:
            await conv.send_message("Meri Billi muji se meoww", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
            if sender != ADMIN_ID:
                try:
                    alert_msg = f"🚨 **SECURITY ALERT!** 🚨\n\nUser `{sender}` tried to report your protected ID: `{target_input}`!\nAction blocked. 🛡️"
                    await bot.send_message(ADMIN_ID, alert_msg)
                except Exception:
                    pass
            return
        
        reason_text = "🎯 **Select a Report Reason:**"
        reason_btns = [
            [Button.inline("Spam", b"rep_spam"), Button.inline("Fake Account", b"rep_fake")],
            [Button.inline("Violence", b"rep_violence"), Button.inline("Pornography", b"rep_porn")],
            [Button.inline("Other", b"rep_other"), Button.inline("Cancel", b"main_menu")]
        ]
        reason_msg = await conv.send_message(reason_text, buttons=reason_btns)
        reason_resp = await conv.wait_event(events.CallbackQuery())
        choice = reason_resp.data.decode().split('_')[1]
        await reason_msg.delete()
        
        reasons = {
            'spam': InputReportReasonSpam(),
            'fake': InputReportReasonFake(),
            'violence': InputReportReasonViolence(),
            'porn': InputReportReasonPornography(),
            'other': InputReportReasonOther()
        }
        selected_reason = reasons.get(choice, InputReportReasonSpam())
        
        await conv.send_message(
            "📝 **Enter Custom Report Text:**\n\n"
            "`(Type 'skip' to use default) ❞`",
            buttons=[[Button.inline("Cancel", b"main_menu")]]
        )
        text_resp = await conv.get_response()
        custom_text = text_resp.text.strip()
        if custom_text.lower() == 'skip':
            custom_text = "Reported via Mass System"
        
        await conv.send_message(
            "🔢 **How many times should EACH account send this report?**\n\n"
            "`(Enter a number between 1 and 100) ❞`",
            buttons=[[Button.inline("Cancel", b"main_menu")]]
        )
        count_resp = await conv.get_response()
        try:
            report_count = int(count_resp.text.strip())
            if report_count > 100: report_count = 100
            if report_count < 1: report_count = 1
        except ValueError:
            report_count = 1
        
        total_planned_reports = len(active_clients) * report_count
        state = {"total": total_planned_reports, "success": 0, "failed": 0, "running": True}
        
        type_str = "Message" if msg_id else "User/Chat"
        logger.info(f"🚀 Mass report started - Type: {type_str}, Target: {target_entity}, Reports per account: {report_count}")
        ui_msg = await conv.send_message(f"🚀 **Targeting {type_str} with {total_planned_reports} reports...**")
        
        async def ui_updater():
            while state["running"]:
                try:
                    pct = int(((state["success"] + state["failed"]) / state["total"]) * 100) if state["total"] > 0 else 0
                    bar = "█" * (pct // 10) + "░" * (10 - (pct // 10))
                    text = (
                        f"╰_╯ **LIVE REPORT PROGRESS** ❞\n\n"
                        f"**Target:** `{target_input}`\n"
                        f"**Multiplier:** `{report_count}x per account`\n"
                        f"**Progress:** [{bar}] {pct}%\n"
                        f"• Reports Sent: `✅ {state['success']}`\n"
                        f"• Failed/Limits: `❌ {state['failed']}`\n"
                    )
                    await ui_msg.edit(text)
                except Exception:
                    pass
                await asyncio.sleep(2)
        
        ui_task = create_supervised_task(ui_updater(), name="report-progress-ui")
        
        async def process_report(client, phone):
            entity = None
            try:
                if str(target_entity).lstrip('-').isdigit():
                    target_num = int(target_entity)
                    entity = await client.get_input_entity(target_num)
                else:
                    target_str = target_entity if target_entity.startswith('@') else f"@{target_entity}"
                    entity = await client.get_input_entity(target_str)
            
            except ValueError as e:
                logger.error(f"[{phone}] Entity resolution failed: {str(e)}")
                state["failed"] += report_count
                return
            except Exception as e:
                logger.error(f"[{phone}] Entity error: {type(e).__name__}: {str(e)}")
                state["failed"] += report_count
                return
            
            if entity is None:
                logger.error(f"[{phone}] Entity is None")
                state["failed"] += report_count
                return
            
            for i in range(report_count):
                try:
                    if msg_id:
                        await client(ReportRequest(
                            peer=entity,
                            id=[msg_id],
                            option=b'',
                            message=custom_text
                        ))
                    else:
                        await client(ReportPeerRequest(
                            peer=entity,
                            reason=selected_reason,
                            message=custom_text
                        ))
                    
                    state["success"] += 1
                    await asyncio.sleep(random.uniform(0.8, 2.5))
                
                except FloodWaitError as e:
                    logger.warning(f"[{phone}] FloodWait: {e.seconds}s")
                    remaining = report_count - (i + 1)
                    state["failed"] += remaining
                    await log_error(phone, "FLOOD_WAIT_REPORT", f"Wait: {e.seconds}s")
                    break
                
                except Exception as e:
                    logger.error(f"[{phone}] Report error: {type(e).__name__}: {str(e)}")
                    state["failed"] += 1
                    await log_error(phone, "REPORT_ERROR", str(e))
                    await asyncio.sleep(1.0)
        
        async def bounded_report(phone, client):
            async with REPORT_SEMAPHORE:
                await process_report(client, phone)
        
        tasks = [bounded_report(p, c) for p, c in active_clients.items()]
        await asyncio.gather(*tasks)
        
        state["running"] = False
        await ui_task
        
        final_text = (
            f"╰_╯ **MASS REPORT COMPLETED** ❞\n\n"
            f"🎯 **Target:** `{target_input}`\n"
            f"💬 **Custom Text:** `{custom_text}`\n"
            f"📊 **Final Report:**\n"
            f"✅ Sent Successfully: `{state['success']}`\n"
            f"❌ Failed (Limits/Errors): `{state['failed']}`\n"
        )
        await ui_msg.edit(final_text, buttons=[[Button.inline("Back 🔙", b"main_menu")]])

# --- 9. DOWNLOAD DATABASE BACKUP 💾 ---
@bot.on(events.CallbackQuery(data=b"download_db"))
async def download_db_handler(event):
    try:
        await safe_callback_answer(event, "Generating Database Backup...", alert=False)
        
        backup_data = {
            "accounts": await accounts_col.find({}).to_list(length=None),
            "ad_config": await ad_config_col.find({}).to_list(length=None),
            "bot_stats": await bot_stats_col.find({}).to_list(length=None),
            "auth_users": await auth_users_col.find({}).to_list(length=None),
            "backup_time": datetime.now(timezone.utc).isoformat()
        }
        
        file_path = "downloads/database_backup.json"
        with open(file_path, "w") as f:
            json.dump(backup_data, f, indent=4)
        
        await bot.send_file(
            event.chat_id,
            file_path,
            caption="╰_╯ **DATABASE BACKUP** ❞\n\nFull JSON export of MongoDB Database.",
            reply_to=event.message_id
        )
        os.remove(file_path)
    except Exception as e:
        logger.error(f"❌ Download DB Error: {e}")
        await safe_callback_answer(event, "Error generating backup.", alert=True)

# --- 10. LOGS & AUTH ---
@bot.on(events.CallbackQuery(data=b"view_logs"))
async def view_logs_handler(event):
    try:
        if not os.path.exists("logs/bot_logs.txt"):
            return await event.edit("📋 **No logs found.**", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
        
        with open("logs/bot_logs.txt", "r") as f:
            lines = f.readlines()
            last_lines = "".join(lines[-30:])
        
        text = f"╰_╯ **SYSTEM LOGS (Last 30 Events) 📋** ❞\n\n`{last_lines}`"
        if len(text) > 4000: text = text[:4000] + "..."
        await event.edit(text, buttons=[[Button.inline("Back 🔙", b"main_menu")]])
    except Exception as e:
        logger.error(f"❌ Logs read error: {e}")
        await safe_callback_answer(event, "Error reading logs.", alert=True)

@bot.on(events.CallbackQuery(data=b"manage_auth"))
async def manage_auth(event):
    try:
        users = await auth_users_col.find({}).to_list(length=None)
        text = "╰_╯ **MANAGE ACCESS** ❞\n\n**Authorized Users:**\n"
        for u in users:
            admin_tag = "(Primary Admin)" if u['_id'] == ADMIN_ID else ""
            text += f"• `{u['_id']}` {admin_tag}\n"
        
        buttons = [
            [Button.inline("Add User ➕", b"add_auth"), Button.inline("Remove User ➖", b"rem_auth")],
            [Button.inline("Back 🔙", b"main_menu")]
        ]
        await event.edit(text, buttons=buttons)
    except Exception as e:
        logger.error(f"❌ Manage auth error: {e}")

@bot.on(events.CallbackQuery(pattern=b"add_auth|rem_auth"))
async def auth_actions(event):
    sender = event.sender_id
    action = event.data.decode()
    await event.delete()
    
    try:
        async with bot.conversation(sender, timeout=60) as conv:
            act_text = "ADD" if action == "add_auth" else "REMOVE"
            await conv.send_message(f"╰_╯ **{act_text} AUTHORIZED USER** ❞\n\n`Send the Telegram User ID: ❞`", buttons=[[Button.inline("Cancel", b"main_menu")]])
            m = await conv.get_response()
            
            try:
                uid = int(m.text.strip())
                if action == "add_auth":
                    await auth_users_col.update_one({"_id": uid}, {"$set": {"_id": uid}}, upsert=True)
                    logger.info(f"✅ Authorized new user: {uid}")
                    await conv.send_message(f"✅ User `{uid}` authorized.", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
                else:
                    if uid == ADMIN_ID:
                        return await conv.send_message("❌ Cannot remove Primary Admin.", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
                    await auth_users_col.delete_one({"_id": uid})
                    logger.info(f"✅ Removed user: {uid}")
                    await conv.send_message(f"✅ User `{uid}` removed.", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
            except ValueError:
                await conv.send_message("❌ Invalid User ID.", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
    except Exception as e:
        logger.error(f"❌ Auth action error: {e}")

# ==========================================
# 🌟 ALWAYS-ACTIVE BOOT & KEEP-ALIVE ENGINE
# ==========================================

async def main():
    """Main Orchestrator: Sabhi background loops aur main bot ko ek sath run karta hai"""
    logger.info("🚀 Boot Sequence Initiated...")
    
    # 1. Main Telethon Bot Start
    await asyncio.wait_for(bot.start(bot_token=BOT_TOKEN), timeout=30)
    
    # 2. Database & Accounts Setup
    await connect_to_mongodb()
    await setup_db()
    await load_and_verify_clients()

    # 3. Port Binding & Self-Ping Tasks (Host Crash & Sleep Se Bachane Ke Liye)
    create_supervised_task(start_dummy_web_server(), name="dummy-web-server")
    create_supervised_task(keep_alive_ping_loop(), name="keep-alive-ping")

    # 4. Background Spammer & Account Monitor Engine
    create_supervised_task(spammer_engine(), name="broadcast-scheduler")
    create_supervised_task(health_check_clients(), name="client-health-check")
    
    logger.info("🟢 Bot fully active hai aur background engines chal rahe hain!")
    
    # 5. Continuous Event Loop (Bot ko hamesha zinda rakhta hai)
    await bot.run_until_disconnected()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot manually stop kar diya gaya.")
