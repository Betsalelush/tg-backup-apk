import asyncio
import hashlib
import json
import os
import queue
import random
import re
import threading
from datetime import datetime

from flask import Flask, Response, jsonify, render_template, request

DATA_DIR    = os.environ.get("TG_DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
CONFIG_FILE = os.path.join(DATA_DIR, "config.json")
TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")

APP_VERSION  = "1.1"
BOT_TOKEN    = "8867679619:AAFf7O96HEbKako4rE-xg_kAHe-OICOQVFw"
REPORT_CHAT  = "@backuppppy"   # הבוט חייב להיות חבר בקבוצה
TG_GROUP_URL = "https://t.me/backuppppy"
GITHUB_URL   = "https://github.com/Betsalelush/tg-backup-apk"

LOG_Q  = queue.Queue(maxsize=500)
STATUS = {"running": False, "paused": False, "engine": None, "thread": None, "transferred": 0}
AUTH_STATE = {}


def _bot_send(text):
    """שולח הודעה לבוט — לעולם לא זורק שגיאה"""
    try:
        import urllib.request, urllib.parse
        data = urllib.parse.urlencode({
            "chat_id": REPORT_CHAT,
            "text": text,
            "disable_notification": "true"
        }).encode()
        urllib.request.urlopen(
            urllib.request.Request(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", data=data
            ), timeout=5
        )
    except Exception:
        pass

def _try_register(phone):
    """רושם משתמש חדש פעם אחת — hash של מספר הטלפון, לא נשמר מידע אישי"""
    try:
        cfg = _load_cfg()
        if cfg.get("registered"):
            return
        h = hashlib.sha256(phone.encode()).hexdigest()[:16]
        _bot_send(f"👤 New user: {h}")
        cfg["registered"] = True
        _save_cfg(cfg)
    except Exception:
        pass

DEFAULT_CFG = {
    "accounts": [],
    "source_chat_id": "",
    "target_chat_id": "",
    "media_type": "video_doc",  # video_doc | photo | all_media | text | all
    "start_from_id": 0,
}

# loop קבוע אחד לכל פעולות Telethon
_TG_LOOP = asyncio.new_event_loop()
threading.Thread(target=_TG_LOOP.run_forever, daemon=True).start()


def _load_cfg():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, encoding="utf-8") as f:
                d = json.load(f)
            out = DEFAULT_CFG.copy(); out.update(d); return out
        except Exception: pass
    return DEFAULT_CFG.copy()

def _save_cfg(cfg):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)

def _last_id_path(source_id):
    return os.path.join(DATA_DIR, f"last_id_{source_id}.txt")

def _save_last_id(mid, source_id):
    with open(_last_id_path(source_id), "w") as f: f.write(str(mid))

def _load_last_id(source_id):
    try:
        with open(_last_id_path(source_id)) as f: return int(f.read().strip())
    except Exception: return 0

def _log(msg, level="info"):
    entry = {"time": datetime.now().strftime("%H:%M:%S"), "level": level, "msg": msg}
    try: LOG_Q.put_nowait(entry)
    except queue.Full:
        try: LOG_Q.get_nowait()
        except queue.Empty: pass
        LOG_Q.put_nowait(entry)

def _run_async(coro):
    future = asyncio.run_coroutine_threadsafe(coro, _TG_LOOP)
    return future.result(timeout=60)

def _msg_matches(msg, media_type):
    if media_type == "video_doc":
        return (msg.video or msg.document) and not (msg.sticker or msg.photo)
    if media_type == "photo":
        return bool(msg.photo)
    if media_type == "all_media":
        return bool(msg.media) and not msg.sticker
    if media_type == "text":
        return bool(msg.text) and not msg.media
    if media_type == "all":
        return True
    return False


# ── Auth ──────────────────────────────────────────────────────────────────────

async def _auth_send_code(acc):
    from telethon import TelegramClient
    name  = acc["client_name"]
    phone = acc["phone_number"]
    session_path = os.path.join(DATA_DIR, name)
    client = TelegramClient(session_path, int(acc["api_id"]), acc["api_hash"])
    await client.connect()
    if await client.is_user_authorized():
        AUTH_STATE[name] = {"client": client, "status": "connected"}
        return {"ok": True, "already": True}
    result = await client.send_code_request(phone)
    AUTH_STATE[name] = {"client": client, "hash": result.phone_code_hash, "status": "pending_code"}
    return {"ok": True, "already": False}

async def _auth_verify_code(acc, code):
    from telethon import errors
    name  = acc["client_name"]
    phone = acc["phone_number"]
    state = AUTH_STATE.get(name)
    if not state: return {"ok": False, "msg": "No pending auth"}
    try:
        await state["client"].sign_in(phone, code, phone_code_hash=state["hash"])
        AUTH_STATE[name]["status"] = "connected"
        return {"ok": True}
    except errors.SessionPasswordNeededError:
        AUTH_STATE[name]["status"] = "pending_pw"
        return {"ok": False, "need_password": True}
    except Exception as e:
        AUTH_STATE[name]["status"] = "error"
        return {"ok": False, "msg": str(e)}

async def _auth_password(acc, pw):
    name  = acc["client_name"]
    state = AUTH_STATE.get(name)
    if not state: return {"ok": False, "msg": "No pending auth"}
    try:
        await state["client"].sign_in(password=pw)
        AUTH_STATE[name]["status"] = "connected"
        return {"ok": True}
    except Exception as e:
        AUTH_STATE[name]["status"] = "error"
        return {"ok": False, "msg": str(e)}

async def _auth_disconnect(acc):
    name = acc["client_name"]
    state = AUTH_STATE.get(name)
    try:
        if state and state.get("client"):
            await state["client"].log_out()
    except Exception:
        pass
    AUTH_STATE.pop(name, None)
    session_path = os.path.join(DATA_DIR, name + ".session")
    try:
        os.remove(session_path)
    except Exception:
        pass
    return {"ok": True}


# ── Backup engine ─────────────────────────────────────────────────────────────

class BackupEngine:
    def __init__(self, config):
        self.config = config
        self.running = False
        self._paused  = False   # simple flag — safe to read/write across threads (CPython GIL)

    def pause(self):
        self._paused = True
        _log("Paused.", "warn")

    def resume(self):
        self._paused = False
        _log("Resumed.", "success")

    def stop(self):
        self.running = False
        self._paused = False

    async def _wait_if_paused(self):
        while self._paused and self.running:
            await asyncio.sleep(0.3)

    async def run(self):
        from telethon import TelegramClient, errors
        self.running = True
        source     = int(self.config["source_chat_id"])
        target     = int(self.config["target_chat_id"])
        media_type = self.config.get("media_type", "video_doc")
        start_from = int(self.config.get("start_from_id") or 0)
        if start_from:
            _save_last_id(start_from, source)
            _log(f"Starting from message ID {start_from}", "info")
        STATUS["transferred"] = 0

        clients = []
        for acc in self.config.get("accounts", []):
            name = acc.get("client_name", "")
            if not acc.get("api_id") or not acc.get("api_hash"): continue
            state = AUTH_STATE.get(name)
            if state and state["status"] == "connected" and state.get("client"):
                clients.append(state["client"])
                _log(f"{name} reusing session", "success")
            else:
                session_path = os.path.join(DATA_DIR, name)
                try:
                    c = TelegramClient(session_path, int(acc["api_id"]), acc["api_hash"])
                    await c.connect()
                    if await c.is_user_authorized():
                        clients.append(c)
                        AUTH_STATE[name] = {"client": c, "status": "connected"}
                        _log(f"{name} connected", "success")
                    else:
                        _log(f"{name} not authorized — use Connect button", "error")
                except Exception as exc:
                    _log(f"Failed {name}: {exc}", "error")

        if not clients:
            _log("No authorized accounts — aborting.", "error")
            self.running = False; STATUS["running"] = False; return

        _log(f"Active [{media_type}] — {source} → {target}", "info")
        idx = 0

        while self.running:
            await self._wait_if_paused()
            if not self.running: break
            client = clients[idx]
            last_id = _load_last_id(source)
            try:
                batch = 0; last_proc = last_id
                max_batch = random.randint(2, 6)
                async for msg in client.iter_messages(source, offset_id=last_id, reverse=True, limit=50):
                    if not self.running: break
                    await self._wait_if_paused()
                    if not self.running: break
                    if msg.id > last_proc: last_proc = msg.id

                    if _msg_matches(msg, media_type):
                        try:
                            caption = re.sub(r"https?://\S+|www\.\S+|t\.me/\S+|@\S+", "", msg.text or "").strip()
                            if msg.media:
                                await client.send_message(target, caption, file=msg.media)
                            else:
                                await client.send_message(target, caption)
                            STATUS["transferred"] += 1
                            _log(f"[Acc {idx+1}] Copied msg {msg.id} — סה\"כ: {STATUS['transferred']}", "success")
                            batch += 1
                            await asyncio.sleep(random.uniform(1.4, 3.8))
                            if batch >= max_batch:
                                _save_last_id(last_proc, source); break
                        except errors.FloodWaitError as exc:
                            _log(f"FloodWait {exc.seconds}s — switching account", "warn")
                            _save_last_id(last_proc, source); await asyncio.sleep(2.8); break
                        except Exception as exc:
                            _log(f"Error msg {msg.id}: {exc}", "error")
                    else:
                        _log(f"[Acc {idx+1}] Skip {msg.id}", "warn")

                if last_proc > last_id:
                    _save_last_id(last_proc, source)
                idx = (idx + 1) % len(clients)
                _log(f"Switching to account {idx+1}", "info")
                await asyncio.sleep(random.uniform(2, 6))
            except Exception as exc:
                _log(f"Error: {exc}", "error")
                threading.Thread(target=_bot_send, args=(f"⚠️ TG Backup error:\n{exc}",), daemon=True).start()
                await asyncio.sleep(30)

        for c in clients:
            try: await c.disconnect()
            except Exception: pass
        _log("Stopped.", "info")
        STATUS["running"] = False


def _start_worker(config):
    engine = BackupEngine(config)
    STATUS["engine"] = engine
    def _run():
        future = asyncio.run_coroutine_threadsafe(engine.run(), _TG_LOOP)
        try: future.result()
        except Exception as e: _log(f"Worker error: {e}", "error")
        finally: STATUS["running"] = False
    t = threading.Thread(target=_run, daemon=True)
    STATUS["thread"] = t; t.start()


# ── Flask ─────────────────────────────────────────────────────────────────────

def create_app():
    app = Flask(__name__, template_folder=TEMPLATE_DIR)

    @app.route("/")
    def index(): return render_template("index.html", config=_load_cfg(),
                                        version=APP_VERSION,
                                        tg_group=TG_GROUP_URL,
                                        github=GITHUB_URL)

    @app.route("/config", methods=["POST"])
    def update_config(): _save_cfg(request.json); return jsonify({"ok": True})

    @app.route("/auth/send_code", methods=["POST"])
    def auth_send_code():
        data = request.json; cfg = _load_cfg(); idx = data.get("acc_idx", 0)
        if idx >= len(cfg["accounts"]): return jsonify({"ok": False, "msg": "Account not found"})
        acc = cfg["accounts"][idx]
        if not acc.get("api_id") or not acc.get("phone_number"):
            return jsonify({"ok": False, "msg": "Fill API ID and Phone first"})
        try:
            result = _run_async(_auth_send_code(acc))
            if result.get("ok") and result.get("already"):
                threading.Thread(target=_try_register, args=(acc["phone_number"],), daemon=True).start()
            return jsonify(result)
        except Exception as e: return jsonify({"ok": False, "msg": str(e)})

    @app.route("/auth/verify", methods=["POST"])
    def auth_verify():
        data = request.json; cfg = _load_cfg(); idx = data.get("acc_idx", 0)
        if idx >= len(cfg["accounts"]): return jsonify({"ok": False, "msg": "Account not found"})
        try:
            result = _run_async(_auth_verify_code(cfg["accounts"][idx], data.get("code", "")))
            if result.get("ok"):
                phone = cfg["accounts"][idx].get("phone_number", "")
                threading.Thread(target=_try_register, args=(phone,), daemon=True).start()
            return jsonify(result)
        except Exception as e: return jsonify({"ok": False, "msg": str(e)})

    @app.route("/auth/password", methods=["POST"])
    def auth_password():
        data = request.json; cfg = _load_cfg(); idx = data.get("acc_idx", 0)
        if idx >= len(cfg["accounts"]): return jsonify({"ok": False, "msg": "Account not found"})
        try:
            result = _run_async(_auth_password(cfg["accounts"][idx], data.get("password", "")))
            if result.get("ok"):
                phone = cfg["accounts"][idx].get("phone_number", "")
                threading.Thread(target=_try_register, args=(phone,), daemon=True).start()
            return jsonify(result)
        except Exception as e: return jsonify({"ok": False, "msg": str(e)})

    @app.route("/auth/disconnect", methods=["POST"])
    def auth_disconnect():
        data = request.json; cfg = _load_cfg(); idx = data.get("acc_idx", 0)
        if idx >= len(cfg["accounts"]): return jsonify({"ok": False, "msg": "Account not found"})
        try: return jsonify(_run_async(_auth_disconnect(cfg["accounts"][idx])))
        except Exception as e: return jsonify({"ok": False, "msg": str(e)})

    @app.route("/auth/status")
    def auth_status():
        return jsonify({n: s.get("status", "unknown") for n, s in AUTH_STATE.items()})

    @app.route("/start", methods=["POST"])
    def start():
        if STATUS["running"]: return jsonify({"ok": False, "msg": "Already running"})
        cfg = _load_cfg()
        if not cfg["source_chat_id"] or not cfg["target_chat_id"]:
            return jsonify({"ok": False, "msg": "Missing chat IDs"})
        STATUS["running"] = True; STATUS["paused"] = False
        _log("Starting…", "info"); _start_worker(cfg)
        return jsonify({"ok": True})

    @app.route("/stop", methods=["POST"])
    def stop():
        if STATUS.get("engine"): STATUS["engine"].stop()
        STATUS["running"] = False; _log("Stopping…", "warn"); return jsonify({"ok": True})

    @app.route("/pause", methods=["POST"])
    def pause():
        if STATUS.get("engine"): STATUS["engine"].pause(); STATUS["paused"] = True
        return jsonify({"ok": True})

    @app.route("/resume", methods=["POST"])
    def resume():
        if STATUS.get("engine"): STATUS["engine"].resume(); STATUS["paused"] = False
        return jsonify({"ok": True})

    @app.route("/status")
    def status(): return jsonify({"running": STATUS["running"], "paused": STATUS["paused"], "transferred": STATUS["transferred"]})

    @app.route("/last_id")
    def last_id():
        src = _load_cfg().get("source_chat_id", "")
        return jsonify({"last_id": _load_last_id(src) if src else 0})

    @app.route("/reset_last_id", methods=["POST"])
    def reset_last_id():
        src = _load_cfg().get("source_chat_id", "")
        if src: _save_last_id(0, src)
        return jsonify({"ok": True})

    @app.route("/logs")
    def stream_logs():
        def generate():
            while True:
                try:
                    entry = LOG_Q.get(timeout=20)
                    yield f"data: {json.dumps(entry, ensure_ascii=False)}\n\n"
                except queue.Empty:
                    yield 'data: {"ping":true}\n\n'
        return Response(generate(), mimetype="text/event-stream")

    return app


if __name__ == "__main__":
    create_app().run(host="0.0.0.0", port=5050, debug=True)
