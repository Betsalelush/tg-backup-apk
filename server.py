import asyncio
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
LAST_ID_FILE = os.path.join(DATA_DIR, "last_id.txt")
TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")

LOG_Q  = queue.Queue(maxsize=500)
STATUS = {"running": False, "paused": False, "engine": None, "thread": None}
AUTH_STATE = {}  # client_name -> {"client", "hash", "status": pending_code|pending_pw|connected|error}

DEFAULT_CFG = {"accounts": [], "source_chat_id": "", "target_chat_id": ""}


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

def _save_last_id(mid):
    with open(LAST_ID_FILE, "w") as f: f.write(str(mid))

def _load_last_id():
    try:
        with open(LAST_ID_FILE) as f: return int(f.read().strip())
    except Exception: return 0

def _log(msg, level="info"):
    entry = {"time": datetime.now().strftime("%H:%M:%S"), "level": level, "msg": msg}
    try: LOG_Q.put_nowait(entry)
    except queue.Full:
        try: LOG_Q.get_nowait()
        except queue.Empty: pass
        LOG_Q.put_nowait(entry)

def _run_async(coro):
    loop = asyncio.new_event_loop()
    try: return loop.run_until_complete(coro)
    finally: loop.close()


# ── Auth helpers ──────────────────────────────────────────────────────────────

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
    client = state["client"]
    try:
        await client.sign_in(phone, code, phone_code_hash=state["hash"])
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


# ── Backup engine ─────────────────────────────────────────────────────────────

class BackupEngine:
    def __init__(self, config):
        self.config = config
        self._pause_event = asyncio.Event()
        self._pause_event.set()
        self.running = False

    def pause(self):  self._pause_event.clear(); _log("Paused.", "warn")
    def resume(self): self._pause_event.set();   _log("Resumed.", "success")
    def stop(self):   self.running = False;       self._pause_event.set()

    async def run(self):
        from telethon import TelegramClient, errors
        self.running = True
        source = int(self.config["source_chat_id"])
        target = int(self.config["target_chat_id"])

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

        _log(f"Active — {source} → {target}", "info")
        idx = 0

        while self.running:
            await self._pause_event.wait()
            if not self.running: break
            client = clients[idx]
            last_id = _load_last_id()
            try:
                batch = 0; last_proc = last_id
                async for msg in client.iter_messages(source, offset_id=last_id, reverse=True, limit=50):
                    if not self.running: break
                    await self._pause_event.wait()
                    if msg.id > last_proc: last_proc = msg.id
                    has_media = (msg.video or msg.document) and not (msg.sticker or msg.photo)
                    if has_media:
                        try:
                            caption = re.sub(r"https?://\S+|www\.\S+|t\.me/\S+|@\S+", "", msg.text or "").strip()
                            await client.send_message(target, caption, file=msg.media)
                            _log(f"[Acc {idx+1}] Copied msg {msg.id}", "success")
                            batch += 1
                            await asyncio.sleep(random.uniform(1.4, 3.8))
                            if batch >= random.randint(2, 6):
                                _save_last_id(last_proc); break
                        except errors.FloodWaitError as exc:
                            wait = min(exc.seconds, 60)
                            _log(f"FloodWait {exc.seconds}s (waiting {wait}s)", "warn")
                            _save_last_id(last_proc); await asyncio.sleep(wait); break
                        except Exception as exc:
                            _log(f"Error msg {msg.id}: {exc}", "error")
                    else:
                        _log(f"[Acc {idx+1}] Skip {msg.id} (no media)", "warn")
                if last_proc > last_id:
                    _save_last_id(last_proc)
                idx = (idx + 1) % len(clients)
                await asyncio.sleep(random.uniform(2, 6))
            except Exception as exc:
                _log(f"Error: {exc}", "error"); await asyncio.sleep(30)

        for c in clients:
            try: await c.disconnect()
            except Exception: pass
        _log("Disconnected.", "info")
        STATUS["running"] = False


def _start_worker(config):
    engine = BackupEngine(config)
    STATUS["engine"] = engine
    def _run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try: loop.run_until_complete(engine.run())
        finally: loop.close(); STATUS["running"] = False
    t = threading.Thread(target=_run, daemon=True)
    STATUS["thread"] = t; t.start()


# ── Flask app ─────────────────────────────────────────────────────────────────

def create_app():
    app = Flask(__name__, template_folder=TEMPLATE_DIR)

    @app.route("/")
    def index():
        return render_template("index.html", config=_load_cfg())

    @app.route("/config", methods=["POST"])
    def update_config():
        _save_cfg(request.json); return jsonify({"ok": True})

    @app.route("/auth/send_code", methods=["POST"])
    def auth_send_code():
        data = request.json
        cfg  = _load_cfg()
        idx  = data.get("acc_idx", 0)
        if idx >= len(cfg["accounts"]):
            return jsonify({"ok": False, "msg": "Account not found"})
        acc = cfg["accounts"][idx]
        if not acc.get("api_id") or not acc.get("phone_number"):
            return jsonify({"ok": False, "msg": "Fill API ID and Phone first"})
        try:
            result = _run_async(_auth_send_code(acc))
            return jsonify(result)
        except Exception as e:
            return jsonify({"ok": False, "msg": str(e)})

    @app.route("/auth/verify", methods=["POST"])
    def auth_verify():
        data = request.json
        cfg  = _load_cfg()
        idx  = data.get("acc_idx", 0)
        if idx >= len(cfg["accounts"]):
            return jsonify({"ok": False, "msg": "Account not found"})
        try:
            result = _run_async(_auth_verify_code(cfg["accounts"][idx], data.get("code", "")))
            return jsonify(result)
        except Exception as e:
            return jsonify({"ok": False, "msg": str(e)})

    @app.route("/auth/password", methods=["POST"])
    def auth_password():
        data = request.json
        cfg  = _load_cfg()
        idx  = data.get("acc_idx", 0)
        if idx >= len(cfg["accounts"]):
            return jsonify({"ok": False, "msg": "Account not found"})
        try:
            result = _run_async(_auth_password(cfg["accounts"][idx], data.get("password", "")))
            return jsonify(result)
        except Exception as e:
            return jsonify({"ok": False, "msg": str(e)})

    @app.route("/auth/status")
    def auth_status():
        out = {}
        for name, state in AUTH_STATE.items():
            out[name] = state.get("status", "unknown")
        return jsonify(out)

    @app.route("/start", methods=["POST"])
    def start():
        if STATUS["running"]: return jsonify({"ok": False, "msg": "Already running"})
        cfg = _load_cfg()
        if not cfg["source_chat_id"] or not cfg["target_chat_id"]:
            return jsonify({"ok": False, "msg": "Missing chat IDs"})
        STATUS["running"] = True; STATUS["paused"] = False
        _log("Starting…", "info"); _start_worker(cfg)
        return jsonify({"ok": True})

    @app.route("/stop",   methods=["POST"])
    def stop():
        if STATUS.get("engine"): STATUS["engine"].stop()
        STATUS["running"] = False; _log("Stopping…", "warn"); return jsonify({"ok": True})

    @app.route("/pause",  methods=["POST"])
    def pause():
        if STATUS.get("engine"): STATUS["engine"].pause(); STATUS["paused"] = True
        return jsonify({"ok": True})

    @app.route("/resume", methods=["POST"])
    def resume():
        if STATUS.get("engine"): STATUS["engine"].resume(); STATUS["paused"] = False
        return jsonify({"ok": True})

    @app.route("/status")
    def status(): return jsonify({"running": STATUS["running"], "paused": STATUS["paused"]})

    @app.route("/last_id")
    def last_id(): return jsonify({"last_id": _load_last_id()})

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
