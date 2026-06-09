import asyncio
import glob
import json
import os
import queue
import random
import re
import shutil
import threading
from datetime import datetime

from flask import Flask, Response, jsonify, render_template, request

import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration

SENTRY_DSN = "https://1f490b846ede82cfc3d5f6f5eb23263b@o4510215210598400.ingest.de.sentry.io/4510674676744272"
sentry_sdk.init(dsn=SENTRY_DSN, integrations=[FlaskIntegration()], traces_sample_rate=0.0)

DATA_DIR    = os.environ.get("TG_DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
CONFIG_FILE = os.path.join(DATA_DIR, "config.json")
TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")

APP_VERSION  = "1.4"
TG_GROUP_URL = "https://t.me/backuppppy"
GITHUB_URL   = "https://github.com/backuppppy/tg-backup-apk"

LOG_Q  = queue.Queue(maxsize=500)

# JOB_STATES: {job_id: {"running": bool, "paused": bool, "engine": BackupEngine}}
JOB_STATES: dict = {}

# Global summary status (kept for header dot / compat)
STATUS = {"running": False, "paused": False, "transferred": 0}
AUTH_STATE = {}


DEFAULT_CFG = {
    "accounts": [],
    "jobs": [],
    # כל job: {id, name, source_chat_id, target_chat_id, media_type,
    #          start_from_id, account_indices: [...], mode: "fast"|"ordered"}
}

# loop קבוע אחד לכל פעולות Telethon
_TG_LOOP = asyncio.new_event_loop()
threading.Thread(target=_TG_LOOP.run_forever, daemon=True).start()


def _update_global_status():
    running_states = [s for s in JOB_STATES.values() if s.get("running")]
    any_running  = bool(running_states)
    all_paused   = bool(running_states) and all(s.get("paused") for s in running_states)
    STATUS["running"] = any_running
    STATUS["paused"]  = all_paused


def _migrate_to_jobs(cfg):
    """מיגרציה חד-פעמית מתצורה ישנה (מקור/יעד יחיד, מ-v1.1 ומטה) לרשימת jobs.
    קריטי: מעתיקה גם את קבצי ההתקדמות הקיימים לשמות החדשים (job-keyed) —
    אחרת המשימה הממוגרת הייתה 'שוכחת' איפה הפסיקה ומתחילה לסרוק את הערוץ
    מהודעה 0, מה שנראה כאילו 'האפליקציה לא מעבירה הודעות' (היא פשוט
    מדלגת מחדש על אלפי הודעות ישנות). שומרת את התוצאה לדיסק כדי שזה ירוץ פעם אחת בלבד."""
    job_key = "migrated"
    try:
        source = int(cfg.get("source_chat_id") or 0)
    except (TypeError, ValueError):
        source = 0
    n = len(cfg.get("accounts", []))

    if source:
        old_simple = os.path.join(DATA_DIR, f"last_id_{source}.txt")
        new_simple = _last_id_path(job_key, source)
        if os.path.exists(old_simple) and not os.path.exists(new_simple):
            try: shutil.copy(old_simple, new_simple)
            except Exception: pass
        for i in range(n):
            old_part = os.path.join(DATA_DIR, f"last_id_{source}_part{n}_{i}.txt")
            new_part = _worker_id_path(job_key, source, i, n)
            if os.path.exists(old_part) and not os.path.exists(new_part):
                try: shutil.copy(old_part, new_part)
                except Exception: pass

    job = {
        "id": job_key,
        "name": "משימה 1",
        "source_chat_id": cfg.get("source_chat_id", ""),
        "target_chat_id": cfg.get("target_chat_id", ""),
        "media_type": cfg.get("media_type", "video_doc"),
        "start_from_id": cfg.get("start_from_id", 0),
        "account_indices": list(range(n)),
        "mode": "fast",
    }
    new_cfg = dict(cfg)
    for k in ("source_chat_id", "target_chat_id", "media_type", "start_from_id"):
        new_cfg.pop(k, None)
    new_cfg["jobs"] = [job]
    try:
        _save_cfg(new_cfg)
    except Exception:
        pass
    _log("שודרגה תצורת ההגדרות לפורמט המשימות (v1.2) — ההתקדמות הקיימת נשמרה והועברה.", "info")
    return new_cfg


def _load_cfg():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, encoding="utf-8") as f:
                d = json.load(f)
            out = DEFAULT_CFG.copy(); out.update(d)
            if not out.get("jobs") and out.get("source_chat_id") and out.get("target_chat_id"):
                out = _migrate_to_jobs(out)
            return out
        except Exception: pass
    return DEFAULT_CFG.copy()

def _save_cfg(cfg):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)

def _last_id_path(job_key, source_id):
    return os.path.join(DATA_DIR, f"last_id_job{job_key}_{source_id}.txt")

def _save_last_id(mid, job_key, source_id):
    with open(_last_id_path(job_key, source_id), "w") as f: f.write(str(mid))

def _load_last_id(job_key, source_id):
    try:
        with open(_last_id_path(job_key, source_id)) as f: return int(f.read().strip())
    except Exception: return 0

def _worker_id_path(job_key, source_id, idx, total):
    return os.path.join(DATA_DIR, f"last_id_job{job_key}_{source_id}_part{total}_{idx}.txt")

def _save_worker_id(mid, job_key, source_id, idx, total):
    with open(_worker_id_path(job_key, source_id, idx, total), "w") as f: f.write(str(mid))

def _load_worker_id(job_key, source_id, idx, total):
    try:
        with open(_worker_id_path(job_key, source_id, idx, total)) as f: return int(f.read().strip())
    except Exception:
        return _load_last_id(job_key, source_id)

def _wave_path(job_key, source_id):
    return os.path.join(DATA_DIR, f"wave_job{job_key}_{source_id}.txt")

def _save_wave(start, end, job_key, source_id):
    with open(_wave_path(job_key, source_id), "w") as f: f.write(f"{start},{end}")

def _load_wave(job_key, source_id):
    try:
        with open(_wave_path(job_key, source_id)) as f:
            a, b = f.read().strip().split(",")
            return (int(a), int(b))
    except Exception:
        return None

def _clear_wave(job_key, source_id):
    try: os.remove(_wave_path(job_key, source_id))
    except Exception: pass

def _job_progress(job, total_accounts):
    job_key = job.get("id") or ""
    try:
        source = int(job.get("source_chat_id") or 0)
    except (TypeError, ValueError):
        return 0
    mode = job.get("mode", "fast")
    n = max(total_accounts, 1)
    if mode == "ordered" and total_accounts > 1:
        return _load_last_id(job_key, source)
    if total_accounts <= 1:
        return _load_worker_id(job_key, source, 0, 1)
    return max((_load_worker_id(job_key, source, i, n) for i in range(n)), default=0)

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
    phone = acc["phone_number"].strip().replace(" ", "").replace("-", "")
    if phone and not phone.startswith("+"):
        phone = "+" + phone
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


class _RateLimiter:
    def __init__(self, max_per_minute, tag=""):
        self.max = max_per_minute
        self.tag = tag
        self.count = 0
        self.window_start = datetime.now()
        self.lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self.lock:
                elapsed = (datetime.now() - self.window_start).total_seconds()
                if elapsed >= 60:
                    self.count = 0
                    self.window_start = datetime.now()
                    elapsed = 0
                if self.count < self.max:
                    self.count += 1
                    return
                wait = max(60 - elapsed, 0.5)
            _log(f"[{self.tag}] תקרת {self.max} הודעות/דקה הושגה — ממתין {int(wait)} שנ'", "warn")
            await asyncio.sleep(wait)


# ── Backup engine — per-job ───────────────────────────────────────────────────

class BackupEngine:
    def __init__(self, job_id, config):
        self.job_id  = job_id
        self.config  = config
        self.running = False
        self._paused = False

    def pause(self):
        self._paused = True
        if self.job_id in JOB_STATES:
            JOB_STATES[self.job_id]["paused"] = True
        _update_global_status()
        _log(f"[{self.job_id}] Paused.", "warn")

    def resume(self):
        self._paused = False
        if self.job_id in JOB_STATES:
            JOB_STATES[self.job_id]["paused"] = False
        _update_global_status()
        _log(f"[{self.job_id}] Resumed.", "success")

    def stop(self):
        self.running = False
        self._paused = False
        self._stopped_manually = True

    async def _wait_if_paused(self):
        while self._paused and self.running:
            await asyncio.sleep(0.3)

    async def _send_downloaded(self, client, target, msg, caption, tmp_dir, tag):
        work_dir = os.path.join(tmp_dir, f"{tag}_{msg.id}")
        os.makedirs(work_dir, exist_ok=True)
        path = None
        try:
            path = await client.download_media(msg.media, file=work_dir + os.sep)
            if path:
                await client.send_message(target, caption, file=path)
            else:
                await client.send_message(target, caption)
        finally:
            try:
                if path and os.path.exists(path): os.remove(path)
                os.rmdir(work_dir)
            except Exception:
                pass

    async def _worker(self, client, idx, total, job_tag, job_key, source, target, media_type, protected, tmp_dir, limiter):
        from telethon import errors
        tag = f"{job_tag} · Acc {idx+1}"
        while self.running:
            await self._wait_if_paused()
            if not self.running: break
            last_id = _load_worker_id(job_key, source, idx, total)
            try:
                last_proc = last_id
                async for msg in client.iter_messages(source, offset_id=last_id, reverse=True, limit=50):
                    if not self.running: break
                    await self._wait_if_paused()
                    if not self.running: break
                    if msg.id > last_proc: last_proc = msg.id

                    if total > 1 and msg.id % total != idx:
                        continue

                    if _msg_matches(msg, media_type):
                        try:
                            caption = re.sub(r"https?://\S+|www\.\S+|t\.me/\S+|@\S+", "", msg.text or "").strip()
                            if limiter:
                                await limiter.acquire()
                            if not msg.media:
                                await client.send_message(target, caption)
                            elif protected:
                                await self._send_downloaded(client, target, msg, caption, tmp_dir, tag)
                            else:
                                try:
                                    await client.send_message(target, caption, file=msg.media)
                                except Exception as exc:
                                    if "protected chat" in str(exc).lower():
                                        protected = True
                                        _log(f"[{tag}] זוהה ערוץ מוגן — עובר למצב הורדה+העלאה", "warn")
                                        await self._send_downloaded(client, target, msg, caption, tmp_dir, tag)
                                    elif "cannot use as file" in str(exc).lower() or "file reference" in str(exc).lower():
                                        await self._send_downloaded(client, target, msg, caption, tmp_dir, tag)
                                    else:
                                        raise
                            STATUS["transferred"] += 1
                            _log(f"[{tag}] Copied msg {msg.id} — סה\"כ: {STATUS['transferred']}", "success")
                            if not protected:
                                await asyncio.sleep(random.uniform(1.4, 3.8))
                        except errors.FloodWaitError as exc:
                            _log(f"[{tag}] FloodWait {exc.seconds}s", "warn")
                            _save_worker_id(last_proc, job_key, source, idx, total)
                            await asyncio.sleep(exc.seconds + 1)
                        except Exception as exc:
                            _log(f"[{tag}] Error msg {msg.id}: {exc}", "error")
                    else:
                        _log(f"[{tag}] Skip {msg.id}", "warn")

                if last_proc > last_id:
                    _save_worker_id(last_proc, job_key, source, idx, total)
                if not protected:
                    await asyncio.sleep(random.uniform(2, 6))
            except Exception as exc:
                _log(f"[{tag}] Error: {exc}", "error")
                if not client.is_connected():
                    try:
                        _log(f"[{tag}] מתחבר מחדש לטלגרם...", "info")
                        await client.connect()
                    except Exception: pass
                await asyncio.sleep(30)

    async def _range_worker(self, client, idx, total, job_tag, job_key, source, target, media_type,
                            protected, tmp_dir, limiter, range_start, range_end):
        from telethon import errors
        tag = f"{job_tag} · Acc {idx+1}"
        last_id = max(range_start - 1, _load_worker_id(job_key, source, idx, total))
        if last_id >= range_end:
            return
        _log(f"[{tag}] קופץ ישירות להודעה {last_id+1} (טווח {range_start}–{range_end})", "info")
        reached_end = False
        while self.running and not reached_end:
            await self._wait_if_paused()
            if not self.running: break
            last_proc = last_id
            try:
                async for msg in client.iter_messages(source, offset_id=last_id, reverse=True, limit=50):
                    if not self.running: break
                    await self._wait_if_paused()
                    if not self.running: break
                    if msg.id > range_end:
                        reached_end = True
                        break
                    if msg.id > last_proc: last_proc = msg.id

                    if _msg_matches(msg, media_type):
                        try:
                            caption = re.sub(r"https?://\S+|www\.\S+|t\.me/\S+|@\S+", "", msg.text or "").strip()
                            if limiter:
                                await limiter.acquire()
                            if not msg.media:
                                await client.send_message(target, caption)
                            elif protected:
                                await self._send_downloaded(client, target, msg, caption, tmp_dir, tag)
                            else:
                                try:
                                    await client.send_message(target, caption, file=msg.media)
                                except Exception as exc:
                                    if "protected chat" in str(exc).lower():
                                        protected = True
                                        _log(f"[{tag}] זוהה ערוץ מוגן — עובר למצב הורדה+העלאה", "warn")
                                        await self._send_downloaded(client, target, msg, caption, tmp_dir, tag)
                                    elif "cannot use as file" in str(exc).lower() or "file reference" in str(exc).lower():
                                        await self._send_downloaded(client, target, msg, caption, tmp_dir, tag)
                                    else:
                                        raise
                            STATUS["transferred"] += 1
                            _log(f"[{tag}] Copied msg {msg.id} — סה\"כ: {STATUS['transferred']}", "success")
                            if not protected:
                                await asyncio.sleep(random.uniform(1.4, 3.8))
                        except errors.FloodWaitError as exc:
                            _log(f"[{tag}] FloodWait {exc.seconds}s", "warn")
                            _save_worker_id(last_proc, job_key, source, idx, total)
                            await asyncio.sleep(exc.seconds + 1)
                        except Exception as exc:
                            _log(f"[{tag}] Error msg {msg.id}: {exc}", "error")
                    else:
                        _log(f"[{tag}] Skip {msg.id}", "warn")
                else:
                    reached_end = True

                if last_proc > last_id:
                    _save_worker_id(last_proc, job_key, source, idx, total)
                last_id = last_proc
                if not reached_end and not protected:
                    await asyncio.sleep(random.uniform(2, 6))
            except Exception as exc:
                _log(f"[{tag}] Error: {exc}", "error")
                if not client.is_connected():
                    try:
                        _log(f"[{tag}] מתחבר מחדש לטלגרם...", "info")
                        await client.connect()
                    except Exception: pass
                await asyncio.sleep(30)

        if reached_end:
            _save_worker_id(range_end, job_key, source, idx, total)
            _log(f"[{tag}] סיים את הטווח שלו ({range_start}–{range_end})", "info")

    async def _run_fast_job(self, job_tag, job_key, clients, source, target, media_type, protected, tmp_dir, use_limiter):
        n = len(clients)
        chunk_size = 25
        while self.running:
            await self._wait_if_paused()
            if not self.running: break

            base = _load_last_id(job_key, source)
            wave = _load_wave(job_key, source)
            if wave and wave[1] > base:
                wave_start, wave_end = wave
                _log(f"[{job_tag}] ממשיך סבב קיים: הודעות {wave_start}–{wave_end}", "info")
            else:
                try:
                    latest_msgs = await clients[0].get_messages(source, limit=1)
                    latest = latest_msgs[0].id if latest_msgs else base
                except Exception as exc:
                    _log(f"[{job_tag}] שגיאה בבדיקת ההודעה האחרונה: {exc}", "error")
                    await asyncio.sleep(30)
                    continue
                if latest <= base:
                    _log(f"[{job_tag}] אין הודעות חדשות כרגע — בודק שוב בעוד דקה", "info")
                    await asyncio.sleep(60)
                    continue
                wave_start = base + 1
                wave_end = min(base + n * chunk_size, latest)
                _save_wave(wave_start, wave_end, job_key, source)
                _log(f"[{job_tag}] סבב חדש: {wave_end - wave_start + 1} הודעות ({wave_start}–{wave_end})", "info")

            tasks = []
            for i, c in enumerate(clients):
                r_start = wave_start + i * chunk_size
                if r_start > wave_end:
                    continue
                r_end = min(r_start + chunk_size - 1, wave_end)
                limiter = _RateLimiter(25, tag=f"{job_tag} · Acc {i+1}") if use_limiter else None
                tasks.append(self._range_worker(c, i, n, job_tag, job_key, source, target, media_type,
                                                 protected, tmp_dir, limiter, r_start, r_end))
            if tasks:
                await asyncio.gather(*tasks)

            if not self.running:
                break

            _save_last_id(wave_end, job_key, source)
            _clear_wave(job_key, source)
            _log(f"[{job_tag}] הגל הושלם — עד הודעה {wave_end}", "success")

    async def _run_ordered_job(self, job_tag, job_key, clients, source, target, media_type, protected, tmp_dir):
        from telethon import errors
        n = len(clients)
        scanner = clients[0]
        sender  = clients[0]
        last_id = _load_last_id(job_key, source)

        q = asyncio.Queue(maxsize=n * 3)
        ready = {}
        cond  = asyncio.Condition()
        state = {"discovered_total": None}
        expected = 0

        async def discover():
            seq = 0
            try:
                async for msg in scanner.iter_messages(source, offset_id=last_id, reverse=True):
                    if not self.running: break
                    await self._wait_if_paused()
                    if not self.running: break
                    if _msg_matches(msg, media_type):
                        await q.put((seq, msg))
                        seq += 1
                    else:
                        _log(f"[{job_tag} · סריקה] Skip {msg.id}", "warn")
            except Exception as exc:
                _log(f"[{job_tag} · סריקה] Error: {exc}", "error")
            finally:
                state["discovered_total"] = seq
                async with cond:
                    cond.notify_all()
                for _ in range(n):
                    await q.put(None)

        async def prepare(client, idx):
            tag = f"{job_tag} · Acc {idx+1}"
            while True:
                item = await q.get()
                if item is None:
                    q.task_done()
                    break
                seq, msg = item
                await self._wait_if_paused()
                caption = re.sub(r"https?://\S+|www\.\S+|t\.me/\S+|@\S+", "", msg.text or "").strip()
                payload = None
                try:
                    if msg.media and protected:
                        work_dir = os.path.join(tmp_dir, f"{tag}_{msg.id}")
                        os.makedirs(work_dir, exist_ok=True)
                        path = await client.download_media(msg.media, file=work_dir + os.sep)
                        payload = {"kind": "downloaded", "path": path, "work_dir": work_dir,
                                   "caption": caption, "msg_id": msg.id}
                    elif msg.media:
                        payload = {"kind": "media_ref", "media": msg.media,
                                   "caption": caption, "msg_id": msg.id}
                    else:
                        payload = {"kind": "text", "caption": caption, "msg_id": msg.id}
                except Exception as exc:
                    _log(f"[{tag}] שגיאה בהכנת הודעה {msg.id}: {exc}", "error")
                    payload = {"kind": "error", "err": str(exc), "msg_id": msg.id}
                async with cond:
                    ready[seq] = payload
                    cond.notify_all()
                q.task_done()

        async def deliver():
            nonlocal expected
            tag = f"{job_tag} · Acc 1"
            while self.running:
                async with cond:
                    while (self.running and expected not in ready
                           and not (state["discovered_total"] is not None and expected >= state["discovered_total"])):
                        try:
                            await asyncio.wait_for(cond.wait(), timeout=1.0)
                        except asyncio.TimeoutError:
                            pass
                    if not self.running:
                        return
                    if expected not in ready:
                        return
                    payload = ready.pop(expected)

                await self._wait_if_paused()
                if not self.running: return
                kind   = payload["kind"]
                msg_id = payload["msg_id"]
                try:
                    if kind == "downloaded":
                        if payload["path"]:
                            await sender.send_message(target, payload["caption"], file=payload["path"])
                        else:
                            await sender.send_message(target, payload["caption"])
                    elif kind == "media_ref":
                        try:
                            await sender.send_message(target, payload["caption"], file=payload["media"])
                        except Exception as exc:
                            _log(f"[{tag}] Error msg {msg_id}: {exc}", "error")
                            expected += 1
                            continue
                    elif kind == "text":
                        await sender.send_message(target, payload["caption"])
                    elif kind == "error":
                        _log(f"[{tag}] מדלג על הודעה {msg_id} (נכשלה ההכנה: {payload['err']})", "error")
                        expected += 1
                        continue

                    STATUS["transferred"] += 1
                    _log(f"[{tag}] Copied msg {msg_id} — סה\"כ: {STATUS['transferred']}", "success")
                    _save_last_id(msg_id, job_key, source)
                    if not protected:
                        await asyncio.sleep(random.uniform(1.4, 3.8))
                except errors.FloodWaitError as exc:
                    _log(f"[{tag}] FloodWait {exc.seconds}s", "warn")
                    async with cond:
                        ready[expected] = payload
                        cond.notify_all()
                    await asyncio.sleep(exc.seconds + 1)
                    continue
                except Exception as exc:
                    _log(f"[{tag}] Error msg {msg_id}: {exc}", "error")
                finally:
                    if kind == "downloaded":
                        try:
                            p, wd = payload.get("path"), payload.get("work_dir")
                            if p and os.path.exists(p): os.remove(p)
                            if wd: os.rmdir(wd)
                        except Exception:
                            pass
                expected += 1

        await asyncio.gather(discover(), *[prepare(c, i) for i, c in enumerate(clients)], deliver())

    async def run(self):
        """Runs a SINGLE job (identified by self.job_id) independently."""
        from telethon import TelegramClient

        self.running = True
        JOB_STATES[self.job_id] = {"running": True, "paused": False, "engine": self}
        _update_global_status()

        cfg          = self.config
        accounts_cfg = cfg.get("accounts", [])

        # Find the specific job for this engine
        job = next((j for j in cfg.get("jobs", []) if j.get("id") == self.job_id), None)
        if not job:
            _log(f"[{self.job_id}] משימה לא נמצאה בתצורה", "error")
            self.running = False
            JOB_STATES[self.job_id]["running"] = False
            _update_global_status()
            return

        job_tag = job.get("name") or self.job_id
        job_key = self.job_id

        try:
            source = int(job.get("source_chat_id") or 0)
            target = int(job.get("target_chat_id") or 0)
        except (TypeError, ValueError):
            _log(f"[{job_tag}] Chat ID לא תקין", "error")
            self.running = False
            JOB_STATES[self.job_id]["running"] = False
            _update_global_status()
            return

        if not source or not target:
            _log(f"[{job_tag}] חסרים Chat ID של מקור/יעד", "error")
            self.running = False
            JOB_STATES[self.job_id]["running"] = False
            _update_global_status()
            return

        # Connect accounts assigned to this job
        clients = []
        for ai in (job.get("account_indices") or []):
            if not isinstance(ai, int) or ai < 0 or ai >= len(accounts_cfg):
                continue
            acc = accounts_cfg[ai]
            name = acc.get("client_name", "")
            if not acc.get("api_id") or not acc.get("api_hash"):
                continue
            # Reuse existing connected session if available
            state = AUTH_STATE.get(name)
            if state and state.get("status") == "connected" and state.get("client"):
                clients.append(state["client"])
                _log(f"[{job_tag}] {name} reusing session", "success")
            else:
                session_path = os.path.join(DATA_DIR, name)
                try:
                    c = TelegramClient(session_path, int(acc["api_id"]), acc["api_hash"])
                    await c.connect()
                    if await c.is_user_authorized():
                        clients.append(c)
                        AUTH_STATE[name] = {"client": c, "status": "connected"}
                        _log(f"[{job_tag}] {name} connected", "success")
                    else:
                        _log(f"[{job_tag}] {name} not authorized — use Connect button", "error")
                except Exception as exc:
                    _log(f"[{job_tag}] Failed {name}: {exc}", "error")

        if not clients:
            _log(f"[{job_tag}] אין חשבונות מחוברים וזמינים — מבטל", "error")
            self.running = False
            JOB_STATES[self.job_id]["running"] = False
            _update_global_status()
            return

        media_type = job.get("media_type", "video_doc")
        mode       = job.get("mode", "fast")
        n          = len(clients)

        start_from = int(job.get("start_from_id") or 0)
        if start_from:
            _save_last_id(start_from, job_key, source)
            for i in range(n):
                _save_worker_id(start_from, job_key, source, i, n)
            _log(f"[{job_tag}] קופץ ישירות להודעה {start_from} (חד-פעמי)", "info")
            try:
                full_cfg = _load_cfg()
                for jb in full_cfg.get("jobs", []):
                    if (jb.get("id") or "") == job_key:
                        jb["start_from_id"] = 0
                _save_cfg(full_cfg)
            except Exception:
                pass
            job["start_from_id"] = 0

        protected = False
        try:
            src_ent  = await clients[0].get_entity(source)
            protected = bool(getattr(src_ent, "noforwards", False))
            if protected:
                tmp_dir = os.path.join(DATA_DIR, "tmp_media")
                os.makedirs(tmp_dir, exist_ok=True)
                _log(f"[{job_tag}] ערוץ מקור מוגן — מצב הורדה+העלאה פעיל", "warn")
        except Exception:
            pass

        tmp_dir    = os.path.join(DATA_DIR, "tmp_media")
        use_limiter = (n == 1 or mode == "fast")
        if use_limiter:
            _log(f"[{job_tag}] תקרת 25 הודעות/דקה לחשבון — פעילה", "info")

        if n == 1:
            mode_label = "⚡ מהיר"
        elif mode == "fast":
            mode_label = "⚡ מהיר/מבולגן"
        else:
            mode_label = "📑 מסודר/כרונולוגי"
        _log(f"[{job_tag}] Active [{media_type}] — {source} → {target} | {n} חשבונות | {mode_label}", "info")

        try:
            if mode == "ordered" and n > 1:
                await self._run_ordered_job(job_tag, job_key, clients, source, target, media_type, protected, tmp_dir)
            elif n > 1:
                await self._run_fast_job(job_tag, job_key, clients, source, target, media_type, protected, tmp_dir, use_limiter)
            else:
                await self._worker(clients[0], 0, 1, job_tag, job_key, source, target, media_type, protected, tmp_dir,
                                   _RateLimiter(25, tag=f"{job_tag} · Acc 1") if use_limiter else None)
        except Exception as exc:
            _log(f"[{job_tag}] Engine error: {exc}", "error")
        finally:
            # Only disconnect clients we opened here (not reused AUTH_STATE sessions)
            for c in clients:
                try:
                    # Check if this is a reused session — don't disconnect it
                    is_reused = any(
                        s.get("client") is c
                        for s in AUTH_STATE.values()
                        if s.get("status") == "connected"
                    )
                    if not is_reused:
                        await c.disconnect()
                except Exception:
                    pass
            _log(f"[{job_tag}] Stopped.", "info")
            self.running = False
            JOB_STATES[self.job_id]["running"] = False
            _update_global_status()


def _start_single_job(job_id, config):
    """Start one job engine in its own coroutine on the shared TG loop."""
    def _run():
        while True:
            cfg = _load_cfg()
            if not any(j.get("id") == job_id for j in cfg.get("jobs", [])):
                break  # job was deleted from config
            engine = BackupEngine(job_id, cfg)
            engine._stopped_manually = False
            JOB_STATES[job_id] = {"running": True, "paused": False, "engine": engine}
            _update_global_status()
            try:
                future = asyncio.run_coroutine_threadsafe(engine.run(), _TG_LOOP)
                future.result()
            except Exception as e:
                _log(f"[{job_id}] Worker error: {e}", "error")
            # Manual stop → don't restart
            if getattr(engine, "_stopped_manually", False):
                break
            _log(f"[{job_id}] משימה נעצרה — מפעיל מחדש בעוד 10 שנ'", "warn")
            time.sleep(10)
        if job_id in JOB_STATES:
            JOB_STATES[job_id]["running"] = False
        _update_global_status()

    threading.Thread(target=_run, daemon=True).start()


# ── Flask ─────────────────────────────────────────────────────────────────────

def create_app():
    app = Flask(__name__, template_folder=TEMPLATE_DIR)

    @app.route("/")
    def index(): return render_template("index.html", config=_load_cfg(),
                                        version=APP_VERSION,
                                        tg_group=TG_GROUP_URL,
                                        github=GITHUB_URL,
                                        sentry_dsn=SENTRY_DSN)

    @app.route("/config", methods=["POST"])
    def update_config(): _save_cfg(request.json); return jsonify({"ok": True})

    # ── Auth ───────────────────────────────────────────────────────────────────

    @app.route("/auth/send_code", methods=["POST"])
    def auth_send_code():
        data = request.json; cfg = _load_cfg(); idx = data.get("acc_idx", 0)
        if idx >= len(cfg["accounts"]): return jsonify({"ok": False, "msg": "Account not found"})
        acc = cfg["accounts"][idx]
        if not acc.get("api_id") or not acc.get("phone_number"):
            return jsonify({"ok": False, "msg": "Fill API ID and Phone first"})
        try:
            result = _run_async(_auth_send_code(acc))
            return jsonify(result)
        except Exception as e: return jsonify({"ok": False, "msg": str(e)})

    @app.route("/auth/verify", methods=["POST"])
    def auth_verify():
        data = request.json; cfg = _load_cfg(); idx = data.get("acc_idx", 0)
        if idx >= len(cfg["accounts"]): return jsonify({"ok": False, "msg": "Account not found"})
        try:
            result = _run_async(_auth_verify_code(cfg["accounts"][idx], data.get("code", "")))
            return jsonify(result)
        except Exception as e: return jsonify({"ok": False, "msg": str(e)})

    @app.route("/auth/password", methods=["POST"])
    def auth_password():
        data = request.json; cfg = _load_cfg(); idx = data.get("acc_idx", 0)
        if idx >= len(cfg["accounts"]): return jsonify({"ok": False, "msg": "Account not found"})
        try:
            result = _run_async(_auth_password(cfg["accounts"][idx], data.get("password", "")))
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

    # ── Global controls (start/stop/pause/resume ALL jobs) ─────────────────────

    @app.route("/start", methods=["POST"])
    def start():
        cfg  = _load_cfg()
        jobs = cfg.get("jobs", [])
        if not jobs:
            return jsonify({"ok": False, "msg": "לא הוגדרו משימות — הוסף משימה בלשונית 'משימות'"})

        # Validate no account appears in more than one job
        seen: set = set()
        for job in jobs:
            for ai in (job.get("account_indices") or []):
                if ai in seen:
                    name = job.get("name", "")
                    return jsonify({"ok": False, "msg": f"חשבון אחד לא יכול להשתתף ביותר ממשימה אחת (זוהה במשימה '{name}')"})
                seen.add(ai)

        started = 0
        for job in jobs:
            jid = job.get("id")
            if not jid: continue
            if JOB_STATES.get(jid, {}).get("running"): continue
            if not job.get("source_chat_id") or not job.get("target_chat_id"): continue
            _log(f"Starting [{job.get('name') or jid}]…", "info")
            _start_single_job(jid, cfg)
            started += 1

        if started == 0:
            return jsonify({"ok": False, "msg": "כל המשימות כבר פועלות או שחסרים Chat ID"})
        return jsonify({"ok": True})

    @app.route("/stop", methods=["POST"])
    def stop():
        for jid, state in list(JOB_STATES.items()):
            engine = state.get("engine")
            if engine: engine.stop()
            JOB_STATES[jid]["running"] = False
        _update_global_status()
        _log("עצירת כל המשימות…", "warn")
        return jsonify({"ok": True})

    @app.route("/pause", methods=["POST"])
    def pause():
        for state in JOB_STATES.values():
            engine = state.get("engine")
            if engine and state.get("running"): engine.pause()
        return jsonify({"ok": True})

    @app.route("/resume", methods=["POST"])
    def resume():
        for state in JOB_STATES.values():
            engine = state.get("engine")
            if engine and state.get("running"): engine.resume()
        return jsonify({"ok": True})

    # ── Per-job controls ───────────────────────────────────────────────────────

    @app.route("/job/<job_id>/start", methods=["POST"])
    def job_start(job_id):
        if JOB_STATES.get(job_id, {}).get("running"):
            return jsonify({"ok": False, "msg": "כבר פועל"})
        cfg = _load_cfg()
        job = next((j for j in cfg.get("jobs", []) if j.get("id") == job_id), None)
        if not job:
            return jsonify({"ok": False, "msg": "משימה לא נמצאה"})
        if not job.get("source_chat_id") or not job.get("target_chat_id"):
            return jsonify({"ok": False, "msg": "חסרים Chat ID של מקור/יעד"})

        # Check that none of this job's accounts are busy in another running job
        my_accounts = set(job.get("account_indices") or [])
        for other_jid, other_state in JOB_STATES.items():
            if other_jid == job_id or not other_state.get("running"):
                continue
            other_job = next((j for j in cfg.get("jobs", []) if j.get("id") == other_jid), None)
            if not other_job: continue
            conflict = my_accounts & set(other_job.get("account_indices") or [])
            if conflict:
                acc_names = [cfg["accounts"][i]["client_name"] for i in conflict if i < len(cfg["accounts"])]
                return jsonify({"ok": False, "msg": f"חשבון {', '.join(acc_names)} כבר משויך למשימה רצה אחרת"})

        _log(f"Starting [{job.get('name') or job_id}]…", "info")
        _start_single_job(job_id, cfg)
        return jsonify({"ok": True})

    @app.route("/job/<job_id>/stop", methods=["POST"])
    def job_stop(job_id):
        state  = JOB_STATES.get(job_id, {})
        engine = state.get("engine")
        if engine: engine.stop()
        if job_id in JOB_STATES:
            JOB_STATES[job_id]["running"] = False
        _update_global_status()
        _log(f"[{job_id}] עוצר…", "warn")
        return jsonify({"ok": True})

    @app.route("/job/<job_id>/pause", methods=["POST"])
    def job_pause_route(job_id):
        state  = JOB_STATES.get(job_id, {})
        engine = state.get("engine")
        if engine and state.get("running"): engine.pause()
        return jsonify({"ok": True})

    @app.route("/job/<job_id>/resume", methods=["POST"])
    def job_resume_route(job_id):
        state  = JOB_STATES.get(job_id, {})
        engine = state.get("engine")
        if engine and state.get("running"): engine.resume()
        return jsonify({"ok": True})

    # ── Status ─────────────────────────────────────────────────────────────────

    @app.route("/status")
    def status():
        return jsonify({
            "running":     STATUS["running"],
            "paused":      STATUS["paused"],
            "transferred": STATUS["transferred"],
        })

    @app.route("/jobs_status")
    def jobs_status():
        cfg = _load_cfg()
        out = []
        for job in cfg.get("jobs", []):
            n   = len(job.get("account_indices") or [])
            jid = job.get("id", "")
            st  = JOB_STATES.get(jid, {})
            out.append({
                "id":      jid,
                "last_id": _job_progress(job, n),
                "running": st.get("running", False),
                "paused":  st.get("paused",  False),
            })
        return jsonify(out)

    @app.route("/reset_last_id", methods=["POST"])
    def reset_last_id():
        data   = request.json or {}
        job_id = data.get("job_id", "")
        cfg    = _load_cfg()
        job    = next((j for j in cfg.get("jobs", []) if j.get("id") == job_id), None)
        if not job:
            return jsonify({"ok": False, "msg": "Job not found"})
        try:
            source = int(job.get("source_chat_id") or 0)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "msg": "Invalid source chat id"})
        if source:
            _save_last_id(0, job_id, source)
            _clear_wave(job_id, source)
            for p in glob.glob(os.path.join(DATA_DIR, f"last_id_job{job_id}_{source}_part*_*.txt")):
                try: os.remove(p)
                except Exception: pass
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
