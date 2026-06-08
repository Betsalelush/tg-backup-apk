import asyncio
import glob
import hashlib
import json
import os
import queue
import random
import re
import shutil
import threading
from datetime import datetime

from flask import Flask, Response, jsonify, render_template, request

DATA_DIR    = os.environ.get("TG_DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
CONFIG_FILE = os.path.join(DATA_DIR, "config.json")
TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")

APP_VERSION  = "1.3"
BOT_TOKEN    = "8867679619:AAFf7O96HEbKako4rE-xg_kAHe-OICOQVFw"
REPORT_CHAT  = "@backuppppy"   # הבוט חייב להיות חבר בקבוצה
TG_GROUP_URL = "https://t.me/backuppppy"
GITHUB_URL   = "https://github.com/Betsalelush/tg-backup-apk"

LOG_Q  = queue.Queue(maxsize=500)
STATUS = {"running": False, "paused": False, "engine": None, "thread": None, "transferred": 0}
AUTH_STATE = {}


def _bot_send(text):
    """שולח הודעה לבוט — לעולם לא זורק שגיאה.
    כל הודעה (כולל דיווחי שגיאה/דיבאג) כוללת את גרסת האפליקציה בסוף,
    כדי שאפשר יהיה לדעת מאיזו גרסה הגיע כל דיווח."""
    try:
        import urllib.request, urllib.parse
        data = urllib.parse.urlencode({
            "chat_id": REPORT_CHAT,
            "text": f"{text}\n\n— גרסה {APP_VERSION}",
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
    "jobs": [],
    # כל job: {id, name, source_chat_id, target_chat_id, media_type,
    #          start_from_id, account_indices: [...], mode: "fast"|"ordered"}
}

# loop קבוע אחד לכל פעולות Telethon
_TG_LOOP = asyncio.new_event_loop()
threading.Thread(target=_TG_LOOP.run_forever, daemon=True).start()


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
            # מיגרציה מתצורה ישנה (מקור/יעד יחיד) למשימה ראשונה ברשימת jobs
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
    """שומר את גבולות ה'גל' הנוכחי (טווח ID רציף שמתחלק בין החשבונות במצב מהיר),
    כדי שאם האפליקציה נסגרת באמצע גל — הגל יתחדש מאותם גבולות בדיוק (ולא יחושב
    מחדש לפי 'הודעה אחרונה' עדכנית, שעלולה לשנות את חלוקת הטווחים בין החשבונות)."""
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
    """ערך 'התקדמות' תצוגתי למשימה — לפי המצב שלה ומספר החשבונות שמוקצים לה."""
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
    """תקרת קצב נוספת — *בנוסף* לכל ההגבלות/השהיות הקיימות, לא במקומן.
    מופע נפרד נוצר לכל חשבון בנפרד (לא משותף בין חשבונות!) — כך שכל חשבון
    מוגבל ל-25 הודעות/דקה משלו, גם כשכמה חשבונות עובדים על אותה משימה.
    סופרת הודעות בחלון נע של 60 שניות, וכשמגיעים לתקרה ממתינה עד שהחלון
    מתאפס. מופעלת רק כשחשבון יחיד מעביר או כשכמה חשבונות עובדים במצב
    'מהיר/מבולגן' (ראו היצירה של _RateLimiter ב-run)."""
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
            _log(f"[{self.tag}] תקרת {self.max} הודעות/דקה הושגה — ממתין {int(wait)} שנ' (בנוסף להגבלות הרגילות)", "warn")
            await asyncio.sleep(wait)


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

    async def _send_downloaded(self, client, target, msg, caption, tmp_dir, tag):
        """מוריד את המדיה פיזית למכשיר ומעלה אותה כקובץ חדש — עוקף 'protected chat'.
        כל הודעה מקבלת תת-תיקייה ייחודית (לפי חשבון+מזהה הודעה) כדי שעבודה
        מקבילה של כמה חשבונות לא תתנגש על אותו שם קובץ זמני."""
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
        """לולאת עבודה רציפה לחשבון יחיד שאחראי על *כל* ערוץ המקור (total תמיד 1 כאן —
        עבור משימות עם כמה חשבונות במצב מהיר משתמשים ב-_run_fast_job/_range_worker
        שמחלקים טווחי-ID רציפים, ובמצב מסודר ב-_run_ordered_job).
        ממשיך מ-offset_id=last_id קדימה ברצף, בלי שום 'סריקה' מההתחלה.
        במצב מוגן (הורדה+העלאה) לא מחילים את עיכובי ההאטה הרגילים — הם נועדו לערוצים
        רגילים בלבד; בערוץ מוגן ההורדה/העלאה כבר איטית מספיק מצד עצמה.
        limiter — תקרת קצב נוספת (25/דקה) ייעודית *לחשבון הזה בלבד*; מצטרפת
        *מעבר* לעיכובים הרגילים, לא במקומם — ראו run()."""
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
                                        _log(f"[{tag}] זוהה ערוץ מוגן — עובר למצב הורדה+העלאה, ללא הגבלות שניות (תקרת הקצב הנוספת אם פעילה — נשארת)", "warn")
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
                threading.Thread(target=_bot_send, args=(f"⚠️ TG Backup error ({tag}):\n{exc}",), daemon=True).start()
                await asyncio.sleep(30)

    async def _range_worker(self, client, idx, total, job_tag, job_key, source, target, media_type,
                            protected, tmp_dir, limiter, range_start, range_end):
        """עובד עם טווח-ID קבוע ורציף (range_start..range_end) — קופץ ישירות
        (offset_id) להודעה הראשונה בטווח שלו וממשיך משם בלבד. לעולם לא נוגע
        בהודעה שמחוץ לטווח שלו — לא של חשבון אחר ולא 'לפני' נקודת ההתחלה —
        כך שאין שום סריקה כפולה/מבוזבזת על תוכן ששייך למישהו אחר.
        בסיום הטווח החשבון פשוט מסיים (ה'גל' הבא ב-_run_fast_job יקצה לו טווח חדש)."""
        from telethon import errors
        tag = f"{job_tag} · Acc {idx+1}"
        last_id = max(range_start - 1, _load_worker_id(job_key, source, idx, total))
        if last_id >= range_end:
            return
        _log(f"[{tag}] קופץ ישירות להודעה {last_id+1} (טווח {range_start}–{range_end} — בלי לסרוק טווחים אחרים)", "info")
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
                                        _log(f"[{tag}] זוהה ערוץ מוגן — עובר למצב הורדה+העלאה, ללא הגבלות שניות (תקרת הקצב הנוספת אם פעילה — נשארת)", "warn")
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
                    # אין יותר הודעות זמינות בטווח — סיימנו אותו
                    reached_end = True

                if last_proc > last_id:
                    _save_worker_id(last_proc, job_key, source, idx, total)
                last_id = last_proc
                if not reached_end and not protected:
                    await asyncio.sleep(random.uniform(2, 6))
            except Exception as exc:
                _log(f"[{tag}] Error: {exc}", "error")
                threading.Thread(target=_bot_send, args=(f"⚠️ TG Backup error ({tag}):\n{exc}",), daemon=True).start()
                await asyncio.sleep(30)

        if reached_end:
            _save_worker_id(range_end, job_key, source, idx, total)
            _log(f"[{tag}] סיים את הטווח שלו ({range_start}–{range_end}) — ממתין לגל הבא", "info")

    async def _run_fast_job(self, job_tag, job_key, clients, source, target, media_type, protected, tmp_dir, use_limiter):
        """מצב 'מהיר/מבולגן' עם כמה חשבונות — עובד ב'סבבים' קטנים וקבועים:
        בכל סבב כל חשבון מקבל טווח-ID **רציף וקבוע בגודלו — 25 הודעות בדיוק**
        (לא טווח גדול ומשתנה שנקבע לפי כמות ההודעות החדשות שהצטברו). חשבון 1
        מקבל את 25 ההודעות הראשונות מנקודת ההמשך, חשבון 2 את ה-25 שאחריו וכו'.
        כל חשבון קופץ ישירות (offset_id) לתחילת החלק שלו ועובד אך ורק עליו
        (_range_worker) — בלי לגעת בהודעה אחת שמחוץ לטווח שלו. בסיום הסבב כולם
        מתקדמים יחד לסבב הבא (25 הודעות נוספות לכל אחד), וכן הלאה — ברצף, בלי
        סריקה חוזרת ובלי בזבוז על תוכן ששייך לחשבון אחר (בניגוד לחלוקה לפי
        modulo, id % n, שגרמה לכל חשבון לעבור על כל הערוץ ולזרוק את רוב מה
        שהוא רואה)."""
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
                    _log(f"[{job_tag}] שגיאה בבדיקת ההודעה האחרונה בערוץ: {exc}", "error")
                    await asyncio.sleep(30)
                    continue
                if latest <= base:
                    _log(f"[{job_tag}] אין הודעות חדשות כרגע בערוץ — בודק שוב בעוד דקה", "info")
                    await asyncio.sleep(60)
                    continue
                wave_start = base + 1
                wave_end = min(base + n * chunk_size, latest)
                _save_wave(wave_start, wave_end, job_key, source)
                _log(f"[{job_tag}] סבב חדש: {wave_end - wave_start + 1} הודעות ({wave_start}–{wave_end}) — {chunk_size} הודעות רצופות לכל חשבון, כל אחד קופץ ישירות לחלק שלו", "info")

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
            _log(f"[{job_tag}] הגל הושלם — כל החשבונות סיימו את הטווחים שלהם (עד הודעה {wave_end})", "success")

    async def _run_ordered_job(self, job_tag, job_key, clients, source, target, media_type, protected, tmp_dir):
        """מצב 'מסודר/כרונולוגי': כמה חשבונות מכינים הודעות (קוראים/מורידים מדיה)
        במקביל — אבל השליחה בפועל ליעד מתבצעת אחת-אחת, לפי הסדר הכרונולוגי
        של ערוץ המקור, כך שאין 'בלאגן' ביעד. תפקידים:
          • סורק (clients[0]) — עובר על ההודעות לפי הסדר וממיין אותן ל-seq עולה
          • מכינים (כל clients) — מורידים מדיה/מכינים caption במקביל
          • שולח יחיד (clients[0]) — שולח ליעד לפי expected seq, אחת בכל פעם
        אין כאן תקרת 25/דקה נוספת (זו מיועדת רק לחשבון יחיד / מצב מהיר-מבולגן —
        ראו run); שאר ההגבלות (עיכוב 1.4-3.8 שנ' בערוץ לא-מוגן) כן חלות."""
        from telethon import errors
        n = len(clients)
        scanner = clients[0]
        sender = clients[0]
        last_id = _load_last_id(job_key, source)

        queue = asyncio.Queue(maxsize=n * 3)
        ready = {}
        cond = asyncio.Condition()
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
                        await queue.put((seq, msg))
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
                    await queue.put(None)

        async def prepare(client, idx):
            tag = f"{job_tag} · Acc {idx+1}"
            while True:
                item = await queue.get()
                if item is None:
                    queue.task_done()
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
                queue.task_done()

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
                        return  # נסרקו כל ההודעות התואמות ונשלחו
                    payload = ready.pop(expected)

                await self._wait_if_paused()
                if not self.running: return
                kind = payload["kind"]
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
        from telethon import TelegramClient
        self.running = True
        STATUS["transferred"] = 0
        accounts_cfg = self.config.get("accounts", [])
        jobs = self.config.get("jobs", [])

        if not jobs:
            _log("לא הוגדרו משימות — אין מה להפעיל.", "error")
            self.running = False; STATUS["running"] = False; return

        # מתחברים פעם אחת לכל החשבונות המוגדרים — כל משימה תקבל את תת-הקבוצה שלה
        name_to_client = {}
        for acc in accounts_cfg:
            name = acc.get("client_name", "")
            if not acc.get("api_id") or not acc.get("api_hash"): continue
            state = AUTH_STATE.get(name)
            if state and state["status"] == "connected" and state.get("client"):
                name_to_client[name] = state["client"]
                _log(f"{name} reusing session", "success")
            else:
                session_path = os.path.join(DATA_DIR, name)
                try:
                    c = TelegramClient(session_path, int(acc["api_id"]), acc["api_hash"])
                    await c.connect()
                    if await c.is_user_authorized():
                        name_to_client[name] = c
                        AUTH_STATE[name] = {"client": c, "status": "connected"}
                        _log(f"{name} connected", "success")
                    else:
                        _log(f"{name} not authorized — use Connect button", "error")
                except Exception as exc:
                    _log(f"Failed {name}: {exc}", "error")

        if not name_to_client:
            _log("No authorized accounts — aborting.", "error")
            self.running = False; STATUS["running"] = False; return

        tmp_dir = os.path.join(DATA_DIR, "tmp_media")
        job_tasks = []
        used_indices = set()  # אכיפת "חשבון אחד למשימה אחת בלבד"

        for j_idx, job in enumerate(jobs):
            job_tag = job.get("name") or f"משימה {j_idx+1}"
            job_key = job.get("id") or f"job{j_idx}"
            try:
                source = int(job.get("source_chat_id") or 0)
                target = int(job.get("target_chat_id") or 0)
            except (TypeError, ValueError):
                _log(f"[{job_tag}] Chat ID לא תקין — מדלג על המשימה", "error")
                continue
            if not source or not target:
                _log(f"[{job_tag}] חסרים Chat ID — מדלג על המשימה", "error")
                continue

            clients = []
            for ai in (job.get("account_indices") or []):
                if not isinstance(ai, int) or ai < 0 or ai >= len(accounts_cfg):
                    continue
                if ai in used_indices:
                    _log(f"[{job_tag}] חשבון #{ai+1} כבר משויך למשימה אחרת — מדלג עליו (חשבון אחד יכול להשתתף במשימה אחת בלבד)", "warn")
                    continue
                name = accounts_cfg[ai].get("client_name", "")
                client = name_to_client.get(name)
                if not client:
                    _log(f"[{job_tag}] חשבון #{ai+1} ({name}) לא מחובר — מדלג עליו", "warn")
                    continue
                used_indices.add(ai)
                clients.append(client)

            if not clients:
                _log(f"[{job_tag}] אין חשבונות מחוברים וזמינים למשימה — מדלגים עליה", "error")
                continue

            media_type = job.get("media_type", "video_doc")
            mode       = job.get("mode", "fast")
            n          = len(clients)
            start_from = int(job.get("start_from_id") or 0)
            if start_from:
                # קפיצה ישירה להודעה המוגדרת — חד-פעמית, ללא "סריקה" מההתחלה.
                # אחרי שמשתמשים בערך פעם אחת מנקים אותו מהתצורה כדי שלא יתאפס
                # שוב בכל הפעלה (זה היה גורם להעברה "לא להתקדם" — מתחיל מאותה
                # נקודה בכל ריצה במקום להמשיך מההתקדמות השמורה).
                _save_last_id(start_from, job_key, source)
                for i in range(n):
                    _save_worker_id(start_from, job_key, source, i, n)
                _log(f"[{job_tag}] קופץ ישירות להודעה {start_from} (חד-פעמי, בלי לסרוק) — מכאן ימשיך מההתקדמות שנשמרת", "info")
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
                src_ent = await clients[0].get_entity(source)
                protected = bool(getattr(src_ent, "noforwards", False))
                if protected:
                    os.makedirs(tmp_dir, exist_ok=True)
                    _log(f"[{job_tag}] ערוץ מקור מוגן (Protected Content) — מצב הורדה+העלאה פעיל, ללא הגבלות שניות", "warn")
            except Exception:
                pass

            # תקרת 25 הודעות/דקה נוספת — רק כשחשבון יחיד מעביר, או כשכמה
            # חשבונות עובדים במצב 'מהיר/מבולגן'. התקרה היא *לכל חשבון בנפרד*
            # (כל חשבון עד 25/דקה משלו — לא תקרה משותפת לכלל המשימה).
            # במצב 'מסודר' עם כמה חשבונות — אין תקרה נוספת (השליחה ממילא טורית
            # ומסודרת). שאר ההגבלות נשארות בכל מצב.
            use_limiter = (n == 1 or mode == "fast")
            if use_limiter:
                _log(f"[{job_tag}] תקרת קצב נוספת פעילה: עד 25 הודעות לדקה לכל חשבון בנפרד (בנוסף לשאר ההגבלות)", "info")

            mode_label = "⚡ מהיר/מבולגן" if (mode == "fast" or n == 1) else "📑 מסודר/כרונולוגי"
            _log(f"[{job_tag}] Active [{media_type}] — {source} → {target} | {n} חשבונות | מצב: {mode_label}", "info")

            if mode == "ordered" and n > 1:
                job_tasks.append(self._run_ordered_job(job_tag, job_key, clients, source, target, media_type, protected, tmp_dir))
            elif n > 1:
                # מצב מהיר עם כמה חשבונות — חלוקה לטווחי-ID רציפים וקבועים מראש
                # (כל חשבון קופץ ישירות לטווח שלו, ללא סריקה כפולה/מבוזבזת).
                job_tasks.append(self._run_fast_job(job_tag, job_key, clients, source, target, media_type, protected, tmp_dir, use_limiter))
            else:
                job_tasks.append(
                    self._worker(clients[0], 0, 1, job_tag, job_key, source, target, media_type, protected, tmp_dir,
                                 _RateLimiter(25, tag=f"{job_tag} · Acc 1") if use_limiter else None)
                )

        if not job_tasks:
            _log("אין משימות פעילות לביצוע (כולן דולגו).", "error")
            self.running = False; STATUS["running"] = False
            for c in name_to_client.values():
                try: await c.disconnect()
                except Exception: pass
            return

        await asyncio.gather(*job_tasks)

        for c in name_to_client.values():
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
        jobs = cfg.get("jobs", [])
        if not jobs:
            return jsonify({"ok": False, "msg": "לא הוגדרו משימות — הוסף משימה בלשונית 'משימות'"})
        seen = set()
        for j in jobs:
            name = j.get("name") or "משימה"
            if not j.get("source_chat_id") or not j.get("target_chat_id"):
                return jsonify({"ok": False, "msg": f"במשימה '{name}' חסרים Chat ID של מקור/יעד"})
            for ai in (j.get("account_indices") or []):
                if ai in seen:
                    return jsonify({"ok": False, "msg": f"חשבון אחד לא יכול להיות משויך ליותר ממשימה אחת (זוהה במשימה '{name}')"})
                seen.add(ai)
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

    @app.route("/jobs_status")
    def jobs_status():
        cfg = _load_cfg()
        out = []
        for job in cfg.get("jobs", []):
            n = len(job.get("account_indices") or [])
            out.append({"id": job.get("id", ""), "last_id": _job_progress(job, n)})
        return jsonify(out)

    @app.route("/reset_last_id", methods=["POST"])
    def reset_last_id():
        data = request.json or {}
        job_id = data.get("job_id", "")
        cfg = _load_cfg()
        job = next((j for j in cfg.get("jobs", []) if j.get("id") == job_id), None)
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
