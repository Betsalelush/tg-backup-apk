#!/usr/bin/env python3
"""Polls Sentry for new issues and relays them to a Telegram chat via bot.

All credentials (Sentry auth token, Telegram bot token/chat id) are read
from environment variables, which the GitHub Actions workflow injects from
encrypted repo secrets. Nothing here is sensitive on its own.
"""
import json
import os
import urllib.parse
import urllib.request

SENTRY_TOKEN = os.environ["SENTRY_AUTH_TOKEN"]
SENTRY_ORG = os.environ["SENTRY_ORG"]
SENTRY_PROJECT = os.environ["SENTRY_PROJECT"]
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

STATE_FILE = "state.json"
MAX_REMEMBERED = 300


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {"seen": []}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def sentry_get(path, query):
    url = f"https://sentry.io/api/0/{path}?{query}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {SENTRY_TOKEN}"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def send_telegram(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }).encode()
    req = urllib.request.Request(url, data=data)
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def format_issue(issue):
    title = issue.get("title") or "Error"
    culprit = issue.get("culprit") or ""
    count = issue.get("count", "?")
    link = issue.get("permalink", "")
    lines = [
        f"\U0001F534 <b>שגיאה חדשה ב-{SENTRY_PROJECT}</b>",
        f"<b>{title}</b>",
    ]
    if culprit:
        lines.append(culprit)
    lines.append(f"מספר אירועים: {count}")
    if link:
        lines.append(link)
    return "\n".join(lines)


def main():
    state = load_state()
    seen = list(state.get("seen", []))
    seen_set = set(seen)

    issues = sentry_get(
        f"projects/{SENTRY_ORG}/{SENTRY_PROJECT}/issues/",
        "query=is:unresolved&sort=new&statsPeriod=24h&limit=25",
    )

    new_issues = [i for i in issues if i["id"] not in seen_set]
    if not new_issues:
        return

    # oldest-first so messages land in the group in chronological order
    for issue in reversed(new_issues):
        send_telegram(format_issue(issue))
        seen.append(issue["id"])
        seen_set.add(issue["id"])

    state["seen"] = seen[-MAX_REMEMBERED:]
    save_state(state)


if __name__ == "__main__":
    main()
