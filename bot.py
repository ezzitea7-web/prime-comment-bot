import hashlib
import hmac
import json
import os
import re
import sqlite3
import threading
import time
from contextlib import closing
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import Request, urlopen

DATABASE = os.getenv("BOT_DB", "bot.sqlite3")
DAY = 86400

PRIVACY_POLICY = "PRIME COMMENT BOT\nPrivacy Policy and Data Deletion\nEffective: 8 November 2026\n\nWho runs the bot\nPrime Comment Bot is operated by Bedisciplined for its Instagram account @primedijitalstudio. Contact: https://t.me/BedisciplinedSupport\n\nWhat the bot processes and why\nThe bot receives or periodically retrieves information about Instagram comments from Meta, which may include the commenter’s account identifier, username, comment text, comment identifier, and post identifier. The bot checks whether the comment is on a configured post. Campaigns may accept any comment on that post or require a keyword. If it matches, the bot uses the comment identifier to send the requested product link by Instagram direct message and may post a public reply confirming delivery or directing the commenter to the storefront if the private reply is blocked. The bot also uses the comment identifier and delivery status to retry eligible requests and avoid duplicate replies.\n\nStorage and retention\nThe bot does not write the commenter’s username, account identifier, or comment text to its queue. For matching comments, the queue stores the comment identifier, the business account identifier, the prepared response, timestamps, and delivery status. Queue records expire seven days after the event timestamp and are removed by the bot when it is running. Railway hosts the bot and its database. Meta processes the original comments and the replies under its own policies. This page covers the bot, and any linked storefront has its own privacy practices.\n\nData access and deletion\nAnyone whose comment was processed can request access to or deletion of their bot data. Contact https://t.me/BedisciplinedSupport with the Instagram username you used and a link to the post or comment, plus the approximate time of the comment if known. We may ask for enough information to verify the request and locate the record. Please do not send your password. A request to Bedisciplined concerns data held by the bot. You can manage the original comment or message through Instagram."

def database():
    return closing(sqlite3.connect(DATABASE, timeout=15))

def init():
    with database() as db, db:
        db.execute("""CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY, account TEXT NOT NULL, dm TEXT NOT NULL,
            reply TEXT NOT NULL, stage TEXT NOT NULL DEFAULT 'dm',
            due REAL NOT NULL, expires REAL NOT NULL, attempts INTEGER DEFAULT 0)""")
        db.execute("CREATE INDEX IF NOT EXISTS due_jobs ON jobs(stage, due)")
        db.execute("""CREATE TABLE IF NOT EXISTS comment_poll_state (
            media_id TEXT PRIMARY KEY, starts_at REAL NOT NULL, watermark REAL NOT NULL)""")
        columns = {column[1] for column in db.execute("PRAGMA table_info(jobs)")}
        if "dm_template" not in columns:
            db.execute("ALTER TABLE jobs ADD COLUMN dm_template TEXT")

def dm_template(campaign):
    return {"attachment": {"type": "template", "payload": {
        "template_type": "button",
        "text": campaign["dm"],
        "buttons": [{"type": "web_url", "title": campaign["button_title"], "url": campaign["url"]}],
    }}}

def enqueue(event, account, campaigns):
    if event.get("object") != "instagram":
        return
    now = time.time()
    with database() as db, db:
        for entry in event.get("entry", []):
            if str(entry.get("id")) != account:
                continue
            for change in entry.get("changes", []):
                if change.get("field") != "comments":
                    continue
                value = change.get("value", {})
                author = value.get("from") or {}
                comment = str(value.get("id", ""))
                media = str((value.get("media") or {}).get("id", ""))
                if not comment.isdigit() or not author.get("id") or str(author["id"]) == account:
                    continue
                words = set(re.findall(r"\w+", str(value.get("text", "")).casefold()))
                for campaign in campaigns:
                    if campaign.get("poll_username") and author.get("username", "").casefold() == campaign["poll_username"].casefold():
                        continue
                    keyword = campaign.get("keyword")
                    if media != campaign["media_id"] or (keyword is not None and keyword.casefold() not in words):
                        continue
                    template = json.dumps(dm_template(campaign)) if "button_title" in campaign else None
                    text = campaign["dm"] if template else campaign["dm"] + "\n" + campaign["url"]
                    db.execute("""INSERT OR IGNORE INTO jobs (id,account,dm,reply,due,expires,dm_template)
                        VALUES (?,?,?,?,?,?,?)""", (comment, account, text, campaign["reply"], now,
                        min(float(entry.get("time", now)), now) + 7 * DAY, template))
                    break

def graph(path, body=None):
    version = os.environ["IG_API_VERSION"]
    login_mode = os.getenv("IG_LOGIN_MODE", "instagram")
    host = "graph.facebook.com" if login_mode == "facebook" else "graph.instagram.com"
    request = Request(f"https://{host}/{version}/{path}",
                      data=json.dumps(body).encode() if body is not None else None,
                      headers={"Authorization": "Bearer " + os.environ["IG_TOKEN"],
                               "Content-Type": "application/json"}, method="POST" if body is not None else "GET")
    with urlopen(request, timeout=20) as response:
        return json.load(response)

def poll_comments(account, campaign, own_username):
    media = campaign["media_id"]
    with database() as db, db:
        now = time.time()
        db.execute("INSERT OR IGNORE INTO comment_poll_state VALUES (?,?,?)", (media, now, now))
        starts_at, watermark = db.execute(
            "SELECT starts_at,watermark FROM comment_poll_state WHERE media_id=?", (media,)
        ).fetchone()
    cutoff = max(starts_at, watermark - 120, now - 7 * DAY)
    parameters = {"fields": "id,text,timestamp,from{id,username}", "limit": 50}
    comments = []
    next_watermark = watermark
    cursors = set()
    while True:
        response = graph(media + "/comments?" + urlencode(parameters))
        batch = response["data"]
        for comment in batch:
            created = datetime.fromisoformat(comment["timestamp"]).timestamp()
            if created < cutoff:
                continue
            next_watermark = max(next_watermark, min(created, now))
            author = comment.get("from") or {}
            if author.get("username", "").casefold() == own_username:
                continue
            comments.append((created, {
                "field": "comments",
                "value": {**comment, "media": {"id": media}},
            }))
        paging = response.get("paging") or {}
        cursor = (paging.get("cursors") or {}).get("after")
        if not paging.get("next") or not cursor:
            break
        if cursor in cursors:
            raise ValueError("Repeated comment cursor")
        cursors.add(cursor)
        parameters["after"] = cursor
    for created, change in sorted(comments, key=lambda item: item[0]):
        enqueue({"object": "instagram", "entry": [{
            "id": account, "time": created, "changes": [change],
        }]}, account, [campaign])
    with database() as db, db:
        db.execute("UPDATE comment_poll_state SET watermark=? WHERE media_id=?", (next_watermark, media))

def poll_worker(account, campaigns):
    ready = set()
    while True:
        for campaign in campaigns:
            if not campaign.get("poll_username"):
                continue
            try:
                poll_comments(account, campaign, campaign["poll_username"].casefold())
                if campaign["media_id"] not in ready:
                    ready.add(campaign["media_id"])
                    print("Comment polling ready for configured Reel", flush=True)
            except HTTPError as error:
                print("Comment polling HTTP error:", error.code, flush=True)
                error.close()
            except Exception as error:
                print("Comment polling error:", type(error).__name__, flush=True)
        time.sleep(30)

def work():
    while True:
        try:
            process_one()
        except Exception as error:
            print("Worker error:", type(error).__name__, flush=True)
        time.sleep(6)

def process_one():
    with database() as db:
        now = time.time()
        with db:
            db.execute("DELETE FROM jobs WHERE expires < ?", (now,))
        job = db.execute("""SELECT id,account,dm,reply,stage,attempts,dm_template FROM jobs
            WHERE stage IN ('dm','reply') AND due <= ? ORDER BY due LIMIT 1""", (now,)).fetchone()
        if not job:
            return
        comment, account, dm, reply, stage, attempts, template = job
        # Persist the in-flight state before sending. If a request's outcome is
        # ambiguous, hold the job for inspection to avoid duplicate public replies.
        with db:
            db.execute("UPDATE jobs SET stage=? WHERE id=?", (stage + "_pending", comment))
        try:
            if stage == "dm":
                message = json.loads(template) if template else {"text": dm}
                graph(account + "/messages", {"recipient": {"comment_id": comment}, "message": message})
            else:
                graph(comment + "/replies", {"message": reply})
        except HTTPError as error:
            payload = error.read()
            try:
                details = json.loads(payload).get("error", {})
                code = details.get("code")
                message = str(details.get("message", ""))
                message = message.replace(os.environ["IG_TOKEN"], "[redacted]")
                message = re.sub(r"EAA[A-Za-z0-9_|-]+", "[redacted]", message)
                print("Meta error details:", stage, error.code, code,
                      details.get("error_subcode"), message[:800], flush=True)
            except (ValueError, AttributeError):
                code = None
            retry = (error.code == 429 or code in {4, 17, 32, 613}) and attempts < 12
            next_stage = stage if retry else "failed"
            if error.code >= 500 and not retry:
                next_stage = stage + "_pending"
            if stage == "dm" and error.code == 403 and code == 200:
                with db:
                    db.execute("UPDATE jobs SET stage='reply',reply=?,due=?,attempts=0 WHERE id=?",
                               ("Thanks for commenting! You can find the Habit Tracker at bedisciplined.shop 🎯",
                                now, comment))
                print("Meta denied private reply; queued public reply", flush=True)
                return
            with db:
                db.execute("UPDATE jobs SET stage=?,due=?,attempts=? WHERE id=?",
                           (next_stage, now + min(3600, 60 * 2 ** attempts), attempts + 1, comment))
            print("Meta response:", error.code, code, next_stage, flush=True)
        except OSError:
            print("Meta request outcome unknown:", stage, flush=True)
        else:
            with db:
                db.execute("UPDATE jobs SET stage=?,due=?,attempts=0 WHERE id=?",
                           ("reply" if stage == "dm" else "done", now, comment))

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def respond(self, status, content=""):
        body = content.encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        url = urlsplit(self.path)
        if url.path in {"/privacy", "/data-deletion"}:
            return self.respond(200, PRIVACY_POLICY)
        if url.path == "/health":
            return self.respond(200, "ok")
        if url.path != "/webhook":
            return self.respond(404)
        query = parse_qs(url.query)
        token = query.get("hub.verify_token", [""])[0]
        if query.get("hub.mode") == ["subscribe"] and hmac.compare_digest(
                token.encode(), os.environ["WEBHOOK_VERIFY_TOKEN"].encode()):
            return self.respond(200, query.get("hub.challenge", [""])[0])
        self.respond(403)

    def do_POST(self):
        if urlsplit(self.path).path != "/webhook":
            return self.respond(404)
        length = self.headers.get("Content-Length", "")
        if not length.isdigit() or not 0 < int(length) <= 1024 * 1024:
            return self.respond(400)
        raw = self.rfile.read(int(length))
        expected = "sha256=" + hmac.new(os.environ["META_APP_SECRET"].encode(),
                                       raw, hashlib.sha256).hexdigest()
        signature = self.headers.get("X-Hub-Signature-256", "")
        if not hmac.compare_digest(expected.encode(), signature.encode()):
            return self.respond(403)
        try:
            event = json.loads(raw)
            enqueue(event, os.environ["IG_ACCOUNT_ID"], self.server.campaigns)
        except (ValueError, TypeError, AttributeError):
            return self.respond(400)
        except sqlite3.Error:
            return self.respond(503)
        self.respond(200, "ok")

if __name__ == "__main__":
    if os.getenv("IG_LOGIN_MODE", "instagram") not in {"instagram", "facebook"}:
        raise SystemExit("IG_LOGIN_MODE must be instagram or facebook")
    for name in ("META_APP_SECRET", "WEBHOOK_VERIFY_TOKEN", "IG_TOKEN", "IG_ACCOUNT_ID", "IG_API_VERSION"):
        if not os.getenv(name):
            raise SystemExit("Missing configuration: " + name)
    if not re.fullmatch(r"v\d+\.\d+", os.environ["IG_API_VERSION"]) or not os.environ["IG_ACCOUNT_ID"].isdigit():
        raise SystemExit("Invalid API version or account ID")
    with open(os.getenv("CAMPAIGN_FILE", "campaigns.json"), encoding="utf-8") as file:
        campaigns = json.load(file)
    for campaign in campaigns:
        url = urlsplit(campaign["url"])
        keyword = campaign.get("keyword")
        if (not campaign["media_id"].isdigit()
                or (keyword is not None and not re.fullmatch(r"\w+", keyword))
                or url.scheme != "https" or not url.hostname or not campaign["dm"].strip()
                or not campaign["reply"].strip()):
            raise SystemExit("Invalid campaign configuration")
        if "button_title" in campaign and not 0 < len(campaign["button_title"].strip()) <= 20:
            raise SystemExit("Button title must have 1–20 characters")
    init()
    server = ThreadingHTTPServer(("0.0.0.0", int(os.getenv("PORT", "8080"))), Handler)
    server.campaigns = campaigns
    threading.Thread(target=work, daemon=True).start()
    if any(campaign.get("poll_username") for campaign in campaigns):
        threading.Thread(target=poll_worker, args=(os.environ["IG_ACCOUNT_ID"], campaigns), daemon=True).start()
    server.serve_forever()
