import hashlib
import hmac
import json
import os
import re
import sqlite3
import threading
import time
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request, urlopen

DATABASE = os.getenv("BOT_DB", "bot.sqlite3")
DAY = 86400

def database():
    return closing(sqlite3.connect(DATABASE, timeout=15))

def init():
    with database() as db, db:
        db.execute("""CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY, account TEXT NOT NULL, dm TEXT NOT NULL,
            reply TEXT NOT NULL, stage TEXT NOT NULL DEFAULT 'dm',
            due REAL NOT NULL, expires REAL NOT NULL, attempts INTEGER DEFAULT 0)""")
        db.execute("CREATE INDEX IF NOT EXISTS due_jobs ON jobs(stage, due)")
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
                    if media != campaign["media_id"] or campaign["keyword"].casefold() not in words:
                        continue
                    template = json.dumps(dm_template(campaign)) if "button_title" in campaign else None
                    text = campaign["dm"] if template else campaign["dm"] + "\n" + campaign["url"]
                    db.execute("""INSERT OR IGNORE INTO jobs (id,account,dm,reply,due,expires,dm_template)
                        VALUES (?,?,?,?,?,?,?)""", (comment, account, text, campaign["reply"], now,
                        min(float(entry.get("time", now)), now) + 7 * DAY, template))
                    break

def graph(path, body):
    version = os.environ["IG_API_VERSION"]
    login_mode = os.getenv("IG_LOGIN_MODE", "instagram")
    host = "graph.facebook.com" if login_mode == "facebook" else "graph.instagram.com"
    request = Request(f"https://{host}/{version}/{path}",
                      data=json.dumps(body).encode(),
                      headers={"Authorization": "Bearer " + os.environ["IG_TOKEN"],
                               "Content-Type": "application/json"}, method="POST")
    with urlopen(request, timeout=20) as response:
        response.read()

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
                code = json.loads(payload).get("error", {}).get("code")
            except (ValueError, AttributeError):
                code = None
            retry = (error.code == 429 or code in {4, 17, 32, 613}) and attempts < 12
            next_stage = stage if retry else "failed"
            if error.code >= 500 and not retry:
                next_stage = stage + "_pending"
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
        if (not campaign["media_id"].isdigit() or not re.fullmatch(r"\w+", campaign["keyword"])
                or url.scheme != "https" or not url.hostname or not campaign["dm"].strip()
                or not campaign["reply"].strip()):
            raise SystemExit("Invalid campaign configuration")
        if "button_title" in campaign and not 0 < len(campaign["button_title"].strip()) <= 20:
            raise SystemExit("Button title must have 1–20 characters")
    init()
    server = ThreadingHTTPServer(("0.0.0.0", int(os.getenv("PORT", "8080"))), Handler)
    server.campaigns = campaigns
    threading.Thread(target=work, daemon=True).start()
    server.serve_forever()
