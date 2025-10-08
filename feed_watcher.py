#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI Feed Watcher (GitHub Actions-ready)
- RSS/Atomを取得して新着のみ出力（Slack任意）
- 既読は state_dir に小さなJSONで保持（DB不要）
- If-None-Match/If-Modified-Since で効率化
- 環境変数 SLACK_WEBHOOK_URL を優先（Secretsで安全運用）
- cutoff_days: 直近N日以内だけ通知
- max_emit: 1回の通知上限
"""

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional
import logging

try:
    import requests
    import feedparser
    import yaml
except ImportError:
    print("Missing dependencies. Install with: pip install -r requirements.txt", file=sys.stderr)
    raise

# -------------------- Helpers --------------------

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def load_yaml(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def load_json(path: Path, default: Dict[str, Any]) -> Dict[str, Any]:
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            logging.warning("State file %s is corrupted; recreating.", path)
    return default.copy()

def save_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(tmp, path)

def pick_entry_id(entry: Dict[str, Any]) -> str:
    for k in ("id", "guid", "link"):
        if entry.get(k):
            return entry[k]
    key = f"{entry.get('title','')}-{entry.get('published','')}-{entry.get('updated','')}"
    return key

def to_epoch(struct_time) -> float:
    try:
        return time.mktime(struct_time) if struct_time else 0.0
    except Exception:
        return 0.0

def post_to_slack(webhook_url: str, text: str) -> None:
    try:
        resp = requests.post(webhook_url, json={"text": text}, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        logging.error("Slack post failed: %s", e)

# -------------------- Core --------------------

def process_feed(
    feed_url: str,
    state_dir: Path,
    max_seen: int = 50,
    source_tag: Optional[str] = None,
    timeout: int = 20,
    cutoff_days: Optional[int] = None,
    max_emit: int = 50,
) -> List[Dict[str, Any]]:
    """Process one feed and emit new items. Returns new items."""
    tag = source_tag or feed_url
    feed_hash = sha1(feed_url)
    st_path = state_dir / f"{feed_hash}.json"
    state = load_json(st_path, {
        "feed_url": feed_url,
        "etag": None,
        "last_modified": None,
        "last_seen_ids": [],
        "updated_at": utcnow_iso(),
    })

    headers = {}
    if state.get("etag"):
        headers["If-None-Match"] = state["etag"]
    if state.get("last_modified"):
        headers["If-Modified-Since"] = state["last_modified"]

    try:
        r = requests.get(feed_url, headers=headers, timeout=timeout)
    except Exception as e:
        logging.error("Fetch error %s: %s", feed_url, e)
        return []

    if r.status_code == 304:
        logging.info("Not modified: %s", feed_url)
        return []

    new_etag = r.headers.get("ETag") or state.get("etag")
    new_last_modified = r.headers.get("Last-Modified") or state.get("last_modified")

    parsed = feedparser.parse(r.content)
    if parsed.bozo and not parsed.entries:
        logging.warning("Parse warning for %s: %s", feed_url, parsed.bozo_exception)
        return []

    entries = parsed.entries or []

    def entry_sort_key(e):
        return max(to_epoch(e.get("published_parsed")), to_epoch(e.get("updated_parsed")))
    entries.sort(key=entry_sort_key, reverse=True)

    seen = set(state.get("last_seen_ids", []))
    new_items: List[Dict[str, Any]] = []
    cutoff_dt = None
    if cutoff_days:
        cutoff_dt = datetime.now(timezone.utc) - timedelta(days=int(cutoff_days))

    for e in entries:
        # 直近N日以内に限定
        if cutoff_dt is not None:
            ts = e.get("published_parsed") or e.get("updated_parsed")
            if ts:
                entry_dt = datetime.fromtimestamp(time.mktime(ts), tz=timezone.utc)
                if entry_dt < cutoff_dt:
                    continue
            else:
                # 日付不明は静音運用のためスキップ
                continue

        eid = pick_entry_id(e)
        h = sha1(eid)
        if h in seen:
            continue

        new_items.append({
            "id": eid,
            "hash": h,
            "title": e.get("title", "(no title)"),
            "link": e.get("link", ""),
            "published": e.get("published", e.get("updated", "")),
            "source": parsed.feed.get("title", tag),
        })

    emit_list: List[Dict[str, Any]] = []
    if new_items:
        MAX_EMIT = max_emit
        emit_list = new_items[:MAX_EMIT]

        # Console
        for item in emit_list:
            print(f"[{item['source']}] {item['title']}\n{item['link']}\n— {item['published']}\n")

        new_hashes = [it["hash"] for it in emit_list]
        state["last_seen_ids"] = (new_hashes + state.get("last_seen_ids", []))[:max_seen]

    state["etag"] = new_etag
    state["last_modified"] = new_last_modified
    state["updated_at"] = utcnow_iso()
    save_json(st_path, state)
    return emit_list


def run(config_path: Path) -> int:
    run_id = f"run-{int(time.time())}-{os.urandom(2).hex()}"
    logging.info("Starting feed watcher with Run ID: %s", run_id)

    cfg = load_yaml(config_path)
    feeds: List[Dict[str, Any]] = cfg.get("feeds", [])
    state_dir = Path(cfg.get("state_dir", "./state")).expanduser().resolve()
    # Secrets優先（ローカルなら export SLACK_WEBHOOK_URL=... でOK）
    slack_webhook = os.environ.get("SLACK_WEBHOOK_URL") or cfg.get("slack_webhook_url")
    slack_mentions = cfg.get("slack_mentions", "")
    max_seen = int(cfg.get("max_seen", 50))
    timeout = int(cfg.get("timeout", 20))
    cutoff_days = cfg.get("cutoff_days")
    max_emit = int(cfg.get("max_emit", 50))

    if not feeds:
        print("No feeds configured. Edit your config YAML.", file=sys.stderr)
        return 1

    # state_dir を確実に作成
    state_dir.mkdir(parents=True, exist_ok=True)

    all_new_items: List[Dict[str, Any]] = []
    for f in feeds:
        url = f.get("url")
        if not url:
            continue
        tag = f.get("name") or url
        try:
            new_items = process_feed(
                url, state_dir,
                max_seen=max_seen,
                source_tag=tag,
                timeout=timeout,
                cutoff_days=cutoff_days,
                max_emit=max_emit
            )
            if new_items:
                logging.info("[%s] Processed %s -> %d new", run_id, tag, len(new_items))
                all_new_items.extend(new_items)
            else:
                logging.info("[%s] Processed %s -> 0 new", run_id, tag)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logging.exception("[%s] Error processing %s: %s", run_id, tag, e)

    if slack_webhook and all_new_items:
        # Build the full message content first
        header_lines = []
        if slack_mentions:
            header_lines.append(slack_mentions)
        header_lines.append(f"📬 *{len(all_new_items)}* new item(s) from your feeds:")

        item_lines = []
        items_by_source: Dict[str, List] = {}
        for item in all_new_items:
            source = item.get("source", "Unknown Source")
            if source not in items_by_source:
                items_by_source[source] = []
            items_by_source[source].append(item)

        # Sort sources by name for consistent order
        for source in sorted(items_by_source.keys()):
            items = items_by_source[source]
            item_lines.append(f"\n*{source}* ({len(items)}):")
            for item in items[:15]:  # 1ソースあたりの上限
                title = item['title'].replace("&", "&amp;")
                item_lines.append(f"• <{item['link']}|{title}>")

        # Combine and send as a single message
        full_message_lines = header_lines + item_lines
        post_to_slack(slack_webhook, "\n".join(full_message_lines))
        logging.info("[%s] Sent Slack notification for %d items.", run_id, len(all_new_items))

    return 0



def main():
    parser = argparse.ArgumentParser(description="Fetch multiple RSS/Atom feeds and emit new items (console/Slack).")
    parser.add_argument("--config", "-c", required=True, help="Path to YAML config.")
    parser.add_argument("--log", default="WARNING", help="Log level (DEBUG/INFO/WARNING/ERROR).")
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log.upper(), logging.WARNING),
                        format="%(asctime)s %(levelname)s %(message)s")
    sys.exit(run(Path(args.config)))

if __name__ == "__main__":
    main()
