"""
X bio -> Shopify lead pipeline via search-engine dorking.

Finds X (Twitter) profiles whose bios contain Shopify store URLs, verifies
each URL is a real Shopify store, and confirms the store is actively
selling (has products + not password-gated).

No X login required. Works against custom-domain stores too via broad mode.
"""

import argparse
import csv
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlparse

import requests
from ddgs import DDGS

from scraper import SESSION, TIMEOUT, check


def _load_env() -> None:
    env_file = Path(__file__).with_name(".env")
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


_load_env()
PROXY_URL = os.environ.get("PROXY_URL")
BRAVE_API_KEY = os.environ.get("BRAVE_API_KEY")
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GOOGLE_CSE_ID = os.environ.get("GOOGLE_CSE_ID")

if PROXY_URL:
    SESSION.proxies = {"http": PROXY_URL, "https": PROXY_URL}


_BRAVE_LAST_REQ = 0.0


def _brave_search(query: str, count: int) -> list[dict]:
    """Brave Search API. Returns rows shaped like DDGS: {href, body, title}.

    Free tier: 1 req/sec, max 20 results/query, no offset pagination.
    """
    global _BRAVE_LAST_REQ
    if not BRAVE_API_KEY:
        raise RuntimeError("BRAVE_API_KEY not set in .env")
    query = query.replace('"', "")
    n = min(20, max(1, count))

    # Throttle: free tier is 1 req/sec
    elapsed = time.time() - _BRAVE_LAST_REQ
    if elapsed < 1.05:
        time.sleep(1.05 - elapsed)

    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "X-Subscription-Token": BRAVE_API_KEY,
    }
    params = {"q": query, "count": n, "safesearch": "off"}
    try:
        r = requests.get(
            "https://api.search.brave.com/res/v1/web/search",
            params=params, headers=headers, timeout=20,
        )
    except requests.RequestException as e:
        raise RuntimeError(f"brave http error: {e}") from None
    finally:
        _BRAVE_LAST_REQ = time.time()

    if r.status_code == 429:
        time.sleep(2)
        return []  # back off, return empty rather than fail
    if r.status_code == 422:
        return []  # Brave rejects some queries (rate or content) — swallow
    if not r.ok:
        raise RuntimeError(f"brave {r.status_code}: {r.text[:120]}")
    data = r.json()
    results = (data.get("web") or {}).get("results") or []
    return [{
        "href": it.get("url", ""),
        "title": it.get("title", "") or "",
        "body": it.get("description", "") or "",
    } for it in results]


def _ddg_search(query: str, count: int, ddgs: DDGS) -> list[dict]:
    return list(ddgs.text(query, max_results=count))


def _google_search(query: str, count: int) -> list[dict]:
    """Google Custom Search API. Free 100 queries/day. 10 results per request."""
    if not (GOOGLE_API_KEY and GOOGLE_CSE_ID):
        raise RuntimeError("GOOGLE_API_KEY and GOOGLE_CSE_ID required in .env")
    out: list[dict] = []
    fetched = 0
    start = 1  # Google uses 1-indexed `start` param
    while fetched < count and start <= 91:  # CSE caps at 100 results total
        n = min(10, count - fetched)
        params = {
            "key": GOOGLE_API_KEY, "cx": GOOGLE_CSE_ID,
            "q": query, "num": n, "start": start, "safe": "off",
        }
        try:
            r = requests.get(
                "https://www.googleapis.com/customsearch/v1",
                params=params, timeout=20,
            )
        except requests.RequestException as e:
            raise RuntimeError(f"google http error: {e}") from None
        if r.status_code == 429:
            time.sleep(2)
            return out
        if r.status_code == 403:
            # Quota exhausted or API not enabled
            raise RuntimeError(f"google 403: {r.text[:120]}")
        if not r.ok:
            raise RuntimeError(f"google {r.status_code}: {r.text[:120]}")
        data = r.json()
        items = data.get("items") or []
        if not items:
            break
        for it in items:
            out.append({
                "href": it.get("link", ""),
                "title": it.get("title", "") or "",
                "body": it.get("snippet", "") or "",
            })
        fetched += len(items)
        if len(items) < n:
            break
        start += n
    return out

SHOPIFY_BIO_RE = re.compile(r"\b([a-z0-9][a-z0-9-]{1,59}\.myshopify\.com)\b", re.I)
URL_RE = re.compile(r"https?://[^\s)\]>\"']+", re.I)
X_PROFILE_RE = re.compile(
    r"^https?://(?:x|twitter)\.com/([A-Za-z0-9_]{1,15})(?:[/?#]|$)"
)
X_RESERVED = {
    "home", "explore", "i", "search", "notifications", "messages",
    "settings", "compose", "intent", "share", "hashtag", "login",
    "signup", "tos", "privacy", "about",
}
JUNK_HOSTS = {
    "t.co", "twitter.com", "x.com", "youtu.be", "youtube.com",
    "instagram.com", "tiktok.com", "facebook.com", "pinterest.com",
    "duckduckgo.com", "google.com", "bing.com", "bit.ly", "linkedin.com",
    "reddit.com", "github.com", "linktr.ee", "shopify.com", "amazon.com",
    "ebay.com", "etsy.com", "spotify.com", "apple.com", "wikipedia.org",
}


SHOPIFY_ECOSYSTEM_TERMS = [
    "klaviyo",
    "recharge",
    "postscript",
    "shopify plus",
    "shopify partner",
    "powered by shopify",
]

# Suffixes mixed in to diversify queries — survive Brave's quote-stripping.
DORK_SUFFIXES = ["", "store", "shop", "brand", "founder", "merch", "apparel"]


def dork_queries(keywords: list[str], broad: bool = False) -> list[str]:
    """
    Build the dork list. Diversified so each variant is unique even after
    Brave strips quoted phrases. Without `broad`, every query pins
    myshopify.com. With `broad`, also pulls in Shopify-ecosystem terms
    that surface custom-domain stores.
    """
    queries: list[str] = []
    targets = ["x.com", "twitter.com"]

    def add(t: str, body: str) -> None:
        queries.append(f"site:{t} {body}")

    if not keywords:
        for t in targets:
            for suf in DORK_SUFFIXES:
                add(t, f'"myshopify.com" {suf}'.strip())
            if broad:
                for term in SHOPIFY_ECOSYSTEM_TERMS:
                    add(t, f'"{term}"')
                    add(t, f'"{term}" shop')
        return queries

    for kw in keywords:
        for t in targets:
            for suf in DORK_SUFFIXES:
                add(t, f'"myshopify.com" {kw} {suf}'.strip())
            add(t, f'".myshopify.com" {kw}')
            add(t, f'{kw} myshopify.com')
            if broad:
                for term in SHOPIFY_ECOSYSTEM_TERMS:
                    add(t, f'"{term}" {kw}')
                    add(t, f'{term} {kw} store')
    return queries


def parse_username(url: str) -> str | None:
    m = X_PROFILE_RE.match(url)
    if not m:
        return None
    handle = m.group(1).lower()
    if handle in X_RESERVED:
        return None
    return handle


def is_junk_host(host: str) -> bool:
    host = host.lower()
    if host in JUNK_HOSTS:
        return True
    return any(host.endswith("." + d) for d in JUNK_HOSTS)


def extract_candidate_urls(text: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for m in SHOPIFY_BIO_RE.finditer(text or ""):
        u = f"https://{m.group(1).lower()}"
        if u not in seen:
            seen.add(u)
            urls.append(u)
    for raw in URL_RE.findall(text or ""):
        url = raw.rstrip(".,!?)\"'>")
        host = urlparse(url).netloc.lower()
        if not host or is_junk_host(host):
            continue
        if "/status/" in url or "/photo/" in url:
            continue
        if url not in seen:
            seen.add(url)
            urls.append(url)
    return urls


def search_dorks(
    queries: list[str],
    per_query: int,
    log: list[str] | None = None,
    engine: str = "ddg",
) -> dict[str, dict]:
    """Run each dork; dedupe by X username; return {username: row}.

    engine: "ddg", "brave", or "both" (runs DDG and Brave per query, merges).
    """
    def emit(msg: str) -> None:
        print(msg, file=sys.stderr)
        if log is not None:
            log.append(msg)

    by_user: dict[str, dict] = {}

    use_ddg = engine in ("ddg", "both", "all")
    use_brave = engine in ("brave", "both", "all")
    use_google = engine in ("google", "all")
    if use_brave and not BRAVE_API_KEY:
        emit("BRAVE_API_KEY missing — Brave disabled.")
        use_brave = False
    if use_google and not (GOOGLE_API_KEY and GOOGLE_CSE_ID):
        emit("GOOGLE_API_KEY/GOOGLE_CSE_ID missing — Google disabled.")
        use_google = False
    if not (use_ddg or use_brave or use_google):
        emit("No engine usable; defaulting to DDG.")
        use_ddg = True

    proxy_status = "via proxy" if PROXY_URL else "direct"
    enabled = []
    if use_ddg: enabled.append("ddg")
    if use_brave: enabled.append("brave")
    if use_google: enabled.append("google")
    emit(f"Search starting [{'+'.join(enabled)}, {proxy_status}], {len(queries)} queries × {per_query} results")

    ddgs = DDGS(proxy=PROXY_URL, timeout=20) if use_ddg else None

    def absorb(hits: list[dict]) -> int:
        before = len(by_user)
        for r in hits:
            href = r.get("href") or ""
            username = parse_username(href)
            if not username or username in by_user:
                continue
            snippet = (r.get("body") or "").strip()
            title = (r.get("title") or "").strip()
            candidates = extract_candidate_urls(f"{title} {snippet}")
            if not candidates:
                continue
            by_user[username] = {
                "username": username,
                "x_profile": f"https://x.com/{username}",
                "bio_snippet": snippet,
                "candidates": candidates,
            }
        return len(by_user) - before

    try:
        brave_zero_streak = 0
        for i, q in enumerate(queries, 1):
            tag = f"[{i}/{len(queries)}]"
            ddg_n = brave_n = 0
            ddg_added = brave_added = 0
            if use_ddg:
                try:
                    hits = list(ddgs.text(q, max_results=per_query))
                    ddg_n = len(hits)
                    ddg_added = absorb(hits)
                except Exception as e:
                    emit(f"{tag} DDG ERROR  {q}  ->  {e}")

            # Skip Brave for site:x.com (Brave 422s those) — only run on twitter.com queries
            brave_skip_reason = None
            if use_brave:
                if "site:x.com" in q:
                    brave_skip_reason = "(skipped: Brave 422s site:x.com)"
                elif brave_zero_streak >= 5:
                    brave_skip_reason = "(skipped: Brave +0 streak)"
                else:
                    try:
                        hits = _brave_search(q, per_query)
                        brave_n = len(hits)
                        brave_added = absorb(hits)
                        brave_zero_streak = 0 if brave_added > 0 else brave_zero_streak + 1
                    except Exception as e:
                        emit(f"{tag} BRAVE ERROR  {q}  ->  {e}")

            google_n = google_added = 0
            if use_google:
                try:
                    hits = _google_search(q, per_query)
                    google_n = len(hits)
                    google_added = absorb(hits)
                except Exception as e:
                    emit(f"{tag} GOOGLE ERROR  {q}  ->  {e}")

            parts = []
            if use_ddg:    parts.append(f"DDG {ddg_n:3d}r/+{ddg_added}")
            if use_brave:
                if brave_skip_reason:
                    parts.append(f"Brave {brave_skip_reason}")
                else:
                    parts.append(f"Brave {brave_n:3d}r/+{brave_added}")
            if use_google: parts.append(f"Google {google_n:3d}r/+{google_added}")
            emit(f"{tag} {' | '.join(parts)}  |  {q}")
    finally:
        if ddgs is not None:
            ddgs.__exit__(None, None, None)

    emit(f"Done. {len(by_user)} unique X profiles with at least one candidate URL.")
    return by_user


def check_active(url: str) -> tuple[bool, str]:
    """Probe Shopify's public /products.json. Returns (active, reason).

    "Active" requires: 200 OK + JSON + at least one product with a price > 0
    + updated within the last ~12 months. Stale stores or zero-priced
    placeholder catalogs are flagged as inactive.
    """
    from datetime import datetime, timezone

    probe = url.rstrip("/") + "/products.json?limit=10"
    try:
        r = SESSION.get(probe, timeout=TIMEOUT, allow_redirects=True)
    except Exception:
        return False, "unreachable"
    if r.status_code in (401, 402, 403):
        return False, "password-gated"
    if r.status_code == 404:
        return False, "no /products.json"
    if r.status_code != 200:
        return False, f"http {r.status_code}"
    ctype = r.headers.get("content-type", "")
    if "json" not in ctype:
        return False, "non-json"
    try:
        data = r.json()
    except ValueError:
        return False, "bad json"
    products = data.get("products") if isinstance(data, dict) else None
    if not products:
        return False, "no products"

    has_price = False
    latest: datetime | None = None
    for p in products:
        for v in p.get("variants", []) or []:
            try:
                if float(v.get("price") or 0) > 0:
                    has_price = True
                    break
            except (TypeError, ValueError):
                pass
        ts = p.get("updated_at") or p.get("created_at")
        if ts:
            try:
                d = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                if latest is None or d > latest:
                    latest = d
            except ValueError:
                pass

    if not has_price:
        return False, f"{len(products)} products, no prices"

    if latest:
        days = max(0, (datetime.now(timezone.utc) - latest).days)
        if days > 365:
            return False, f"stale ({days}d since update)"
        when = "today" if days == 0 else f"{days}d ago"
        return True, f"{len(products)}+ products, updated {when}"
    return True, f"{len(products)}+ products"


def verify_one(profile: dict) -> dict:
    """Verify each candidate URL; pick the first active Shopify store."""
    chosen: dict | None = None
    fallback: dict | None = None
    for url in profile["candidates"]:
        det = check(url)
        if det is None or not det["shopify"]:
            continue
        active, reason = check_active(det["url"])
        record = {
            "shopify_url": det["url"],
            "signals": det["signals"],
            "shopify": True,
            "active": active,
            "active_reason": reason,
        }
        if active:
            chosen = record
            break
        if fallback is None:
            fallback = record
    result = chosen or fallback or {
        "shopify_url": profile["candidates"][0],
        "signals": "",
        "shopify": False,
        "active": False,
        "active_reason": "not shopify",
    }
    is_newb = ".myshopify.com" in result["shopify_url"].lower()
    return {
        "username": profile["username"],
        "x_profile": profile["x_profile"],
        "bio_snippet": profile["bio_snippet"],
        **result,
        "category": "newb" if is_newb else "established",
    }


def verify_all(profiles: list[dict], workers: int) -> list[dict]:
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(verify_one, profiles))


def main() -> int:
    p = argparse.ArgumentParser(prog="bio_dork")
    p.add_argument("-k", "--keywords", nargs="*", default=[])
    p.add_argument("-n", "--per-query", type=int, default=50)
    p.add_argument("-o", "--output", default="x_bio_leads.csv")
    p.add_argument("-w", "--workers", type=int, default=64)
    p.add_argument(
        "--broad", action="store_true",
        help="Also include custom-domain Shopify stores (lossier).",
    )
    p.add_argument("--no-verify", action="store_true")
    args = p.parse_args()

    queries = dork_queries(args.keywords, broad=args.broad)
    print(f"Running {len(queries)} dork queries...", file=sys.stderr)
    by_user = search_dorks(queries, args.per_query)
    print(f"Found {len(by_user)} candidate X profiles", file=sys.stderr)

    rows = list(by_user.values())
    if not args.no_verify and rows:
        print("Verifying Shopify + active status...", file=sys.stderr)
        rows = verify_all(rows, args.workers)
        rows.sort(
            key=lambda r: (
                not (r["shopify"] and r["active"]),
                not r["shopify"],
                r["username"],
            )
        )

    fields = ["username", "x_profile", "bio_snippet", "shopify_url",
              "shopify", "active", "active_reason", "category", "signals"]
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    active = sum(1 for r in rows if r.get("active"))
    shop = sum(1 for r in rows if r.get("shopify"))
    newbs = sum(1 for r in rows if r.get("active") and r.get("category") == "newb")
    est = sum(1 for r in rows if r.get("active") and r.get("category") == "established")
    print(
        f"\n{active} active / {shop} Shopify / {len(rows)} candidates  "
        f"(newbs: {newbs}, established: {est}). Wrote {args.output}.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
