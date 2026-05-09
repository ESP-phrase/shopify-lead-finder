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
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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


# Proxy pool — load 500 sticky-session endpoints from proxies.txt
def _load_proxy_pool() -> list[str]:
    f = Path(__file__).with_name("proxies.txt")
    if not f.exists():
        return [PROXY_URL] if PROXY_URL else []
    out = []
    from urllib.parse import quote
    for line in f.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Encode the username portion (which has ; , : characters in the geo string)
        # Format: user:pass@host:port
        if "@" in line and "://" not in line:
            creds, host = line.rsplit("@", 1)
            user, pw = creds.split(":", 1) if ":" in creds else (creds, "")
            # Re-encode username for safe URL form
            url = f"http://{quote(user, safe='')}:{quote(pw, safe='')}@{host}"
            out.append(url)
        else:
            out.append(line if line.startswith("http") else f"http://{line}")
    return out


import random as _random
_full_pool = _load_proxy_pool()
try:
    _pool_size = int(os.environ.get("PROXY_POOL_SIZE", "0"))
except ValueError:
    _pool_size = 0
if _pool_size > 0 and len(_full_pool) > _pool_size:
    PROXY_POOL = _random.sample(_full_pool, _pool_size)
else:
    PROXY_POOL = _full_pool


def get_random_proxy() -> str | None:
    """Pick a random proxy from the pool. Falls back to PROXY_URL or None."""
    if PROXY_POOL:
        return _random.choice(PROXY_POOL)
    return PROXY_URL


def get_proxies_dict() -> dict | None:
    p = get_random_proxy()
    return {"http": p, "https": p} if p else None

if PROXY_POOL or PROXY_URL:
    # Patch SESSION.request to pick a fresh proxy per call
    _orig_request = SESSION.request

    def _request_with_pool(method, url, **kwargs):
        if "proxies" not in kwargs:
            kwargs["proxies"] = get_proxies_dict()
        return _orig_request(method, url, **kwargs)

    SESSION.request = _request_with_pool
    print(f"[proxy] pool loaded: {len(PROXY_POOL)} sticky-session endpoints", file=sys.stderr)


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
            proxies=get_proxies_dict(),
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
    """Scrape Google search results directly through the residential proxy.

    No API key required — uses DataImpulse (or whatever PROXY_URL is set to)
    to dodge bot detection. Parses HTML SERP into row dicts that match the
    DDGS shape: {href, title, body}.
    """
    from bs4 import BeautifulSoup

    proxies = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }

    out: list[dict] = []
    seen: set[str] = set()
    n = min(20, max(10, count))  # Google ignores num > ~20-30 anyway

    try:
        r = requests.get(
            "https://www.google.com/search",
            params={"q": query, "num": n, "hl": "en", "gl": "us"},
            headers=headers, proxies=proxies, timeout=25,
        )
    except requests.RequestException as e:
        raise RuntimeError(f"google scrape http error: {e}") from None

    if r.status_code == 429 or "captcha" in r.text.lower()[:2000]:
        raise RuntimeError("google blocked us (CAPTCHA / 429) — try again or rotate proxy IP")
    if not r.ok:
        raise RuntimeError(f"google scrape {r.status_code}")

    soup = BeautifulSoup(r.text, "html.parser")
    # Google's SERP layout has changed many times; try several selectors.
    for block in soup.select("div.MjjYud, div.tF2Cxc, div.g"):
        a = block.find("a", href=True)
        if not a:
            continue
        href = a["href"]
        if not href.startswith("http"):
            continue
        # Strip Google redirect wrappers
        if "/url?" in href and "url=" in href:
            from urllib.parse import parse_qs, urlparse as _up
            q = parse_qs(_up(href).query).get("url") or parse_qs(_up(href).query).get("q")
            if q:
                href = q[0]
        if href in seen:
            continue
        seen.add(href)
        h3 = block.find("h3")
        title = (h3.get_text(strip=True) if h3 else "").strip()
        # Snippet: the largest div that's not the title
        snippet_el = block.select_one("div[data-sncf], div[role=text], span[role=text], span.aCOpRe, div.VwiC3b")
        body = (snippet_el.get_text(" ", strip=True) if snippet_el else "").strip()
        if not body:
            # fallback: any text in the block minus title
            text = block.get_text(" ", strip=True)
            body = text.replace(title, "", 1).strip()
        out.append({"href": href, "title": title, "body": body})
        if len(out) >= count:
            break
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

# Keyword packs — preset niche/category lists for one-click broad runs.
KEYWORD_PACKS: dict[str, list[str]] = {
    "ecom_general": [
        "ecommerce", "dtc", "online store", "my shop", "founder",
        "shopify store", "brand", "merch", "apparel",
    ],
    "dropshipping": [
        "dropshipping", "dropshipper", "dropship", "winning products",
        "aliexpress", "cj dropshipping", "spocket",
    ],
    "niches": [
        "skincare", "fitness", "coffee", "tea", "candles", "jewelry",
        "art prints", "pet supplies", "home decor", "fashion", "sneakers",
    ],
    "creators_pod": [
        "print on demand", "merch", "stickers", "enamel pin",
        "artist", "illustrator", "designer", "creator",
    ],
}


def expand_pack(name: str) -> list[str]:
    return list(KEYWORD_PACKS.get(name, []))


def dork_queries(keywords: list[str], broad: bool = False) -> list[str]:
    """Build the dork list, deduped and minus nonsensical suffix combos."""
    seen: set[str] = set()
    queries: list[str] = []
    targets = ["x.com", "twitter.com"]

    def add(t: str, body: str) -> None:
        q = f"site:{t} {body.strip()}"
        if q not in seen:
            seen.add(q)
            queries.append(q)

    def smart_suffixes(kw: str) -> list[str]:
        kw_l = kw.lower()
        return [s for s in DORK_SUFFIXES if not s or s not in kw_l]

    if not keywords:
        for t in targets:
            for suf in DORK_SUFFIXES:
                add(t, f'"myshopify.com" {suf}')
            if broad:
                for term in SHOPIFY_ECOSYSTEM_TERMS:
                    add(t, f'"{term}"')
                    add(t, f'"{term}" shop')
        return queries

    for kw in keywords:
        sufs = smart_suffixes(kw)
        for t in targets:
            for suf in sufs:
                add(t, f'"myshopify.com" {kw} {suf}')
            add(t, f'".myshopify.com" {kw}')
            add(t, f'{kw} myshopify.com')
            if broad:
                for term in SHOPIFY_ECOSYSTEM_TERMS:
                    add(t, f'"{term}" {kw}')
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
    on_log=None,
    on_candidate=None,
    parallel: int = 8,
) -> dict[str, dict]:
    """Run each dork; dedupe by X username; return {username: row}.

    engine: "ddg", "brave", or "both"
    on_log: optional callable(msg) for live streaming
    on_candidate: optional callable(profile_dict) — fires once per new
                  unique X profile discovered, before verification.
    parallel: number of queries to run concurrently (DDG only — Brave has
              a global 1 req/sec throttle that auto-serializes).
    """
    def emit(msg: str) -> None:
        print(msg, file=sys.stderr)
        if log is not None:
            log.append(msg)
        if on_log is not None:
            try:
                on_log(msg)
            except Exception:
                pass

    by_user: dict[str, dict] = {}

    use_ddg = engine in ("ddg", "both", "all")
    use_brave = engine in ("brave", "both", "all")
    use_google = engine in ("google", "all")
    if use_brave and not BRAVE_API_KEY:
        emit("BRAVE_API_KEY missing — Brave disabled.")
        use_brave = False
    if use_google and not PROXY_URL:
        emit("PROXY_URL missing — Google scraping needs a proxy to dodge CAPTCHAs. Disabled.")
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

    # Lock so absorb() isn't called concurrently from parallel query workers
    absorb_lock = threading.Lock()

    def absorb(hits: list[dict]) -> int:
        with absorb_lock:
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
                profile = {
                    "username": username,
                    "x_profile": f"https://x.com/{username}",
                    "bio_snippet": snippet,
                    "candidates": candidates,
                }
                by_user[username] = profile
                if on_candidate is not None:
                    try:
                        on_candidate(profile)
                    except Exception:
                        pass
            return len(by_user) - before

    brave_streak = {"n": 0}
    streak_lock = threading.Lock()
    completed = {"n": 0}
    completed_lock = threading.Lock()

    def run_one_query(q: str) -> str:
        ddg_n = brave_n = google_n = 0
        ddg_added = brave_added = google_added = 0
        brave_skipped = False

        if use_ddg:
            try:
                # Each query thread gets its own DDGS with a random pool proxy
                with DDGS(proxy=get_random_proxy(), timeout=20) as ddgs_local:
                    hits = list(ddgs_local.text(q, max_results=per_query))
                ddg_n = len(hits)
                ddg_added = absorb(hits)
            except Exception:
                pass

        if use_brave:
            with streak_lock:
                streak = brave_streak["n"]
            if streak >= 8:
                brave_skipped = True
            else:
                try:
                    hits = _brave_search(q, per_query)
                    brave_n = len(hits)
                    brave_added = absorb(hits)
                    with streak_lock:
                        brave_streak["n"] = 0 if brave_added > 0 else brave_streak["n"] + 1
                except Exception:
                    pass

        if use_google:
            try:
                hits = _google_search(q, per_query)
                google_n = len(hits)
                google_added = absorb(hits)
            except Exception:
                pass

        parts = []
        if use_ddg:    parts.append(f"DDG {ddg_n:3d}r/+{ddg_added}")
        if use_brave:
            parts.append("Brave (skip)" if brave_skipped else f"Brave {brave_n:3d}r/+{brave_added}")
        if use_google: parts.append(f"Google {google_n:3d}r/+{google_added}")

        with completed_lock:
            completed["n"] += 1
            tag = f"[{completed['n']}/{len(queries)}]"
        return f"{tag} {' | '.join(parts)}  |  {q}"

    # Parallel queries — major speedup vs sequential. Brave's global rate
    # limit serializes itself via _BRAVE_LAST_REQ inside _brave_search.
    with ThreadPoolExecutor(max_workers=parallel) as pool:
        futures = [pool.submit(run_one_query, q) for q in queries]
        for fut in as_completed(futures):
            try:
                emit(fut.result())
            except Exception as e:
                emit(f"  query thread error: {e}")

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
