"""
Shopify store detector.

Reads domains (one per line) from a file or stdin, checks each for
Shopify fingerprints, writes hits to a CSV.

The original spec used snscrape to pull bios off X, but snscrape stopped
working when X killed unauthenticated endpoints in 2023. Source your
domain list from Apollo, Clay, StoreLeads, or BuiltWith.
"""

import argparse
import csv
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter

MAX_BODY_BYTES = 200_000
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})
TIMEOUT: tuple[float, float] = (3, 5)


def normalize(url: str) -> str | None:
    url = url.strip()
    if not url:
        return None
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    parsed = urlparse(url)
    if not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}"


def detect_shopify(url: str) -> tuple[bool, list[str]]:
    """Return (is_shopify, list_of_signals_matched)."""
    signals = []
    try:
        r = SESSION.get(url, timeout=TIMEOUT, allow_redirects=True, stream=True)
    except requests.RequestException:
        return False, []

    headers = {k.lower(): v for k, v in r.headers.items()}
    if "x-shopid" in headers or "x-shopify-stage" in headers:
        signals.append("shopify-header")
    if "shopify" in headers.get("powered-by", "").lower():
        signals.append("powered-by")

    try:
        raw = r.raw.read(MAX_BODY_BYTES, decode_content=True) or b""
    except Exception:
        raw = b""
    finally:
        r.close()

    body = raw.decode("utf-8", errors="ignore").lower()
    if "cdn.shopify.com" in body:
        signals.append("cdn")
    if "shopify.theme" in body or "shopify.shop" in body:
        signals.append("shopify-js")
    if 'name="generator" content="shopify"' in body:
        signals.append("generator-meta")
    if ".myshopify.com" in body:
        signals.append("myshopify-domain")

    return bool(signals), signals


def check(url: str) -> dict | None:
    normalized = normalize(url)
    if not normalized:
        return None
    is_shop, signals = detect_shopify(normalized)
    return {
        "url": normalized,
        "shopify": is_shop,
        "signals": ",".join(signals),
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="shopify-scraper",
        description="Detect Shopify stores from a list of domains.",
    )
    p.add_argument(
        "input",
        nargs="?",
        default="domains.txt",
        help="Input file with one domain per line, or '-' for stdin "
        "(default: domains.txt).",
    )
    p.add_argument(
        "-o", "--output", default="shopify_leads.csv",
        help="CSV output path (default: shopify_leads.csv).",
    )
    p.add_argument(
        "-w", "--workers", type=int, default=128,
        help="Concurrent workers (default: 128).",
    )
    p.add_argument(
        "-t", "--timeout", type=float, default=5.0,
        help="Read timeout in seconds (default: 5).",
    )
    p.add_argument(
        "--all", action="store_true",
        help="Write all rows to CSV, not just Shopify hits.",
    )
    p.add_argument(
        "-q", "--quiet", action="store_true",
        help="Only print Shopify hits and the summary.",
    )
    return p.parse_args(argv)


def load_domains(path: str) -> list[str]:
    stream = sys.stdin if path == "-" else open(path, encoding="utf-8")
    try:
        return [
            line.strip() for line in stream
            if line.strip() and not line.lstrip().startswith("#")
        ]
    finally:
        if stream is not sys.stdin:
            stream.close()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])

    global TIMEOUT
    TIMEOUT = (3, args.timeout)
    adapter = HTTPAdapter(pool_connections=args.workers, pool_maxsize=args.workers)
    SESSION.mount("http://", adapter)
    SESSION.mount("https://", adapter)

    try:
        domains = load_domains(args.input)
    except FileNotFoundError:
        print(f"Missing {args.input}. Add one domain per line.", file=sys.stderr)
        return 1

    if not domains:
        print("No domains to check.", file=sys.stderr)
        return 1

    print(
        f"Checking {len(domains)} domains with {args.workers} workers...",
        file=sys.stderr,
    )
    start = time.time()
    results = []

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(check, d): d for d in domains}
        for fut in as_completed(futures):
            row = fut.result()
            if row is None:
                continue
            results.append(row)
            if row["shopify"]:
                print(f"[SHOPIFY] {row['url']}  {row['signals']}", file=sys.stderr)
            elif not args.quiet:
                print(f"          {row['url']}", file=sys.stderr)

    rows_out = results if args.all else [r for r in results if r["shopify"]]
    hits = sum(1 for r in results if r["shopify"])

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["url", "shopify", "signals"])
        writer.writeheader()
        writer.writerows(rows_out)

    elapsed = time.time() - start
    print(
        f"\n{hits}/{len(results)} Shopify hits in {elapsed:.1f}s. "
        f"Wrote {args.output}.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
