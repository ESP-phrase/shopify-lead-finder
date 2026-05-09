"""
X (Twitter) -> Shopify lead pipeline.

Searches X profiles by keyword via twscrape, extracts the bio website link,
then runs each website through the Shopify detector.

Setup:
  1. Copy accounts.txt.example to accounts.txt and add throwaway X creds
  2. python x_scraper.py -k klaviyo "dtc founder"

Output: x_shopify_leads.csv with username, bio, followers, website, signals.
"""

import argparse
import asyncio
import csv
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from twscrape import API, AccountsPool

from scraper import check

ACCOUNTS_FILE = Path("accounts.txt")
DB_FILE = "twscrape.db"


async def build_pool() -> AccountsPool:
    pool = AccountsPool(DB_FILE)
    if not ACCOUNTS_FILE.exists():
        print(
            f"Missing {ACCOUNTS_FILE}. Copy accounts.txt.example and add creds.",
            file=sys.stderr,
        )
        sys.exit(1)

    existing = {a.username for a in await pool.get_all()}
    added = 0
    for line in ACCOUNTS_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 4:
            print(f"Skipping malformed line: {line[:30]}...", file=sys.stderr)
            continue
        user, pw, mail, mail_pw = parts[:4]
        if user in existing:
            continue
        await pool.add_account(user, pw, mail, mail_pw)
        added += 1

    if added:
        print(f"Added {added} new accounts. Logging in...", file=sys.stderr)
    await pool.login_all()
    return pool


def website_from_user(user) -> str | None:
    """Pick the most plausible website from a User's bio links."""
    if not user.descriptionLinks:
        return None
    for link in user.descriptionLinks:
        if link.url and not link.url.startswith("https://t.co/"):
            return link.url
    return user.descriptionLinks[0].url or None


async def search_profiles(
    keywords: list[str], per_keyword: int
) -> list[dict]:
    pool = await build_pool()
    api = API(pool)
    seen: set[str] = set()
    profiles: list[dict] = []

    for kw in keywords:
        print(f"\nSearching X: {kw!r}", file=sys.stderr)
        count = 0
        try:
            async for user in api.search_user(kw):
                if count >= per_keyword:
                    break
                if user.username in seen:
                    continue
                seen.add(user.username)
                website = website_from_user(user)
                if not website:
                    continue
                profiles.append({
                    "username": user.username,
                    "displayname": user.displayname or "",
                    "bio": (user.rawDescription or "").replace("\n", " ").strip(),
                    "followers": user.followersCount,
                    "verified": bool(user.verified or user.blue),
                    "location": user.location or "",
                    "website": website,
                })
                count += 1
                print(
                    f"  @{user.username:24}  {website}",
                    file=sys.stderr,
                )
        except Exception as e:
            print(f"  Error on {kw!r}: {e}", file=sys.stderr)

    return profiles


def detect_all(profiles: list[dict], workers: int) -> list[dict]:
    """Run the Shopify detector on each profile's website."""
    rows: list[dict] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_to_profile = {
            pool.submit(check, p["website"]): p for p in profiles
        }
        for fut in future_to_profile:
            profile = future_to_profile[fut]
            det = fut.result()
            if det is None:
                continue
            rows.append({
                **profile,
                "resolved_url": det["url"],
                "shopify": det["shopify"],
                "signals": det["signals"],
            })
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(prog="x_scraper")
    parser.add_argument(
        "-k", "--keywords", nargs="+", required=True,
        help="Search keywords (each becomes a separate X search).",
    )
    parser.add_argument(
        "-n", "--per-keyword", type=int, default=50,
        help="Max profiles per keyword (default: 50).",
    )
    parser.add_argument(
        "-o", "--output", default="x_shopify_leads.csv",
        help="CSV output path (default: x_shopify_leads.csv).",
    )
    parser.add_argument(
        "-w", "--workers", type=int, default=64,
        help="Concurrent Shopify detector workers (default: 64).",
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Write all profiles to CSV, not just Shopify hits.",
    )
    args = parser.parse_args()

    profiles = asyncio.run(search_profiles(args.keywords, args.per_keyword))
    print(
        f"\nFound {len(profiles)} profiles with bio websites. "
        f"Detecting Shopify...",
        file=sys.stderr,
    )

    rows = detect_all(profiles, args.workers)
    hits = [r for r in rows if r["shopify"]]
    output = rows if args.all else hits

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "username", "displayname", "bio", "followers", "verified",
            "location", "website", "resolved_url", "shopify", "signals",
        ])
        writer.writeheader()
        writer.writerows(output)

    print(
        f"\n{len(hits)}/{len(rows)} Shopify hits. Wrote {args.output}.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
