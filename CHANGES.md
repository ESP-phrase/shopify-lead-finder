# Lead Finder — what we've shipped

A web tool that finds X (Twitter) handles whose bios contain active Shopify
stores. For cold outreach.

## How it works
1. Dorks search engines for `site:x.com "myshopify.com" <keyword>` etc.
2. Extracts X handle + any Shopify URL from the result snippet
3. Hits each store's HTML to confirm it's actually Shopify
4. Hits `/products.json` to confirm the store has products and is active
5. Categorizes each lead: **active** vs **dormant**, **newb** (.myshopify.com)
   vs **established** (custom domain)
6. Surfaces it all in a dashboard with one-click `@handle` copy + CSV export

## Major fixes / features

### Lead targeting
- **Bio dork via DuckDuckGo** — no X login required, no ToS violation, no account-suspension risk (vs. the original snscrape / twscrape plan, which both required X credentials)
- **Multiple dork variants per keyword** — 3 templates (× 2 site domains) ran for every keyword instead of 1
- **Broad mode** — adds Shopify-ecosystem dorks (Klaviyo, Recharge, Postscript, Shopify Plus, etc.) that surface custom-domain Shopify stores in addition to .myshopify.com newbs
- **Junk-host filter** — strips t.co, linktr.ee, bit.ly, social media links, search engines from candidate URL extraction

### Active-store verification
- **`/products.json` probe** — only counts a store as active if Shopify's public products endpoint returns real data
- **Price filter** — rejects catalogs with zero-priced placeholder products (test stores)
- **Recency filter** — rejects stores whose last product update is over 365 days old (abandoned stores)
- **Reason strings** — every dormant lead shows *why*: `password-gated`, `no /products.json`, `stale (832d since update)`, `no products`, etc.

### Reliability
- **DataImpulse residential proxy** wired through DDGS — fixes the rate-limiting that was killing searches before. URL-encoded creds stored in `.env`, auto-loaded on import.
- **HTTPS support** through the same proxy via CONNECT tunneling
- **Retry-friendly logging** — every dork run produces a per-query log line: `[3/6] 22 raw, +5 new candidates | site:x.com "myshopify.com" dropshipping`. Visible in UI under a collapsible "Search log" panel.

### Dashboard UX
- **Dark glass UI** — Inter variable font, purple→cyan gradient accents, ambient radial glows, custom card layout
- **Live proxy indicator** — green pulse dot top-right when DataImpulse is active
- **Stat tiles** — Active leads / Newbs / Established / Search time
- **Filter pills with matching counts** — Active / Active newbs / Active est. / Dormant / All shopify. Counts and click-filtered views are now consistent (was a bug)
- **Search-within-results** input — filter handles/bios live without re-running the search
- **One-click copy** of `@handle` per lead, plus "Copy all visible @handles" for bulk paste into outreach tools
- **CSV export** of verified Shopify rows
- **Loading overlay** with conic-gradient spinner that animates during the search instead of leaving the page silent
- **Empty state** with hint copy when zero matches
- **Hides non-Shopify hits by default** — false positives like `skool.com`, `ycombinator.com` no longer clutter results (toggle to show)

## Components on the page

- **Bio dork tab (`/dork`)** — the main tool. No setup.
- **Domain checker tab (`/`)** — bulk-verify a list of domains for Shopify
- **X login tab (`/x`)** — twscrape-based fallback that needs X account creds. Built but not currently in active use.

## Files

- `app.py` — Flask UI
- `bio_dork.py` — search → extract → verify pipeline
- `scraper.py` — Shopify HTML/header detector and threaded URL-list checker
- `x_scraper.py` — twscrape pipeline (logged-in X search), optional
- `.env` — proxy credentials (DataImpulse)
- `accounts.txt` — throwaway X creds for the `/x` tab, optional
