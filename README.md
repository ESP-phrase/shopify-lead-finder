# Shopify Lead Finder

Web tool that finds X (Twitter) handles whose bios contain active Shopify
stores — for cold outreach.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env   # then fill in your keys
python app.py
```

Open http://127.0.0.1:5000/dork.

## Required env vars (`.env`)

```
# residential proxy (recommended) — DataImpulse, Smartproxy, etc.
PROXY_URL=http://USER:PASS@gw.example.com:823

# search engines (one or more)
BRAVE_API_KEY=BSA...
GOOGLE_API_KEY=AIza...
GOOGLE_CSE_ID=a1b2c3...
```

DDG works without a key but has shallow X coverage.

## How it works

1. Dorks search engines (`site:x.com "myshopify.com" <kw>`) to find X profiles
2. Extracts handle + Shopify URL from each result snippet
3. Verifies the URL is Shopify (HTML fingerprints)
4. Hits `/products.json` to confirm the store has products + isn't stale
5. Categorizes leads: **active/dormant**, **newb (.myshopify.com)** vs
   **established (custom domain)**

See `CHANGES.md` for detailed feature list.
