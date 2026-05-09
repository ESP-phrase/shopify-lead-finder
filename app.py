"""ShopifySift — Shopify-store-in-X-bio finder for cold outreach."""

import asyncio
import csv
import io
import json
import os
import queue
import re
import secrets
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from pathlib import Path

from flask import (
    Flask, Response, flash, redirect, render_template_string,
    request, session, stream_with_context, url_for,
)

import db
from scraper import check
from bio_dork import (
    dork_queries, search_dorks, verify_all, PROXY_URL, BRAVE_API_KEY,
    KEYWORD_PACKS,
)

# x_scraper pulls heavy deps (twscrape) — lazy-import inside the /x route
ACCOUNTS_FILE = "accounts.txt"

app = Flask(__name__)

_FLASK_SECRET = os.environ.get("FLASK_SECRET")
if _FLASK_SECRET:
    app.secret_key = _FLASK_SECRET
elif os.environ.get("FLASK_ENV") == "production":
    raise RuntimeError(
        "FLASK_SECRET env var required in production. "
        "Generate with: python -c \"import secrets; print(secrets.token_hex(32))\""
    )
else:
    # Dev only — sessions invalidate on every restart, which is fine locally
    app.secret_key = secrets.token_hex(32)
    print("[warn] Using ephemeral FLASK_SECRET (dev mode)", file=sys.stderr)

db.init_db()

# Stripe — wired but lazy. If STRIPE_SECRET_KEY is set, we use real
# Checkout. Otherwise fallback to test-mode (manual credit add).
import bcrypt
try:
    import stripe
except ImportError:
    stripe = None

STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET")
if stripe and STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY


def hash_password(pw: str) -> str:
    return bcrypt.hashpw(pw.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(pw: str, password_hash: str) -> bool:
    if not password_hash:
        return False
    try:
        return bcrypt.checkpw(pw.encode("utf-8"), password_hash.encode("utf-8"))
    except (ValueError, TypeError):
        return False


EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# Credit plans — Stripe will hook into /checkout/confirm later
PLANS = {
    "starter": {
        "id": "starter", "name": "Starter", "credits": 100, "price": 29,
        "tagline": "Best for testing real outreach",
        "features": ["100 searches", "All engines", "Active-store verification", "CSV export"],
    },
    "pro": {
        "id": "pro", "name": "Pro", "credits": 500, "price": 99,
        "tagline": "For full-time cold-DM operators",
        "features": ["500 searches", "All engines + niche packs", "Saved searches", "Priority support", "Bulk CSV export"],
        "featured": True,
    },
    "power": {
        "id": "power", "name": "Power", "credits": 2000, "price": 249,
        "tagline": "For agencies running ICP-wide sweeps",
        "features": ["2000 searches", "Everything in Pro", "API access (coming soon)", "Dedicated support", "Best ¢/search"],
        "best_value": True,
    },
}


def current_user() -> dict | None:
    uid = session.get("user_id")
    if not uid:
        return None
    return db.get_user(uid)


def require_login(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            return redirect(url_for("login_page", next=request.path))
        # Stale session — user_id set but user gone from DB (e.g. deleted account)
        if db.get_user(session["user_id"]) is None:
            session.clear()
            return redirect(url_for("login_page"))
        return f(*args, **kwargs)
    return wrapper

SHARED_STYLE = r"""
<link rel="icon" type="image/svg+xml" href="/static/favicon.svg">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Geist:wght@400;500;600;700&family=Geist+Mono:wght@400;500&display=swap">
<style>
  :root {
    --bg: #16181c;
    --bg-card: #1f2227;
    --bg-cream: #f5d9c4;
    --bg-cream-alt: #fbe4d2;
    --text: #f5f0eb;
    --text-dark: #1a1612;
    --text2: #a8a39e;
    --muted: #74706c;
    --line: rgba(255,255,255,.07);
    --line-strong: rgba(255,255,255,.14);
    --line-dark: rgba(26,22,18,.14);
    --accent: #ff7a3c;
    --accent-hover: #ff8b4f;
    --accent-soft: rgba(255,122,60,.16);
    --green: #4ade80;
    --red: #f87171;
    color-scheme: dark;
  }
  * { box-sizing: border-box; }
  html, body { margin: 0; }
  body {
    font-family: 'Geist', -apple-system, BlinkMacSystemFont, system-ui, sans-serif;
    background: var(--bg); color: var(--text);
    min-height: 100vh; font-size: 14px; line-height: 1.55;
    -webkit-font-smoothing: antialiased;
    background-image: radial-gradient(700px circle at 100% 0%, rgba(255,122,60,.06), transparent 50%);
  }
  a { color: var(--accent); text-decoration: none; }
  a:hover { text-decoration: underline; }
  nav.top-nav {
    position: sticky; top: 0; z-index: 50;
    background: rgba(22,24,28,.78); backdrop-filter: saturate(140%) blur(14px);
    border-bottom: 1px solid var(--line);
  }
  nav.top-nav .inner {
    display: flex; align-items: center; justify-content: space-between;
    padding: 1rem 1.5rem; max-width: 1180px; margin: 0 auto; height: 64px;
  }
  .brand-mark {
    display: inline-flex; align-items: center; gap: .55rem;
    font-weight: 600; font-size: 15px; color: var(--text);
    letter-spacing: -.01em; text-decoration: none;
  }
  .brand-mark svg { width: 22px; height: 22px; }
  .brand-mark:hover { text-decoration: none; color: var(--text); }
  .nav-r { display: flex; align-items: center; gap: 1.25rem; font-size: 14px; }
  .nav-r a { color: var(--text2); font-weight: 500; text-decoration: none; }
  .nav-r a:hover { color: var(--text); }
  .btn-primary {
    background: var(--accent); color: #fff; padding: .55rem 1.1rem;
    font-weight: 600; border-radius: 8px; font-size: 13px; border: 1px solid var(--accent);
    cursor: pointer; text-decoration: none; display: inline-block;
    transition: all .12s ease;
  }
  .btn-primary:hover { background: var(--accent-hover); border-color: var(--accent-hover); transform: translateY(-1px); text-decoration: none; }
  .btn-ghost {
    background: transparent; color: var(--text2);
    border: 1px solid var(--line-strong); padding: .55rem 1.1rem;
    border-radius: 8px; font-size: 13px; font-weight: 500; cursor: pointer;
    text-decoration: none; display: inline-block;
  }
  .btn-ghost:hover { color: var(--text); border-color: var(--text2); text-decoration: none; }
  .credit-pill {
    background: var(--accent-soft); color: var(--accent);
    border: 1px solid rgba(255,122,60,.28); padding: .35rem .8rem;
    border-radius: 999px; font-size: 12px; font-weight: 600;
    font-family: 'Geist Mono', monospace;
  }
  .flash {
    max-width: 480px; margin: 1rem auto; padding: .75rem 1rem;
    background: rgba(248,113,113,.1); border: 1px solid rgba(248,113,113,.25);
    border-radius: 8px; color: #ffb1b1; font-size: 13px;
  }
</style>
"""

NAV = r"""
<nav class="top-nav">
  <div class="inner">
    <a href="/" class="brand-mark">
      <svg viewBox="0 0 32 32" fill="none">
        <rect width="32" height="32" rx="7" fill="#0a0a0a"/>
        <path d="M8 8 L24 24" stroke="#fff" stroke-width="3.2" stroke-linecap="round"/>
        <path d="M24 8 L8 24" stroke="#fff" stroke-width="3.2" stroke-linecap="round"/>
        <circle cx="16" cy="16" r="3.4" fill="#ff7a3c"/>
      </svg>
      ShopifySift
    </a>
    <div class="nav-r">
      {% if user %}
        <a href="/app/dork">Search</a>
        <a href="/dashboard">Dashboard</a>
        <a href="/pricing">Pricing</a>
        <span class="credit-pill">{{ user.credits }} credits</span>
        <a href="/logout" class="btn-ghost">Log out</a>
      {% else %}
        <a href="/pricing">Pricing</a>
        <a href="/login">Log in</a>
        <a href="/signup" class="btn-primary">Start free</a>
      {% endif %}
    </div>
  </div>
</nav>
"""

LANDING_PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>ShopifySift · Real Shopify operators hiding in X bios</title>
<meta name="description" content="The #1 lead search tool for Shopify operators. Find founders posting their store, URL, or niche in their X bio — so you can reach out first.">
<link rel="icon" type="image/svg+xml" href="/static/favicon.svg">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Geist:wght@400;500;600;700&family=Geist+Mono:wght@400;500&display=swap">
<style>
  :root {
    --bg: #0b0b0d;
    --bg-card: #131316;
    --bg-card-hover: #18181c;
    --text: #fafafa;
    --text2: #a3a3a3;
    --muted: #6b6b70;
    --line: rgba(255,255,255,.07);
    --line-strong: rgba(255,255,255,.14);
    --accent: #ff7a3c;
    --accent-soft: rgba(255,122,60,.14);
    --accent-line: rgba(255,122,60,.28);
    --green: #4ade80;
    color-scheme: dark;
  }
  * { box-sizing: border-box; }
  html, body { margin: 0; padding: 0; }
  body {
    font-family: 'Geist', -apple-system, BlinkMacSystemFont, system-ui, sans-serif;
    background: var(--bg); color: var(--text);
    font-size: 15px; line-height: 1.55; -webkit-font-smoothing: antialiased;
  }
  .mono { font-family: 'Geist Mono', ui-monospace, monospace; }
  a { color: inherit; text-decoration: none; }

  .container { max-width: 1180px; margin: 0 auto; padding: 0 1.5rem; }
  section { padding: 5rem 0; }

  .eyebrow {
    font-family: 'Geist Mono', monospace; font-size: 11.5px;
    color: var(--accent); text-transform: uppercase; letter-spacing: .12em;
    font-weight: 600; margin-bottom: 1.25rem;
  }

  /* nav */
  nav.nav { padding: 1.5rem 0; }
  nav .inner { display: flex; align-items: center; justify-content: space-between; }
  .brand-mark {
    display: inline-flex; align-items: center; gap: .55rem;
    font-weight: 600; font-size: 16px; letter-spacing: -.01em;
  }
  .brand-mark .x {
    width: 26px; height: 26px; display: grid; place-items: center;
    color: var(--accent);
  }
  .brand-mark .x svg { width: 100%; height: 100%; }
  .nav-r { display: flex; align-items: center; gap: 2rem; font-size: 14px; }
  .nav-r a { color: var(--text2); font-weight: 500; }
  .nav-r a:hover { color: var(--text); }
  .btn-orange {
    background: var(--accent); color: #fff; padding: .55rem 1.1rem;
    border-radius: 8px; font-weight: 600; font-size: 13.5px; transition: all .12s;
  }
  .btn-orange:hover { background: #ff8b4f; }

  /* HERO */
  .hero { padding: 3rem 0 4rem; }
  .hero-grid { display: grid; grid-template-columns: 1fr 1.05fr; gap: 4rem; align-items: center; }
  @media (max-width: 960px) { .hero-grid { grid-template-columns: 1fr; gap: 2.5rem; } }
  .hero h1 {
    font-size: clamp(40px, 5.5vw, 60px);
    line-height: 1; letter-spacing: -.025em; font-weight: 600;
    margin: 0 0 1.5rem; max-width: 12ch;
  }
  .hero h1 .gr { color: var(--accent); }
  .hero p.lede {
    color: var(--text2); font-size: 16px; max-width: 460px;
    margin: 0 0 2rem; line-height: 1.55;
  }
  .hero .cta { display: flex; gap: .75rem; align-items: center; flex-wrap: wrap; margin-bottom: 1.25rem; }
  .btn-cta {
    padding: .85rem 1.5rem; border-radius: 9px; font-size: 14px; font-weight: 600;
    display: inline-flex; align-items: center; gap: .4rem; transition: all .12s;
  }
  .btn-cta.orange { background: var(--accent); color: #fff; }
  .btn-cta.orange:hover { background: #ff8b4f; transform: translateY(-1px); }
  .btn-cta.ghost {
    background: transparent; color: var(--text);
    border: 1px solid var(--line-strong);
  }
  .btn-cta.ghost:hover { border-color: var(--text2); }
  .meta {
    color: var(--text2); font-size: 13px;
    display: inline-flex; align-items: center; gap: .4rem;
  }
  .meta .check { color: var(--green); }

  /* hero mockup */
  .mockup {
    background: var(--bg-card); border: 1px solid var(--line);
    border-radius: 14px; padding: 0; overflow: hidden;
    box-shadow: 0 30px 80px -20px rgba(0,0,0,.6);
  }
  .mock-bar {
    padding: .65rem 1rem; border-bottom: 1px solid var(--line);
    display: flex; align-items: center; gap: .65rem;
    font-family: 'Geist Mono', monospace; font-size: 11px; color: var(--muted);
  }
  .mock-bar .dots { display: inline-flex; gap: 6px; }
  .mock-bar .dots span { width: 10px; height: 10px; border-radius: 50%; background: #2a2a2a; }
  .mock-bar .ttl { flex: 1; text-align: center; }
  .mock-body { padding: 1.25rem; }
  .mock-search {
    display: flex; gap: .5rem; align-items: center;
    background: rgba(255,255,255,.04); border: 1px solid var(--line);
    border-radius: 8px; padding: .5rem .85rem; margin-bottom: 1rem;
    font-size: 13.5px;
  }
  .mock-search .lhs { color: var(--muted); flex: 1; }
  .mock-search .pill {
    font-family: 'Geist Mono', monospace; font-size: 10.5px;
    background: var(--accent-soft); color: var(--accent);
    padding: .15rem .55rem; border-radius: 4px; font-weight: 600;
  }
  .mock-stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: .5rem; margin-bottom: 1rem; }
  .mock-stat {
    background: rgba(255,255,255,.025); border: 1px solid var(--line);
    border-radius: 8px; padding: .65rem .8rem;
  }
  .mock-stat .n { font-size: 22px; font-weight: 700; letter-spacing: -.02em; line-height: 1; }
  .mock-stat .l {
    font-family: 'Geist Mono', monospace; font-size: 9px; color: var(--muted);
    text-transform: uppercase; letter-spacing: .08em; margin-top: .35rem; font-weight: 600;
  }
  .mock-leads { display: flex; flex-direction: column; gap: .45rem; }
  .mock-lead {
    display: grid; grid-template-columns: auto 1fr auto; gap: .85rem; align-items: center;
    padding: .7rem .85rem; border: 1px solid var(--line); border-radius: 8px;
    background: rgba(255,255,255,.015);
  }
  .mock-lead.hit { border-left: 3px solid var(--green); }
  .avi {
    width: 32px; height: 32px; border-radius: 50%;
    background: linear-gradient(135deg, var(--accent), #fbbf24);
    color: #0a0a0a; display: grid; place-items: center;
    font-weight: 700; font-size: 13px;
  }
  .mock-lead .top {
    display: flex; align-items: center; gap: .45rem; flex-wrap: wrap; margin-bottom: .15rem;
  }
  .mock-lead .h { font-weight: 600; font-size: 13.5px; }
  .badge {
    font-family: 'Geist Mono', monospace; font-size: 9.5px;
    padding: .12rem .45rem; border-radius: 4px;
    text-transform: uppercase; letter-spacing: .04em; font-weight: 600;
  }
  .badge.active { background: rgba(74,222,128,.14); color: var(--green); }
  .badge.ecomm { background: rgba(56,189,248,.14); color: #38bdf8; }
  .badge.store { background: rgba(167,139,250,.14); color: #a78bfa; }
  .mock-lead .body { font-size: 12px; color: var(--text2); line-height: 1.4; }
  .copy-btn {
    background: rgba(255,255,255,.05); color: var(--text);
    border: 1px solid var(--line); border-radius: 6px;
    padding: .35rem .8rem; font: inherit; font-size: 12px; cursor: pointer;
  }

  /* STATS BAR */
  .stats-bar {
    border-top: 1px solid var(--line); border-bottom: 1px solid var(--line);
    padding: 1.75rem 0;
  }
  .stats-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem; }
  @media (max-width: 720px) { .stats-row { grid-template-columns: repeat(2, 1fr); } }
  .stat-tile { display: flex; align-items: center; gap: .75rem; }
  .stat-tile .ico {
    width: 32px; height: 32px; border-radius: 8px;
    background: var(--accent-soft); color: var(--accent);
    display: grid; place-items: center; flex-shrink: 0;
  }
  .stat-tile .num { font-size: 22px; font-weight: 700; letter-spacing: -.02em; color: var(--accent); line-height: 1.1; }
  .stat-tile .lbl { font-size: 12px; color: var(--text2); }

  /* TWO-COL SECTION (left text, right cards) */
  .two-col { display: grid; grid-template-columns: 1fr 1.4fr; gap: 4rem; align-items: start; }
  @media (max-width: 960px) { .two-col { grid-template-columns: 1fr; gap: 2rem; } }
  .two-col h2 {
    font-size: clamp(28px, 3.8vw, 38px);
    line-height: 1.1; letter-spacing: -.02em; margin: 0 0 1rem; font-weight: 600; max-width: 14ch;
  }
  .two-col p { color: var(--text2); font-size: 15px; line-height: 1.55; margin: 0; max-width: 36ch; }

  .feat-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; }
  @media (max-width: 720px) { .feat-grid { grid-template-columns: 1fr; } }
  .feat-card {
    background: var(--bg-card); border: 1px solid var(--line);
    border-radius: 12px; padding: 1.5rem;
  }
  .feat-card .icon {
    width: 38px; height: 38px; border-radius: 9px;
    background: var(--accent-soft); color: var(--accent);
    display: grid; place-items: center; margin-bottom: 1rem;
  }
  .feat-card h4 { font-size: 15px; margin: 0 0 .35rem; font-weight: 600; }
  .feat-card p { font-size: 13px; color: var(--text2); margin: 0; line-height: 1.5; max-width: none; }

  .step-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: .85rem; }
  @media (max-width: 720px) { .step-grid { grid-template-columns: repeat(2, 1fr); } }
  .step-card { padding: 1rem 1.1rem; }
  .step-card .icon {
    width: 32px; height: 32px; border-radius: 8px;
    background: rgba(255,255,255,.04); color: var(--accent);
    display: grid; place-items: center; margin-bottom: .85rem;
  }
  .step-card h4 { font-size: 13.5px; margin: 0 0 .35rem; font-weight: 600; }
  .step-card p { font-size: 12px; color: var(--text2); margin: 0; line-height: 1.5; }

  /* TESTIMONIALS */
  .quotes-section { text-align: center; }
  .quotes-section .eyebrow { text-align: center; }
  .quotes-section h2 {
    font-size: clamp(28px, 3.8vw, 38px);
    line-height: 1.15; letter-spacing: -.02em; margin: 0 auto 3rem;
    font-weight: 600; max-width: 18ch;
  }
  .quotes-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 1.25rem; }
  @media (max-width: 960px) { .quotes-grid { grid-template-columns: 1fr; } }
  .quote-card {
    background: var(--bg-card); border: 1px solid var(--line);
    border-radius: 12px; padding: 1.6rem; text-align: left;
  }
  .stars { color: var(--accent); font-size: 14px; letter-spacing: .15em; margin-bottom: 1rem; }
  .quote-card .q { font-size: 14.5px; line-height: 1.55; margin: 0 0 1.5rem; color: var(--text); }
  .quote-card .who { display: flex; gap: .65rem; align-items: center; }
  .quote-card .who .avi { width: 32px; height: 32px; font-size: 13px; }
  .quote-card .who .nm { font-weight: 600; font-size: 13.5px; }
  .quote-card .who .ttl { font-size: 12px; color: var(--text2); margin-top: .1rem; }

  /* PRICING */
  .pricing-section { text-align: center; }
  .pricing-section .eyebrow { text-align: center; }
  .pricing-section h2 {
    font-size: clamp(28px, 3.8vw, 38px);
    line-height: 1.15; letter-spacing: -.02em; margin: 0 auto 3rem;
    font-weight: 600; max-width: 22ch;
  }
  .plans { display: grid; grid-template-columns: repeat(2, 1fr); gap: 1.25rem; max-width: 720px; margin: 0 auto; }
  @media (max-width: 720px) { .plans { grid-template-columns: 1fr; } }
  .plan {
    background: var(--bg-card); border: 1px solid var(--line);
    border-radius: 14px; padding: 1.75rem; text-align: left; position: relative;
  }
  .plan.featured { border-color: var(--accent-line); background: linear-gradient(180deg, rgba(255,122,60,.05), transparent 60%), var(--bg-card); }
  .plan-tag {
    position: absolute; top: 1.4rem; right: 1.4rem;
    background: var(--accent-soft); color: var(--accent);
    font-family: 'Geist Mono', monospace; font-size: 10px;
    padding: .2rem .5rem; border-radius: 4px;
    text-transform: uppercase; letter-spacing: .06em; font-weight: 700;
  }
  .plan h3 { font-size: 16px; margin: 0 0 .85rem; font-weight: 600; }
  .plan .pr { font-size: 38px; font-weight: 700; letter-spacing: -.025em; line-height: 1; margin-bottom: .25rem; }
  .plan .pr small { font-size: 14px; color: var(--text2); font-weight: 400; }
  .plan .desc { color: var(--text2); font-size: 13.5px; margin-bottom: 1.25rem; line-height: 1.5; }
  .plan ul { list-style: none; padding: 0; margin: 0 0 1.25rem; }
  .plan li {
    font-size: 13.5px; padding: .35rem 0; color: var(--text2);
    display: flex; gap: .55rem; align-items: center;
  }
  .plan li::before { content: "✓"; color: var(--accent); font-weight: 700; }
  .plan-btn {
    display: block; width: 100%; padding: .8rem; border-radius: 9px;
    font-size: 13.5px; font-weight: 600; text-align: center; transition: all .12s;
  }
  .plan-btn.ghost {
    background: transparent; color: var(--text); border: 1px solid var(--line-strong);
  }
  .plan-btn.ghost:hover { border-color: var(--text2); }
  .plan-btn.fill { background: var(--accent); color: #fff; }
  .plan-btn.fill:hover { background: #ff8b4f; }

  /* FAQ */
  .faq-section { display: grid; grid-template-columns: 220px 1fr; gap: 4rem; align-items: start; }
  @media (max-width: 800px) { .faq-section { grid-template-columns: 1fr; gap: 2rem; } }
  .faq-section .head .eyebrow { margin-bottom: .25rem; }
  .faq-section .head h2 {
    font-size: clamp(28px, 3.8vw, 38px); line-height: 1.15;
    letter-spacing: -.02em; margin: 0; font-weight: 600;
  }
  .faq-list { background: var(--bg-card); border: 1px solid var(--line); border-radius: 12px; }
  .faq-item { border-bottom: 1px solid var(--line); }
  .faq-item:last-child { border-bottom: 0; }
  .faq-item summary {
    list-style: none; cursor: pointer; padding: 1rem 1.25rem;
    font-size: 14.5px; font-weight: 500;
    display: flex; justify-content: space-between; align-items: center;
  }
  .faq-item summary::-webkit-details-marker { display: none; }
  .faq-item summary::after { content: "+"; color: var(--text2); font-size: 22px; font-weight: 300; }
  .faq-item[open] summary::after { content: "−"; }
  .faq-item p {
    color: var(--text2); margin: 0 1.25rem 1.25rem;
    font-size: 13.5px; line-height: 1.6; max-width: 560px;
  }

  /* GET STARTED CTA */
  .cta-section .cta-card {
    background: var(--bg-card); border: 1px solid var(--accent-line);
    border-radius: 16px; padding: 3rem;
    background-image:
      radial-gradient(400px circle at 5% 100%, rgba(255,122,60,.06), transparent 50%),
      radial-gradient(400px circle at 95% 0%, rgba(255,122,60,.04), transparent 50%);
  }
  .cta-grid { display: grid; grid-template-columns: 1.3fr 1fr; gap: 3rem; align-items: center; }
  @media (max-width: 800px) { .cta-grid { grid-template-columns: 1fr; gap: 2rem; } }
  .cta-card h2 {
    font-size: clamp(28px, 3.8vw, 38px); line-height: 1.1;
    letter-spacing: -.02em; margin: 0 0 1.5rem; font-weight: 600;
  }
  .cta-checks { list-style: none; padding: 0; margin: 0; display: flex; flex-direction: column; gap: .65rem; }
  .cta-checks li { display: flex; gap: .65rem; align-items: flex-start; font-size: 13.5px; color: var(--text2); }
  .cta-checks li::before {
    content: "✓"; color: var(--accent); font-weight: 700;
    flex-shrink: 0; line-height: 1.5;
  }
  .form-card {
    background: var(--bg); border: 1px solid var(--line);
    border-radius: 12px; padding: 1.75rem;
  }
  .form-card .lbl {
    font-family: 'Geist Mono', monospace; font-size: 11px;
    color: var(--accent); text-transform: uppercase; letter-spacing: .08em;
    font-weight: 600; margin-bottom: .85rem;
  }
  .form-card h3 { margin: 0 0 1.25rem; font-size: 18px; font-weight: 600; }
  .form-card label { display: block; font-size: 11px; color: var(--text2); margin: 0 0 .35rem;
    text-transform: uppercase; letter-spacing: .06em; font-weight: 600; }
  .form-card input[type=email] {
    width: 100%; background: rgba(255,255,255,.04); color: var(--text);
    border: 1px solid var(--line); border-radius: 8px;
    padding: .75rem .9rem; font: inherit; font-size: 14px; margin-bottom: 1rem;
    transition: border-color .12s;
  }
  .form-card input[type=email]:focus { outline: none; border-color: var(--accent); box-shadow: 0 0 0 3px rgba(255,122,60,.12); }
  .form-card button {
    width: 100%; padding: .85rem; font: inherit; font-weight: 600; cursor: pointer;
    background: var(--accent); color: #fff; border: 0; border-radius: 8px;
    font-size: 14px; transition: all .12s;
  }
  .form-card button:hover { background: #ff8b4f; }
  .form-card .meta-line {
    text-align: center; margin-top: .85rem; font-size: 12px; color: var(--text2);
  }

  /* FOOTER */
  footer.f { padding: 3rem 0 2rem; border-top: 1px solid var(--line); margin-top: 3rem; }
  .f-grid { display: grid; grid-template-columns: 2fr 1fr 1fr 1fr; gap: 2.5rem; margin-bottom: 2.5rem; }
  @media (max-width: 720px) { .f-grid { grid-template-columns: 1fr 1fr; } }
  .f-grid h5 {
    font-family: 'Geist Mono', monospace; font-size: 11px;
    color: var(--text2); text-transform: uppercase; letter-spacing: .08em;
    margin: 0 0 1rem; font-weight: 600;
  }
  .f-grid ul { list-style: none; padding: 0; margin: 0; }
  .f-grid li { padding: .25rem 0; font-size: 13.5px; }
  .f-grid li a { color: var(--text2); }
  .f-grid li a:hover { color: var(--text); }
  .f-grid .blurb { color: var(--text2); font-size: 13px; max-width: 240px; line-height: 1.5; margin-top: .85rem; }
  .f-bottom {
    border-top: 1px solid var(--line); padding-top: 1.25rem;
    display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 1rem;
    color: var(--muted); font-size: 12px;
  }
  .f-social { display: flex; gap: .85rem; align-items: center; }
  .f-social a {
    width: 30px; height: 30px; border-radius: 50%;
    background: rgba(255,255,255,.04); display: grid; place-items: center;
    color: var(--text2); transition: all .12s;
  }
  .f-social a:hover { color: var(--text); background: rgba(255,255,255,.08); }
</style>
</head>
<body>

<nav class="nav">
  <div class="container inner">
    <a href="/" class="brand-mark">
      <span class="x">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round">
          <line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>
        </svg>
      </span>
      ShopifySift
    </a>
    <div class="nav-r">
      <a href="#features">Features</a>
      <a href="#pricing">Pricing</a>
      <a href="#faq">FAQ</a>
      {% if user %}
        <a href="/dashboard" class="btn-orange">Dashboard</a>
      {% else %}
        <a href="/login">Log in</a>
        <a href="/dashboard" class="btn-orange">Dashboard</a>
      {% endif %}
    </div>
  </div>
</nav>

<section class="hero">
  <div class="container">
    <div class="hero-grid">
      <div>
        <div class="eyebrow">// The #1 lead search tool for Shopify operators</div>
        <h1>Real Shopify operators. Hiding in <span class="gr">X bios</span>.</h1>
        <p class="lede">Apollo doesn't have these leads. Clay doesn't either. ShopifySift finds founders posting their store, URL, or niche in their X bio — so you can reach out first.</p>
        <div class="cta">
          <a href="/signup" class="btn-cta orange">Start free →</a>
          <a href="#how" class="btn-cta ghost">See how it works</a>
        </div>
        <div class="meta"><span class="check">✓</span> No credit card. No setup. Start in seconds.</div>
      </div>
      <div class="mockup">
        <div class="mock-bar">
          <span class="dots"><span></span><span></span><span></span></span>
          <span class="ttl">SHOPIFYSIFT · LIVE SEARCH</span>
        </div>
        <div class="mock-body">
          <div class="mock-search">
            <span class="lhs">Search keywords...</span>
            <span class="pill">USA · X</span>
          </div>
          <div class="mock-stats">
            <div class="mock-stat"><div class="n">7</div><div class="l">Active Leads</div></div>
            <div class="mock-stat"><div class="n">4</div><div class="l">New today</div></div>
            <div class="mock-stat"><div class="n">3</div><div class="l">Replied</div></div>
            <div class="mock-stat"><div class="n">1.2k</div><div class="l">Total found</div></div>
          </div>
          <div class="mock-leads">
            <div class="mock-lead">
              <div class="avi">J</div>
              <div>
                <div class="top">
                  <span class="h">@jackkessler_</span>
                  <span class="badge active">● active</span>
                  <span class="badge ecomm">e-comm</span>
                </div>
                <div class="body">Building @TrendlyStore — premium dropshipping products.</div>
              </div>
              <button class="copy-btn">Copy</button>
            </div>
            <div class="mock-lead hit">
              <div class="avi">N</div>
              <div>
                <div class="top">
                  <span class="h">@nurecom</span>
                  <span class="badge active">● active</span>
                  <span class="badge ecomm">e-comm</span>
                </div>
                <div class="body">Founder of @NureStore — free shipping over $79.</div>
              </div>
              <button class="copy-btn">Copy</button>
            </div>
            <div class="mock-lead">
              <div class="avi">D</div>
              <div>
                <div class="top">
                  <span class="h">@domen_luk</span>
                  <span class="badge active">● active</span>
                  <span class="badge store">store setup</span>
                </div>
                <div class="body">CPA marketer. Building in the health supplement niche.</div>
              </div>
              <button class="copy-btn">Copy</button>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</section>

<div class="stats-bar">
  <div class="container">
    <div class="stats-row">
      <div class="stat-tile">
        <div class="ico"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg></div>
        <div><div class="num">14,000+</div><div class="lbl">Verified founders found</div></div>
      </div>
      <div class="stat-tile">
        <div class="ico"><svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg></div>
        <div><div class="num">50/search</div><div class="lbl">Average active leads</div></div>
      </div>
      <div class="stat-tile">
        <div class="ico"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg></div>
        <div><div class="num">12s</div><div class="lbl">First lead in your inbox</div></div>
      </div>
      <div class="stat-tile">
        <div class="ico"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg></div>
        <div><div class="num">0</div><div class="lbl">Bounced emails ever</div></div>
      </div>
    </div>
  </div>
</div>

<section id="features">
  <div class="container">
    <div class="two-col">
      <div>
        <div class="eyebrow">// Built different</div>
        <h2>Leads with a face, not a row in a CSV.</h2>
        <p>Apollo gives you 50,000+ emails. Half bounce. The other half ignore you. We dig into X handles whose bios literally say "founder of", "new store", "my build", etc. They'll respond.</p>
      </div>
      <div class="feat-grid">
        <div class="feat-card">
          <div class="icon"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/></svg></div>
          <h4>Bio-level targeting</h4>
          <p>We look exactly where X is full of unfiltered signals: URLs, stores, niches, growth goals, product launches.</p>
        </div>
        <div class="feat-card">
          <div class="icon"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="20 6 9 17 4 12"/></svg></div>
          <h4>Active + store check</h4>
          <p>Every lead, their website, has payments enabled, has inventory, and launched recently. "Active" means they're building right now.</p>
        </div>
        <div class="feat-card">
          <div class="icon"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg></div>
          <h4>One-click outreach</h4>
          <p>Click and the lead is copied, ready to DM. CSV export for Apollo / Clay / Instantly.</p>
        </div>
      </div>
    </div>
  </div>
</section>

<section id="how">
  <div class="container">
    <div class="two-col">
      <div>
        <div class="eyebrow">// Made for operators</div>
        <h2>Type a niche. Get DM-ready handles. That's it.</h2>
      </div>
      <div class="step-grid">
        <div class="feat-card step-card">
          <div class="icon"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/></svg></div>
          <h4>Pick a niche</h4>
          <p>"Pick a niche" is optional. X is already public — we surface what's already out there.</p>
        </div>
        <div class="feat-card step-card">
          <div class="icon"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="4"/></svg></div>
          <h4>Check engine</h4>
          <p>ShopifySift scans X bios for stores, niches, keywords, and buying intent signals.</p>
        </div>
        <div class="feat-card step-card">
          <div class="icon"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><polyline points="9 12 11 14 15 10"/></svg></div>
          <h4>Verify live</h4>
          <p>We verify the store is live and active so you don't waste a single outreach.</p>
        </div>
        <div class="feat-card step-card">
          <div class="icon"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg></div>
          <h4>Copy and DM</h4>
          <p>Grab verified leads and start conversations that convert.</p>
        </div>
      </div>
    </div>
  </div>
</section>

<section class="quotes-section">
  <div class="container">
    <div class="eyebrow">// Loved by operators</div>
    <h2>Operators replacing $300/mo lead tools.</h2>
    <div class="quotes-grid">
      <div class="quote-card">
        <div class="stars">★★★★★</div>
        <p class="q">"Cancelled Apollo after a week. The leads here are actually replying because the data is real. ShopifySift just works."</p>
        <div class="who"><div class="avi">M</div><div><div class="nm">Marcus T.</div><div class="ttl">7-figure store owner</div></div></div>
      </div>
      <div class="quote-card">
        <div class="stars">★★★★★</div>
        <p class="q">"Used to spend 4 hours scraping LinkedIn for store founders. ShopifySift pulls 100 fresh leads and they all exist."</p>
        <div class="who"><div class="avi">S</div><div><div class="nm">Sam L.</div><div class="ttl">DTC operator</div></div></div>
      </div>
      <div class="quote-card">
        <div class="stars">★★★★★</div>
        <p class="q">"The 'verified + active' filter is huge. I'm getting replies from operators, not dead accounts. It's like Clay on easy mode."</p>
        <div class="who"><div class="avi">D</div><div><div class="nm">Devin R.</div><div class="ttl">E-commerce builder</div></div></div>
      </div>
    </div>
  </div>
</section>

<section>
  <div class="container">
    <div class="two-col">
      <div>
        <div class="eyebrow">// We sift X so you don't waste a single DM.</div>
        <h2>Stop sending DMs into the void.</h2>
        <p>Apollo shows you 50,000+ names. Half bounce, the rest ghost. ShopifySift finds the operators who are building right now.</p>
      </div>
      <div style="display:grid; grid-template-columns: 1fr 1fr; gap: .85rem;">
        <div class="feat-card" style="padding: 1.25rem;">
          <div class="icon" style="margin-bottom: .65rem;"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/></svg></div>
          <div style="font-size: 28px; font-weight: 700; color: var(--accent); letter-spacing: -.02em; line-height: 1;">14k+</div>
          <div style="font-size: 12px; color: var(--text2); margin-top: .35rem;">Leads discovered</div>
        </div>
        <div class="feat-card" style="padding: 1.25rem;">
          <div class="icon" style="margin-bottom: .65rem;"><svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg></div>
          <div style="font-size: 28px; font-weight: 700; color: var(--accent); letter-spacing: -.02em; line-height: 1;">50</div>
          <div style="font-size: 12px; color: var(--text2); margin-top: .35rem;">Avg active leads / search</div>
        </div>
        <div class="feat-card" style="padding: 1.25rem;">
          <div class="icon" style="margin-bottom: .65rem;"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg></div>
          <div style="font-size: 28px; font-weight: 700; color: var(--accent); letter-spacing: -.02em; line-height: 1;">12s</div>
          <div style="font-size: 12px; color: var(--text2); margin-top: .35rem;">First lead in your inbox</div>
        </div>
        <div class="feat-card" style="padding: 1.25rem;">
          <div class="icon" style="margin-bottom: .65rem;"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg></div>
          <div style="font-size: 28px; font-weight: 700; color: var(--accent); letter-spacing: -.02em; line-height: 1;">0</div>
          <div style="font-size: 12px; color: var(--text2); margin-top: .35rem;">Bounced emails ever</div>
        </div>
      </div>
    </div>
  </div>
</section>

<section id="pricing" class="pricing-section">
  <div class="container">
    <div class="eyebrow">// Pricing</div>
    <h2>Start free. Upgrade when you're winning.</h2>
    <div class="plans">
      <div class="plan">
        <div class="plan-tag">Free</div>
        <h3>Starter</h3>
        <div class="pr">$0<small> /month</small></div>
        <p class="desc">Try it. Test the leads. No card.</p>
        <ul>
          <li>5 searches</li>
          <li>All results (100 per search)</li>
          <li>Active + store verified</li>
          <li>CSV export</li>
        </ul>
        <a href="/signup" class="plan-btn ghost">Start for free</a>
      </div>
      <div class="plan featured">
        <div class="plan-tag">Popular</div>
        <h3>Operator</h3>
        <div class="pr">$29<small> /month</small></div>
        <p class="desc">For serious cash-generators. Extra scale, more power.</p>
        <ul>
          <li>Unlimited searches</li>
          <li>Active sustains pipeline</li>
          <li>Saved searches</li>
          <li>Bulk CSV export</li>
          <li>Priority support</li>
        </ul>
        <a href="/pricing" class="plan-btn fill">Start 7-day free trial</a>
      </div>
    </div>
  </div>
</section>

<section id="faq">
  <div class="container">
    <div class="faq-section">
      <div class="head">
        <div class="eyebrow">// FAQ</div>
        <h2>Common questions.</h2>
      </div>
      <div class="faq-list">
        <details class="faq-item">
          <summary>How is this different from Apollo or Clay?</summary>
          <p>Apollo and Clay sell huge lists of B2B contacts pulled from LinkedIn and ZoomInfo. We pull a smaller list of X handles whose bios literally point at a Shopify store they built. Smaller list, but every lead has a real "in" for your DM. Conversion is typically 5-10× cold email.</p>
        </details>
        <details class="faq-item">
          <summary>Where does the data come from?</summary>
          <p>Public X (Twitter) bios indexed by DuckDuckGo and Brave Search. We dork search engines for X profiles whose bio text contains Shopify store URLs, then verify each store is real and actively selling. No private databases, no scraped accounts.</p>
        </details>
        <details class="faq-item">
          <summary>Is this scraping? Is it legal?</summary>
          <p>We don't scrape X directly — we query public search engine indexes that already contain public bio data. Operating within search engine ToS. What you do with the resulting handles (DMs, follows, etc.) follows X's user-facing rules.</p>
        </details>
        <details class="faq-item">
          <summary>What does "active" actually mean?</summary>
          <p>The store's <code>/products.json</code> endpoint returns at least one product, that product has a price greater than $0, and the catalog has been updated in the last 365 days. This filters out dev stores, abandoned catalogs, and password-gated stores.</p>
        </details>
        <details class="faq-item">
          <summary>How many leads per search?</summary>
          <p>Average is 30-80 active leads per niche keyword. Broader keywords + more keywords = more leads. A "skincare" + "fitness" + "coffee" + "candles" run typically pulls 150-250 active handles.</p>
        </details>
        <details class="faq-item">
          <summary>Do you store the leads I find?</summary>
          <p>We log search history (keywords + counts) for your dashboard, but we don't keep the actual lead handles or store URLs server-side. Export them to CSV right after a search to save them.</p>
        </details>
      </div>
    </div>
  </div>
</section>

<section class="cta-section">
  <div class="container">
    <div class="cta-card">
      <div class="cta-grid">
        <div>
          <div class="eyebrow">// Get started</div>
          <h2>Sift your first niche.<br>In about 12 seconds.</h2>
          <ul class="cta-checks">
            <li>5 free searches. No credit card.</li>
            <li>No tutorials. Just type a niche and watch Shopify-handled bios stream in.</li>
            <li>No setup. No learning curve.</li>
            <li>Cancel anytime. Come back anytime.</li>
          </ul>
        </div>
        <div class="form-card">
          <div class="lbl">Get instant access</div>
          <h3>Start sifting →</h3>
          <form action="/signup" method="post">
            <label>Your email address</label>
            <input type="email" name="email" required placeholder="founder@yourstore.com">
            <button type="submit">Start free →</button>
            <div class="meta-line">No credit card · Free forever</div>
          </form>
        </div>
      </div>
    </div>
  </div>
</section>

<footer class="f">
  <div class="container">
    <div class="f-grid">
      <div>
        <a href="/" class="brand-mark">
          <span class="x">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round">
              <line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>
            </svg>
          </span>
          ShopifySift
        </a>
        <p class="blurb">Real Shopify leads hiding in X bios. For operators doing cold outreach.</p>
      </div>
      <div>
        <h5>Product</h5>
        <ul>
          <li><a href="#features">Features</a></li>
          <li><a href="#how">How it works</a></li>
          <li><a href="#pricing">Pricing</a></li>
          <li><a href="#">Updates</a></li>
        </ul>
      </div>
      <div>
        <h5>Company</h5>
        <ul>
          <li><a href="#">About</a></li>
          <li><a href="mailto:hi@shopifysift.app">Contact</a></li>
          <li><a href="#">Blog</a></li>
        </ul>
      </div>
      <div>
        <h5>Legal</h5>
        <ul>
          <li><a href="/terms">Terms</a></li>
          <li><a href="/privacy">Privacy</a></li>
        </ul>
      </div>
    </div>
    <div class="f-bottom">
      <span>© 2026 ShopifySift. All rights reserved.</span>
      <div class="f-social">
        <a href="#" aria-label="X"><svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"/></svg></a>
        <a href="#" aria-label="Discord"><svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M20.317 4.37a19.79 19.79 0 0 0-4.885-1.515.074.074 0 0 0-.079.037c-.21.375-.444.864-.608 1.25a18.27 18.27 0 0 0-5.487 0 12.64 12.64 0 0 0-.617-1.25.077.077 0 0 0-.079-.037A19.736 19.736 0 0 0 3.677 4.37a.07.07 0 0 0-.032.027C.533 9.046-.32 13.58.099 18.057a.082.082 0 0 0 .031.057 19.9 19.9 0 0 0 5.993 3.03.078.078 0 0 0 .084-.028 14.09 14.09 0 0 0 1.226-1.994.076.076 0 0 0-.041-.106 13.107 13.107 0 0 1-1.872-.892.077.077 0 0 1-.008-.128 10.2 10.2 0 0 0 .372-.292.074.074 0 0 1 .077-.01c3.928 1.793 8.18 1.793 12.062 0a.074.074 0 0 1 .078.01c.12.098.246.198.373.292a.077.077 0 0 1-.006.127 12.299 12.299 0 0 1-1.873.892.077.077 0 0 0-.041.107c.36.698.772 1.362 1.225 1.993a.076.076 0 0 0 .084.028 19.839 19.839 0 0 0 6.002-3.03.077.077 0 0 0 .032-.054c.5-5.177-.838-9.674-3.549-13.66a.061.061 0 0 0-.031-.03zM8.02 15.33c-1.183 0-2.157-1.085-2.157-2.419 0-1.333.956-2.419 2.157-2.419 1.21 0 2.176 1.096 2.157 2.42 0 1.333-.956 2.418-2.157 2.418zm7.975 0c-1.183 0-2.157-1.085-2.157-2.419 0-1.333.955-2.419 2.157-2.419 1.21 0 2.176 1.096 2.157 2.42 0 1.333-.946 2.418-2.157 2.418z"/></svg></a>
        <a href="mailto:hi@shopifysift.app" aria-label="Email"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><polyline points="22,6 12,13 2,6"/></svg></a>
      </div>
    </div>
  </div>
</footer>

</body>
</html>
"""

AUTH_PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{ title }} · ShopifySift</title>
""" + SHARED_STYLE + r"""
<style>
  .auth-wrap { max-width: 440px; margin: 5rem auto; padding: 0 1.5rem; }
  .auth-eyebrow {
    font-family: 'Geist Mono', monospace; font-size: 11px;
    color: var(--accent); text-transform: uppercase; letter-spacing: .08em;
    margin-bottom: 1rem; font-weight: 600;
  }
  .auth-card {
    background: var(--bg-card); border: 1px solid var(--line);
    border-radius: 14px; padding: 2.25rem;
  }
  .auth-card h1 {
    font-size: 28px; margin: 0 0 .5rem; letter-spacing: -.025em;
    line-height: 1.1; font-weight: 600;
  }
  .auth-card p.sub { color: var(--text2); font-size: 14.5px; margin: 0 0 1.75rem; line-height: 1.5; }
  label { display: block; font-size: 11px; color: var(--text2); margin: 0 0 .4rem;
    text-transform: uppercase; letter-spacing: .06em; font-weight: 600; }
  input[type=email], input[type=password] {
    background: rgba(0,0,0,.35); color: var(--text);
    border: 1px solid var(--line-strong); border-radius: 9px;
    padding: .85rem 1rem; font: inherit; font-size: 14.5px;
    width: 100%; margin-bottom: 1.25rem; transition: all .12s;
  }
  input[type=email]:focus, input[type=password]:focus { outline: none; border-color: var(--accent); box-shadow: 0 0 0 3px rgba(255,122,60,.15); }
  button[type=submit] {
    width: 100%; padding: .9rem; font: inherit; font-weight: 600; cursor: pointer;
    background: var(--accent); color: #fff; border: 1px solid var(--accent);
    border-radius: 9px; font-size: 14.5px; transition: all .12s;
  }
  button[type=submit]:hover { background: var(--accent-hover); border-color: var(--accent-hover); transform: translateY(-1px); }
  .switch-link { text-align: center; margin-top: 1.5rem; font-size: 13px; color: var(--text2); }
  .switch-link a { color: var(--accent); font-weight: 500; }
  .auth-perks {
    list-style: none; padding: 0; margin: 1.5rem 0 0; display: flex;
    flex-direction: column; gap: .55rem; font-size: 13px; color: var(--text2);
  }
  .auth-perks li { display: flex; gap: .55rem; align-items: center; }
  .auth-perks li::before { content: "✓"; color: var(--accent); font-weight: 700; }
</style>
</head>
<body>
""" + NAV + r"""
{% with messages = get_flashed_messages() %}
  {% for m in messages %}<div class="flash">{{ m }}</div>{% endfor %}
{% endwith %}
<div class="auth-wrap">
  <div class="auth-eyebrow">// {{ title }}</div>
  <div class="auth-card">
    <h1>{{ title }}</h1>
    <p class="sub">{{ sub }}</p>
    <form method="post">
      <label>Email</label>
      <input type="email" name="email" required autofocus placeholder="you@example.com" value="{{ request.form.get('email','') }}">
      {% if show_password %}
        <label>Password</label>
        <input type="password" name="password" required minlength="8" placeholder="At least 8 characters">
      {% endif %}
      <button type="submit">{{ cta }} →</button>
    </form>
    <ul class="auth-perks">
      <li>5 free searches to start</li>
      <li>No credit card required</li>
      <li>Cancel anytime — free tier is forever</li>
    </ul>
    <p class="switch-link">{{ switch_text|safe }}</p>
  </div>
</div>
</body>
</html>
"""

APP_SHELL = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{ page_title }} · ShopifySift</title>
<link rel="icon" type="image/svg+xml" href="/static/favicon.svg">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Geist:wght@400;500;600;700&family=Geist+Mono:wght@400;500&display=swap">
<style>
  :root {
    --bg: #0b0b0d; --bg-card: #131316; --sidebar: #0e0e11;
    --text: #fafafa; --text2: #a3a3a3; --muted: #6b6b70;
    --line: rgba(255,255,255,.06); --line-strong: rgba(255,255,255,.12);
    --accent: #ff7a3c; --accent-soft: rgba(255,122,60,.14);
    --orange: #ff7a3c; --blue: #38bdf8; --pink: #ec4899; --purple: #a78bfa;
    --green: #4ade80; --red: #f87171;
    color-scheme: dark;
  }
  * { box-sizing: border-box; }
  html, body { margin: 0; padding: 0; }
  body {
    font-family: 'Geist', -apple-system, BlinkMacSystemFont, system-ui, sans-serif;
    background: var(--bg); color: var(--text);
    font-size: 14px; line-height: 1.55; -webkit-font-smoothing: antialiased;
  }
  a { color: inherit; text-decoration: none; }
  .layout { display: grid; grid-template-columns: 240px 1fr; min-height: 100vh; }
  aside.sidebar {
    background: var(--sidebar); border-right: 1px solid var(--line);
    display: flex; flex-direction: column; padding: 1.5rem 1rem 1rem;
  }
  .side-brand {
    display: flex; align-items: center; gap: .55rem;
    padding: .25rem .75rem 1.5rem;
  }
  .side-brand .x-mark { width: 28px; height: 28px; color: var(--accent); }
  .side-brand .name { font-size: 16px; font-weight: 600; letter-spacing: -.01em; }
  .side-nav { display: flex; flex-direction: column; gap: .15rem; }
  .side-nav a {
    display: flex; align-items: center; gap: .75rem;
    padding: .65rem .85rem; border-radius: 8px;
    color: var(--text2); font-size: 14px; font-weight: 500;
    transition: all .12s; position: relative;
  }
  .side-nav a:hover { background: rgba(255,255,255,.04); color: var(--text); }
  .side-nav a.active {
    background: var(--accent-soft); color: var(--accent);
  }
  .side-nav a.active::before {
    content: ''; position: absolute; left: -1rem; top: 8px; bottom: 8px;
    width: 3px; background: var(--accent); border-radius: 0 2px 2px 0;
  }
  .side-nav a svg { width: 18px; height: 18px; opacity: .9; flex-shrink: 0; }
  .side-spacer { flex: 1; min-height: 1.5rem; }
  .credit-widget {
    background: linear-gradient(180deg, rgba(255,122,60,.06), transparent), var(--bg-card);
    border: 1px solid var(--line); border-radius: 12px; padding: 1.1rem 1.15rem;
    margin-bottom: .65rem;
  }
  .credit-widget .top {
    display: flex; align-items: center; gap: .65rem; margin-bottom: .15rem;
  }
  .credit-widget .num {
    font-size: 32px; font-weight: 700; letter-spacing: -.025em;
    color: var(--accent); line-height: 1;
  }
  .credit-widget .bolt { color: var(--accent); opacity: .85; }
  .credit-widget .lbl { font-size: 12.5px; color: var(--text2); margin-bottom: .65rem; }
  .credit-widget .progress {
    height: 4px; background: rgba(255,255,255,.08); border-radius: 2px;
    overflow: hidden; margin-bottom: 1rem;
  }
  .credit-widget .progress > span {
    display: block; height: 100%; background: var(--accent); border-radius: 2px;
  }
  .credit-widget .ctxt { padding-top: .55rem; border-top: 1px solid var(--line); }
  .credit-widget .ctxt .head { font-size: 12.5px; font-weight: 600; margin-bottom: .25rem; }
  .credit-widget .ctxt .sub { font-size: 12px; color: var(--text2); margin-bottom: .85rem; line-height: 1.45; }
  .credit-widget a.upgrade {
    display: block; text-align: center; padding: .55rem; font-size: 12.5px; font-weight: 600;
    background: var(--accent); color: #fff; border-radius: 7px; transition: all .12s;
  }
  .credit-widget a.upgrade:hover { background: #ff8b4f; }
  .side-logout {
    display: flex; align-items: center; gap: .75rem;
    padding: .65rem .85rem; border-radius: 8px;
    color: var(--text2); font-size: 14px; font-weight: 500;
    border-top: 1px solid var(--line); margin-top: .25rem; padding-top: 1rem;
  }
  .side-logout:hover { color: var(--text); }
  .side-logout svg { width: 18px; height: 18px; opacity: .9; }

  main.main { padding: 1.25rem 2rem 4rem; min-width: 0; position: relative; }
  main.main::before {
    content: ''; position: absolute; right: 0; top: 0;
    width: 600px; height: 500px; pointer-events: none;
    background:
      radial-gradient(ellipse 400px 200px at 90% 30%, rgba(255,122,60,.18), transparent 60%);
    z-index: 0;
  }
  main.main > * { position: relative; z-index: 1; }

  header.topbar {
    display: flex; align-items: center; gap: 1rem; margin-bottom: 3rem;
  }
  .topbar .search { flex: 1; position: relative; max-width: 480px; }
  .topbar .search input {
    width: 100%; background: rgba(255,255,255,.04);
    border: 1px solid var(--line); border-radius: 10px;
    padding: .65rem 2.6rem .65rem 2.6rem; font: inherit; font-size: 13.5px;
    color: var(--text);
  }
  .topbar .search input::placeholder { color: var(--muted); }
  .topbar .search input:focus { outline: none; border-color: var(--line-strong); }
  .topbar .search .ico-l {
    position: absolute; left: .85rem; top: 50%; transform: translateY(-50%);
    color: var(--muted);
  }
  .topbar .search kbd {
    position: absolute; right: .55rem; top: 50%; transform: translateY(-50%);
    background: rgba(255,255,255,.06); color: var(--text2);
    padding: .15rem .4rem; border-radius: 4px; font-family: 'Geist Mono', monospace;
    font-size: 10.5px; font-weight: 500;
  }
  .topbar .pricing-link {
    color: var(--text2); font-size: 14px; font-weight: 500; padding: .5rem .9rem;
  }
  .topbar .pricing-link:hover { color: var(--text); }
  .topbar .credit-pill {
    background: var(--accent-soft); color: var(--accent);
    border: 1px solid rgba(255,122,60,.22); padding: .5rem .95rem;
    border-radius: 999px; font-size: 13px; font-weight: 600;
    font-family: 'Geist Mono', monospace;
  }
  .topbar .user-chip {
    display: flex; align-items: center; gap: .45rem;
    background: var(--bg-card); border: 1px solid var(--line);
    border-radius: 999px; padding: .25rem .55rem .25rem .25rem;
    cursor: pointer; transition: all .12s; position: relative;
  }
  .topbar .user-chip:hover { border-color: var(--line-strong); }
  .topbar .user-chip .avi {
    width: 30px; height: 30px; border-radius: 50%;
    background: linear-gradient(135deg, #ff7a3c, #fbbf24);
    color: #0a0a0a; display: grid; place-items: center;
    font-weight: 700; font-size: 12.5px;
  }
  .topbar .user-chip .chev { color: var(--text2); }
  .topbar .user-chip .menu {
    position: absolute; top: calc(100% + .5rem); right: 0;
    background: var(--bg-card); border: 1px solid var(--line);
    border-radius: 10px; padding: .35rem;
    min-width: 180px; display: none; z-index: 50;
    box-shadow: 0 12px 40px rgba(0,0,0,.5);
  }
  .topbar .user-chip:hover .menu { display: block; }
  .topbar .user-chip .menu a {
    display: block; padding: .55rem .75rem; border-radius: 7px;
    font-size: 13.5px; color: var(--text2);
  }
  .topbar .user-chip .menu a:hover { background: rgba(255,255,255,.05); color: var(--text); }

  .page-eyebrow {
    font-family: 'Geist Mono', monospace; font-size: 12px;
    color: var(--accent); text-transform: uppercase; letter-spacing: .12em;
    font-weight: 600; margin-bottom: 1rem;
  }
  .page-title {
    font-size: clamp(36px, 5vw, 52px); font-weight: 600;
    letter-spacing: -.025em; line-height: 1; margin: 0 0 .85rem;
  }
  .page-meta { color: var(--text2); font-size: 14px; margin: 0 0 3rem; }

  /* shared cards */
  .panel {
    background: var(--bg-card); border: 1px solid var(--line);
    border-radius: 14px; padding: 1.75rem;
  }
  .panel h2 {
    font-size: 18px; margin: 0 0 .35rem; font-weight: 600; letter-spacing: -.01em;
  }
  .panel p.sub { color: var(--text2); font-size: 13.5px; margin: 0 0 1.5rem; }

  table.t {
    width: 100%; background: var(--bg-card); border: 1px solid var(--line);
    border-radius: 12px; border-collapse: separate; border-spacing: 0;
    overflow: hidden; font-size: 13.5px;
  }
  table.t th, table.t td { padding: .85rem 1.1rem; text-align: left; }
  table.t th {
    background: rgba(255,255,255,.02); color: var(--text2);
    font-family: 'Geist Mono', monospace; font-size: 11px;
    text-transform: uppercase; letter-spacing: .08em; font-weight: 600;
  }
  table.t tr + tr td { border-top: 1px solid var(--line); }
  table.t td.kw { color: var(--text); font-weight: 500; }
  table.t td .num-active { color: var(--green); font-weight: 600; }

  .empty-card {
    background: var(--bg-card); border: 1px dashed var(--line-strong);
    border-radius: 14px; padding: 4rem 2rem; text-align: center;
  }
  .empty-card .ico {
    width: 48px; height: 48px; margin: 0 auto 1.25rem;
    border-radius: 50%; background: rgba(255,255,255,.04);
    display: grid; place-items: center; color: var(--text2);
  }
  .empty-card p { font-size: 16px; color: var(--text); font-weight: 500; margin: 0 0 .75rem; }
  .empty-card a.cta { color: var(--accent); font-size: 14px; font-weight: 500; }
  .empty-card a.cta:hover { text-decoration: underline; }

  .stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem; margin-bottom: 2rem; }
  @media (max-width: 1100px) { .stats { grid-template-columns: repeat(2, 1fr); } }
  .stat-card {
    background: var(--bg-card); border: 1px solid var(--line);
    border-radius: 14px; padding: 1.6rem 1.6rem 1.4rem;
  }
  .stat-card.featured { border-color: rgba(255,122,60,.4); background: linear-gradient(180deg, rgba(255,122,60,.06), transparent 70%), var(--bg-card); }
  .stat-card .ico {
    width: 44px; height: 44px; border-radius: 50%;
    display: grid; place-items: center; margin-bottom: 1.5rem;
  }
  .stat-card.c-orange .ico { background: var(--accent-soft); color: var(--accent); }
  .stat-card.c-blue   .ico { background: rgba(56,189,248,.14); color: var(--blue); }
  .stat-card.c-pink   .ico { background: rgba(236,72,153,.14); color: var(--pink); }
  .stat-card.c-purple .ico { background: rgba(167,139,250,.14); color: var(--purple); }
  .stat-card .num { font-size: 56px; font-weight: 700; letter-spacing: -.03em; line-height: 1; }
  .stat-card.c-orange .num { color: var(--accent); }
  .stat-card .lbl {
    font-family: 'Geist Mono', monospace; font-size: 12px;
    color: var(--text2); text-transform: uppercase; letter-spacing: .12em;
    font-weight: 500; margin-top: 1rem;
  }

  .actions { display: flex; gap: .65rem; flex-wrap: wrap; margin-bottom: 3rem; }
  .actions a {
    padding: .8rem 1.4rem; border-radius: 9px; font-size: 14px; font-weight: 600;
    display: inline-flex; align-items: center; gap: .4rem; transition: all .12s;
  }
  .actions .btn-primary { background: var(--accent); color: #fff; }
  .actions .btn-primary:hover { background: #ff8b4f; }
  .actions .btn-ghost {
    background: var(--bg-card); border: 1px solid var(--line); color: var(--text);
  }
  .actions .btn-ghost:hover { border-color: var(--line-strong); }

  .form-row { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
  .form-row.full { grid-template-columns: 1fr; }
  .form-row label {
    display: block; font-size: 11px; color: var(--text2); margin: 0 0 .35rem;
    text-transform: uppercase; letter-spacing: .06em; font-weight: 600;
  }
  .form-row input, .form-row select {
    width: 100%; background: rgba(0,0,0,.3); color: var(--text);
    border: 1px solid var(--line-strong); border-radius: 9px;
    padding: .7rem .9rem; font: inherit; font-size: 14px;
  }
  .form-row input:focus { outline: none; border-color: var(--accent); box-shadow: 0 0 0 3px rgba(255,122,60,.12); }
  .form-row input[disabled] { color: var(--muted); cursor: not-allowed; }
  .panel + .panel { margin-top: 1rem; }

  .danger-card { border-color: rgba(248,113,113,.25); }
  .danger-card h2 { color: var(--red); }
  .btn-danger {
    background: rgba(248,113,113,.14); color: var(--red);
    border: 1px solid rgba(248,113,113,.3); padding: .65rem 1.2rem;
    border-radius: 8px; font: inherit; font-size: 13.5px; font-weight: 600;
    cursor: pointer; text-decoration: none; display: inline-block;
  }
  .btn-danger:hover { background: rgba(248,113,113,.22); }

  @media (max-width: 900px) {
    .layout { grid-template-columns: 1fr; }
    aside.sidebar { display: none; }
  }
</style>
</head>
<body>

{% with messages = get_flashed_messages() %}
  {% for m in messages %}<div style="background: rgba(248,113,113,.1); border: 1px solid rgba(248,113,113,.25); padding: .75rem 1rem; margin: 1rem; border-radius: 8px; color: #ffb1b1; font-size: 13px;">{{ m }}</div>{% endfor %}
{% endwith %}

<div class="layout">
  <aside class="sidebar">
    <a href="/" class="side-brand">
      <span class="x-mark">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" width="100%" height="100%">
          <line x1="18" y1="6" x2="6" y2="18"/>
          <line x1="6" y1="6" x2="18" y2="18"/>
        </svg>
      </span>
      <span class="name">ShopifySift</span>
    </a>
    <nav class="side-nav">
      <a href="/dashboard" class="{{ 'active' if active=='dashboard' else '' }}">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>
        Dashboard
      </a>
      <a href="/app/dork" class="{{ 'active' if active=='search' else '' }}">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/></svg>
        Search
      </a>
      <a href="/app/leads" class="{{ 'active' if active=='leads' else '' }}">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>
        Leads
      </a>
      <a href="/app/history" class="{{ 'active' if active=='history' else '' }}">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
        History
      </a>
      <a href="/pricing" class="{{ 'active' if active=='credits' else '' }}">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>
        Credits
      </a>
      <a href="/pricing" class="{{ 'active' if active=='pricing' else '' }}">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M20.59 13.41 13.42 20.58a2 2 0 0 1-2.83 0L2 12V2h10l8.59 8.59a2 2 0 0 1 0 2.82z"/><line x1="7" y1="7" x2="7.01" y2="7"/></svg>
        Pricing
      </a>
      <a href="/app/settings" class="{{ 'active' if active=='settings' else '' }}">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>
        Settings
      </a>
    </nav>
    <div class="side-spacer"></div>
    <div class="credit-widget">
      <div class="top">
        <div class="num">{{ user.credits }}</div>
        <span class="bolt"><svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg></span>
      </div>
      <div class="lbl">Credits remaining</div>
      <div class="progress"><span style="width: {{ (user.credits / 100 * 100)|int if user.credits < 100 else 100 }}%"></span></div>
      <div class="ctxt">
        <div class="head">Need more credits?</div>
        <div class="sub">Get more searches and unlock more leads.</div>
        <a href="/pricing" class="upgrade">Upgrade now</a>
      </div>
    </div>
    <a href="/logout" class="side-logout">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg>
      Log out
    </a>
  </aside>

  <main class="main">
    <header class="topbar">
      <div class="search">
        <span class="ico-l"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/></svg></span>
        <input type="text" placeholder="Search..." />
        <kbd>⌘K</kbd>
      </div>
      <a href="/pricing" class="pricing-link">Pricing</a>
      <span class="credit-pill">{{ user.credits }} credits</span>
      <div class="user-chip">
        <div class="avi">{{ (user.email or 'U')[:2]|upper }}</div>
        <span class="chev"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="6 9 12 15 18 9"/></svg></span>
        <div class="menu">
          <a href="/dashboard">Dashboard</a>
          <a href="/app/dork">Search</a>
          <a href="/app/settings">Settings</a>
          <a href="/pricing">Get more credits</a>
          <a href="/logout">Log out</a>
        </div>
      </div>
    </header>

    {{ body|safe }}
  </main>
</div>

</body>
</html>
"""

DASHBOARD_BODY = r"""
<div class="page-eyebrow">// Dashboard</div>
<h1 class="page-title">Welcome back.</h1>
<p class="page-meta">{{ user.email }} · joined {{ user.created_at[:10] }}</p>

<section class="stats">
  <div class="stat-card c-orange featured">
    <div class="ico"><svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg></div>
    <div class="num">{{ user.credits }}</div>
    <div class="lbl">Credits Remaining</div>
  </div>
  <div class="stat-card c-blue">
    <div class="ico"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"><circle cx="11" cy="11" r="7"/><path d="m21 21-4.3-4.3"/></svg></div>
    <div class="num">{{ stats.searches }}</div>
    <div class="lbl">Searches Run</div>
  </div>
  <div class="stat-card c-pink">
    <div class="ico"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"><circle cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="5"/><circle cx="12" cy="12" r="1.5" fill="currentColor"/></svg></div>
    <div class="num">{{ stats.total_active }}</div>
    <div class="lbl">Active Leads Found</div>
  </div>
  <div class="stat-card c-purple">
    <div class="ico"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"><path d="M3 3v18h18"/><rect x="7" y="13" width="3" height="6" fill="currentColor" stroke="none"/><rect x="12" y="9" width="3" height="10" fill="currentColor" stroke="none"/><rect x="17" y="5" width="3" height="14" fill="currentColor" stroke="none"/></svg></div>
    <div class="num">{{ stats.total_found }}</div>
    <div class="lbl">Total Candidates</div>
  </div>
</section>

<div class="actions">
  <a href="/app/dork" class="btn-primary">Run a search →</a>
  <a href="/pricing" class="btn-ghost">Get more credits</a>
  <a href="/" class="btn-ghost">Back to home</a>
</div>

<div class="page-eyebrow" style="margin-top: 2rem;">// Recent searches</div>
{% if history %}
  <table class="t">
    <thead><tr><th>When</th><th>Keywords</th><th>Engine</th><th>Active</th><th>Total</th></tr></thead>
    <tbody>
    {% for s in history[:5] %}
      <tr>
        <td style="color:var(--muted)">{{ s.created_at[:16] }}</td>
        <td class="kw">{{ s.keywords or '(broad)' }}</td>
        <td style="color:var(--muted)">{{ s.engine }}</td>
        <td><span class="num-active">{{ s.active_leads }}</span></td>
        <td>{{ s.leads_found }}</td>
      </tr>
    {% endfor %}
    </tbody>
  </table>
{% else %}
  <div class="empty-card">
    <div class="ico"><svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/></svg></div>
    <p>No searches yet.</p>
    <a class="cta" href="/app/dork">Run your first one →</a>
  </div>
{% endif %}
"""

LEADS_BODY = r"""
<style>
  .lead-row {
    display: grid; grid-template-columns: auto 1fr auto; gap: 1rem; align-items: center;
    padding: .85rem 1.1rem; background: var(--bg-card);
    border: 1px solid var(--line); border-radius: 12px;
    transition: border-color .15s;
  }
  .lead-row + .lead-row { margin-top: .55rem; }
  .lead-row.active { border-left: 3px solid var(--green); }
  .lead-row.dormant { opacity: .55; }
  .lead-row:hover { border-color: var(--line-strong); }
  .lead-row .avi {
    width: 40px; height: 40px; border-radius: 50%;
    background: linear-gradient(135deg, var(--accent), #fbbf24);
    color: #0a0a0a; display: grid; place-items: center;
    font-weight: 700; font-size: 14px; flex-shrink: 0;
  }
  .lead-row .top {
    display: flex; gap: .5rem; align-items: center; flex-wrap: wrap; margin-bottom: .15rem;
  }
  .lead-row .h { font-weight: 600; color: var(--accent); font-size: 14.5px; }
  .lead-row .h:hover { text-decoration: underline; }
  .lead-row .badge {
    font-family: 'Geist Mono', monospace; font-size: 10px;
    padding: .15rem .5rem; border-radius: 999px;
    text-transform: uppercase; letter-spacing: .05em; font-weight: 700;
  }
  .lead-row .badge.active { background: rgba(74,222,128,.14); color: var(--green); border: 1px solid rgba(74,222,128,.28); }
  .lead-row .badge.dormant { background: rgba(255,255,255,.04); color: var(--muted); border: 1px solid var(--line); }
  .lead-row .badge.newb { background: rgba(255,122,60,.14); color: var(--accent); border: 1px solid rgba(255,122,60,.28); }
  .lead-row .badge.est { background: rgba(245,210,140,.14); color: #e8c181; border: 1px solid rgba(245,210,140,.28); }
  .lead-row .bio { font-size: 12.5px; color: var(--text2); margin-top: .15rem; line-height: 1.5;
    overflow: hidden; text-overflow: ellipsis; display: -webkit-box;
    -webkit-line-clamp: 2; -webkit-box-orient: vertical; }
  .lead-row .url { font: 11.5px ui-monospace, monospace; color: var(--muted); margin-top: .25rem; }
  .lead-row .copy-btn {
    background: rgba(255,255,255,.05); color: var(--muted); border: 1px solid var(--line);
    border-radius: 6px; font-size: 11.5px; padding: .35rem .65rem; cursor: pointer;
    font-family: inherit; transition: all .12s; flex-shrink: 0;
  }
  .lead-row .copy-btn:hover { color: var(--accent); border-color: rgba(255,122,60,.3); }
  .lead-row .copy-btn.copied { color: var(--green); border-color: var(--green); }
  .filter-pills {
    display: inline-flex; gap: .25rem; padding: .25rem;
    background: var(--bg-card); border: 1px solid var(--line);
    border-radius: 999px; margin-bottom: 1.5rem;
  }
  .filter-pills button {
    background: transparent; color: var(--text2); border: 0;
    padding: .45rem .9rem; font: inherit; font-size: 12.5px; font-weight: 500;
    border-radius: 999px; cursor: pointer; transition: all .15s;
  }
  .filter-pills button.on { background: rgba(255,255,255,.08); color: var(--text); }
</style>

<div class="page-eyebrow">// Leads</div>
<h1 class="page-title">Your leads.</h1>
<p class="page-meta">{{ stats.total_active }} active · {{ stats.total_found }} total candidates across {{ stats.searches }} search{{ '' if stats.searches == 1 else 'es' }}</p>

<section class="stats">
  <div class="stat-card c-pink featured">
    <div class="ico"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"><circle cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="5"/><circle cx="12" cy="12" r="1.5" fill="currentColor"/></svg></div>
    <div class="num">{{ active_count }}</div>
    <div class="lbl">Active (saved)</div>
  </div>
  <div class="stat-card c-purple">
    <div class="ico"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"><path d="M3 3v18h18"/><rect x="7" y="13" width="3" height="6" fill="currentColor" stroke="none"/><rect x="12" y="9" width="3" height="10" fill="currentColor" stroke="none"/><rect x="17" y="5" width="3" height="14" fill="currentColor" stroke="none"/></svg></div>
    <div class="num">{{ leads|length }}</div>
    <div class="lbl">Total saved</div>
  </div>
  <div class="stat-card c-blue">
    <div class="ico"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"><circle cx="11" cy="11" r="7"/><path d="m21 21-4.3-4.3"/></svg></div>
    <div class="num">{{ stats.searches }}</div>
    <div class="lbl">Searches run</div>
  </div>
  <div class="stat-card c-orange">
    <div class="ico"><svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg></div>
    <div class="num">{{ user.credits }}</div>
    <div class="lbl">Credits left</div>
  </div>
</section>

{% if leads %}
  <div style="display: flex; gap: 1rem; align-items: center; margin-bottom: 1rem; flex-wrap: wrap;">
    <div class="filter-pills" id="leadPills">
      <button type="button" class="on" data-filter="active">Active ({{ active_count }})</button>
      <button type="button" data-filter="all">All ({{ leads|length }})</button>
    </div>
    <button id="copyAllBtn" class="btn-ghost" style="font-size: 13px; padding: .5rem 1rem; border-radius: 8px; cursor: pointer; background: var(--bg-card); border: 1px solid var(--line); color: var(--text);">Copy all visible @handles</button>
    <a href="/app/dork" class="btn-primary" style="margin-left: auto; padding: .5rem 1rem; border-radius: 8px; font-size: 13px; font-weight: 600; color: #fff; background: var(--accent); text-decoration: none;">Run a new search →</a>
  </div>
  <div id="leadsList">
    {% for ld in leads %}
      <div class="lead-row {{ 'active' if ld.is_active else 'dormant' }}"
           data-active="{{ '1' if ld.is_active else '0' }}"
           data-text="{{ (ld.username + ' ' + (ld.bio_snippet or ''))|lower }}">
        <div class="avi">{{ (ld.username or '?')[:1]|upper }}</div>
        <div>
          <div class="top">
            <a class="h" href="{{ ld.x_profile or 'https://x.com/' + ld.username }}" target="_blank" rel="noopener">@{{ ld.username }}</a>
            <button class="copy-btn" data-copy="@{{ ld.username }}">copy</button>
            {% if ld.is_active %}
              <span class="badge active">● active</span>
            {% elif ld.is_shopify %}
              <span class="badge dormant">{{ ld.active_reason or 'dormant' }}</span>
            {% endif %}
            {% if ld.category %}
              <span class="badge {{ 'newb' if ld.category == 'newb' else 'est' }}">{{ ld.category }}</span>
            {% endif %}
          </div>
          <div class="bio">{{ ld.bio_snippet }}</div>
          {% if ld.shopify_url %}
            <div class="url"><a href="{{ ld.shopify_url }}" target="_blank" rel="noopener" style="color:inherit;">{{ ld.shopify_url }}</a></div>
          {% endif %}
        </div>
      </div>
    {% endfor %}
  </div>

  <script>
    document.querySelectorAll('.lead-row .copy-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        navigator.clipboard.writeText(btn.dataset.copy);
        btn.classList.add('copied'); btn.textContent = 'copied';
        setTimeout(() => { btn.classList.remove('copied'); btn.textContent = 'copy'; }, 1100);
      });
    });
    document.getElementById('leadPills').addEventListener('click', e => {
      if (e.target.tagName !== 'BUTTON') return;
      document.querySelectorAll('#leadPills button').forEach(b => b.classList.remove('on'));
      e.target.classList.add('on');
      const filter = e.target.dataset.filter;
      document.querySelectorAll('#leadsList .lead-row').forEach(r => {
        const a = r.dataset.active === '1';
        r.style.display = (filter === 'all' || (filter === 'active' && a)) ? '' : 'none';
      });
    });
    document.getElementById('copyAllBtn').addEventListener('click', () => {
      const visible = [...document.querySelectorAll('#leadsList .lead-row')].filter(r => r.style.display !== 'none');
      const handles = visible.map(r => '@' + r.dataset.text.split(' ')[0]).join('\n');
      navigator.clipboard.writeText(handles);
      const btn = document.getElementById('copyAllBtn');
      const orig = btn.textContent;
      btn.textContent = `Copied ${visible.length} handles ✓`;
      setTimeout(() => btn.textContent = orig, 1300);
    });
  </script>
{% else %}
  <div class="empty-card">
    <div class="ico"><svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg></div>
    <p>No leads yet.</p>
    <p style="font-size: 13px; color: var(--text2); font-weight: 400; margin: .25rem 0 1rem;">Run a search and verified leads will appear here automatically — they're now saved across tabs.</p>
    <a class="cta" href="/app/dork">Run your first search →</a>
  </div>
{% endif %}
"""

HISTORY_BODY = r"""
<div class="page-eyebrow">// Search history</div>
<h1 class="page-title">Search history.</h1>
<p class="page-meta">Every search you've run on this account</p>

{% if history %}
  <table class="t">
    <thead><tr><th>When</th><th>Keywords</th><th>Engine</th><th>Broad</th><th>Active</th><th>Total</th></tr></thead>
    <tbody>
    {% for s in history %}
      <tr>
        <td style="color:var(--muted)">{{ s.created_at[:16] }}</td>
        <td class="kw">{{ s.keywords or '(broad)' }}</td>
        <td style="color:var(--muted)">{{ s.engine }}</td>
        <td style="color:var(--muted)">{{ 'yes' if s.broad else 'no' }}</td>
        <td><span class="num-active">{{ s.active_leads }}</span></td>
        <td>{{ s.leads_found }}</td>
      </tr>
    {% endfor %}
    </tbody>
  </table>
{% else %}
  <div class="empty-card">
    <div class="ico"><svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg></div>
    <p>No searches yet.</p>
    <a class="cta" href="/app/dork">Run your first one →</a>
  </div>
{% endif %}
"""

SETTINGS_BODY = r"""
<div class="page-eyebrow">// Settings</div>
<h1 class="page-title">Account settings.</h1>
<p class="page-meta">Manage your account and preferences</p>

<div class="panel">
  <h2>Account</h2>
  <p class="sub">Your basic info. Email is your unique identifier.</p>
  <div class="form-row full" style="margin-bottom: 1rem;">
    <label>Email</label>
    <input type="email" value="{{ user.email }}" disabled>
  </div>
  <div class="form-row">
    <div>
      <label>Member since</label>
      <input type="text" value="{{ user.created_at[:10] }}" disabled>
    </div>
    <div>
      <label>User ID</label>
      <input type="text" value="#{{ user.id }}" disabled>
    </div>
  </div>
</div>

<div class="panel">
  <h2>Plan & credits</h2>
  <p class="sub">{{ user.credits }} credits remaining on the {{ user.plan|capitalize }} plan.</p>
  <div class="actions" style="margin: 0;">
    <a href="/pricing" class="btn-primary">Get more credits</a>
    <a href="/app/history" class="btn-ghost">Usage history</a>
  </div>
</div>

<div class="panel danger-card">
  <h2>Danger zone</h2>
  <p class="sub">Permanently delete your account and all associated data (search history, leads, credits). This cannot be undone.</p>
  <form method="post" action="/account/delete" style="display: flex; gap: .65rem; align-items: center; flex-wrap: wrap;"
        onsubmit="return confirm('Permanently delete your account? This cannot be undone.');">
    <input type="text" name="confirm" placeholder='Type "delete my account" to confirm'
           style="flex: 1; min-width: 280px; background: rgba(0,0,0,.3); color: var(--text); border: 1px solid var(--line-strong); border-radius: 9px; padding: .7rem .9rem; font: inherit; font-size: 14px;" required>
    <button type="submit" class="btn-danger" style="background: rgba(248,113,113,.18); border-color: rgba(248,113,113,.4);">Delete forever</button>
  </form>
  <p style="margin: 1rem 0 0; font-size: 13px; color: var(--text2);">
    Or just <a href="/logout" style="color:var(--accent);">log out</a>.
  </p>
</div>
"""

PRICING_PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pricing · ShopifySift</title>
""" + SHARED_STYLE + r"""
<style>
  .pricing-wrap { max-width: 1080px; margin: 4rem auto 5rem; padding: 0 1.5rem; }
  .pricing-eyebrow {
    font-family: 'Geist Mono', monospace; font-size: 12px;
    color: var(--accent); text-transform: uppercase; letter-spacing: .08em;
    text-align: center; font-weight: 600; margin-bottom: 1rem;
  }
  .pricing-wrap h1 {
    font-size: clamp(36px, 5vw, 52px); text-align: center;
    margin: 0 0 .75rem; letter-spacing: -.025em; line-height: 1.05; font-weight: 600;
  }
  .pricing-wrap p.lede {
    text-align: center; color: var(--text2); max-width: 560px;
    margin: 0 auto 3rem; font-size: 16px;
  }
  .plans { display: grid; grid-template-columns: repeat(3, 1fr); gap: 1.25rem; }
  @media (max-width: 900px) { .plans { grid-template-columns: 1fr; } }
  .plan {
    background: var(--bg-card); border: 1px solid var(--line);
    border-radius: 14px; padding: 2rem; position: relative; display: flex; flex-direction: column;
  }
  .plan.featured { border: 2px solid var(--accent); background: linear-gradient(180deg, rgba(255,122,60,.06), transparent 50%), var(--bg-card); }
  .plan .pp {
    font-family: 'Geist Mono', monospace; font-size: 11px; color: var(--text2);
    text-transform: uppercase; letter-spacing: .08em; margin-bottom: .5rem; font-weight: 600;
  }
  .plan.featured .pp { color: var(--accent); }
  .plan h3 { font-size: 22px; margin: 0 0 .35rem; font-weight: 600; letter-spacing: -.015em; }
  .plan .price { font-size: 44px; font-weight: 700; letter-spacing: -.025em; line-height: 1; margin-bottom: .25rem; }
  .plan .price small { font-size: 14px; color: var(--text2); font-weight: 400; }
  .plan .ppc { font-size: 12px; color: var(--text2); margin-bottom: 1.25rem; font-family: 'Geist Mono', monospace; }
  .plan .tagline { color: var(--text2); font-size: 14px; margin-bottom: 1.5rem; line-height: 1.5; }
  .plan ul { list-style: none; padding: 0; margin: 0 0 1.5rem; flex: 1; }
  .plan li {
    font-size: 14px; padding: .4rem 0; color: var(--text2);
    display: flex; gap: .55rem; align-items: center;
  }
  .plan li::before { content: "✓"; color: var(--accent); font-weight: 700; }
  .plan .buy-btn {
    width: 100%; padding: .85rem; font: inherit; font-weight: 600; cursor: pointer;
    background: var(--bg-card); color: var(--text); border: 1px solid var(--line-strong);
    border-radius: 9px; font-size: 14.5px; text-decoration: none; text-align: center;
    display: block; transition: all .12s;
  }
  .plan .buy-btn:hover { border-color: var(--text2); }
  .plan.featured .buy-btn { background: var(--accent); color: #fff; border-color: var(--accent); }
  .plan.featured .buy-btn:hover { background: var(--accent-hover); border-color: var(--accent-hover); }
  .featured-tag {
    position: absolute; top: -10px; left: 50%; transform: translateX(-50%);
    background: var(--accent); color: #fff; padding: .25rem .8rem;
    font-family: 'Geist Mono', monospace; font-size: 11px;
    text-transform: uppercase; letter-spacing: .06em; font-weight: 600;
    border-radius: 4px;
  }
  .best-tag { position: absolute; top: -10px; right: 1.25rem;
    background: var(--bg); color: var(--text2);
    border: 1px solid var(--line-strong);
    padding: .25rem .65rem; font-family: 'Geist Mono', monospace; font-size: 10.5px;
    text-transform: uppercase; letter-spacing: .06em; border-radius: 4px;
  }
  .pricing-meta {
    text-align: center; margin-top: 3rem; color: var(--text2); font-size: 13.5px;
  }
  .pricing-meta a { color: var(--accent); }
</style>
</head>
<body>
""" + NAV + r"""
{% with messages = get_flashed_messages() %}
  {% for m in messages %}<div class="flash">{{ m }}</div>{% endfor %}
{% endwith %}
<div class="pricing-wrap">
  <div class="pricing-eyebrow">// Buy credits</div>
  <h1>Pay only for searches you run.</h1>
  <p class="lede">Credits never expire. Buy a pack, run searches at your pace. No subscriptions, no auto-renewals.</p>
  <div class="plans">
    {% for plan in plans %}
      <div class="plan {{ 'featured' if plan.featured else '' }}">
        {% if plan.featured %}<div class="featured-tag">Most popular</div>{% endif %}
        {% if plan.best_value %}<div class="best-tag">Best value</div>{% endif %}
        <div class="pp">{{ plan.name }}</div>
        <h3>{{ plan.credits }} searches</h3>
        <div class="price">${{ plan.price }}<small> one-time</small></div>
        <div class="ppc">${{ '%.2f'|format(plan.price / plan.credits) }} per search</div>
        <p class="tagline">{{ plan.tagline }}</p>
        <ul>
          {% for f in plan.features %}<li>{{ f }}</li>{% endfor %}
        </ul>
        <a href="/checkout?plan={{ plan.id }}" class="buy-btn">Buy {{ plan.name }} →</a>
      </div>
    {% endfor %}
  </div>
  <p class="pricing-meta">
    Need something custom? Higher volume? <a href="mailto:hi@shopifysift.app">Email us</a> for enterprise pricing.
  </p>
</div>
</body>
</html>
"""

CHECKOUT_PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Checkout · ShopifySift</title>
""" + SHARED_STYLE + r"""
<style>
  .checkout-wrap { max-width: 980px; margin: 3rem auto 5rem; padding: 0 1.5rem; }
  .ck-eyebrow {
    font-family: 'Geist Mono', monospace; font-size: 12px;
    color: var(--accent); text-transform: uppercase; letter-spacing: .08em;
    margin-bottom: .75rem; font-weight: 600;
  }
  .checkout-wrap h1 {
    font-size: clamp(28px, 4vw, 36px); margin: 0 0 .5rem;
    letter-spacing: -.025em; line-height: 1.1; font-weight: 600;
  }
  .checkout-wrap > p.sub { color: var(--text2); margin: 0 0 2.5rem; font-size: 15px; }
  .ck-grid { display: grid; grid-template-columns: 1.4fr 1fr; gap: 2rem; align-items: start; }
  @media (max-width: 800px) { .ck-grid { grid-template-columns: 1fr; } }
  .ck-card {
    background: var(--bg-card); border: 1px solid var(--line);
    border-radius: 14px; padding: 2rem;
  }
  .ck-card h2 {
    font-size: 16px; margin: 0 0 1.5rem; font-weight: 600;
    display: flex; align-items: center; gap: .55rem;
  }
  .ck-section + .ck-section { margin-top: 2rem; padding-top: 2rem; border-top: 1px solid var(--line); }
  .ck-row { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-bottom: 1rem; }
  .ck-row > * { min-width: 0; }
  .ck-row.full { grid-template-columns: 1fr; }
  .ck-row label { display: block; font-size: 11px; color: var(--text2); margin: 0 0 .35rem;
    text-transform: uppercase; letter-spacing: .06em; font-weight: 600; }
  .ck-row input {
    width: 100%; background: rgba(0,0,0,.35); color: var(--text);
    border: 1px solid var(--line-strong); border-radius: 9px;
    padding: .75rem .9rem; font: inherit; font-size: 14.5px; transition: border-color .12s;
  }
  .ck-row input:focus { outline: none; border-color: var(--accent); box-shadow: 0 0 0 3px rgba(255,122,60,.12); }
  .ck-row input[disabled] { color: var(--muted); cursor: not-allowed; }

  .stripe-shim {
    display: flex; align-items: center; gap: .65rem;
    background: var(--accent-soft); border: 1px solid rgba(255,122,60,.25);
    border-radius: 10px; padding: .85rem 1rem; margin-bottom: 1.5rem;
    color: var(--accent); font-size: 13px;
  }

  .ck-summary {
    background: var(--bg-card); border: 1px solid var(--line);
    border-radius: 14px; padding: 2rem; position: sticky; top: 1.5rem;
  }
  .ck-summary h3 { font-size: 14px; margin: 0 0 1.25rem; text-transform: uppercase;
    letter-spacing: .08em; color: var(--text2); font-family: 'Geist Mono', monospace; font-weight: 600; }
  .ck-line {
    display: flex; justify-content: space-between; align-items: baseline;
    padding: .55rem 0; font-size: 14px;
  }
  .ck-line .lhs { color: var(--text2); }
  .ck-line .rhs { color: var(--text); font-weight: 500; }
  .ck-line.major { padding-top: 1rem; margin-top: 1rem; border-top: 1px solid var(--line);
    font-size: 18px; }
  .ck-line.major .lhs { color: var(--text); font-weight: 600; }
  .ck-line.major .rhs { color: var(--accent); font-weight: 700; font-size: 22px; letter-spacing: -.02em; }

  .pay-btn {
    width: 100%; padding: 1rem; font: inherit; font-weight: 700; cursor: pointer;
    background: var(--accent); color: #fff; border: 1px solid var(--accent);
    border-radius: 10px; font-size: 15px; margin-top: 1.5rem; transition: all .12s;
    display: flex; align-items: center; justify-content: center; gap: .5rem;
  }
  .pay-btn:hover { background: var(--accent-hover); transform: translateY(-1px); }

  .ck-secure {
    display: flex; align-items: center; justify-content: center; gap: .4rem;
    margin-top: 1rem; font-size: 11.5px; color: var(--muted);
  }

  .ck-back {
    color: var(--text2); font-size: 13px; margin-bottom: 1rem; display: inline-flex;
    align-items: center; gap: .35rem;
  }
  .ck-back:hover { color: var(--text); }
</style>
</head>
<body>
""" + NAV + r"""
{% with messages = get_flashed_messages() %}
  {% for m in messages %}<div class="flash">{{ m }}</div>{% endfor %}
{% endwith %}

<div class="checkout-wrap">
  <a class="ck-back" href="/pricing">← Back to plans</a>
  <div class="ck-eyebrow">// Checkout</div>
  <h1>Complete your purchase</h1>
  <p class="sub">Logged in as <strong>{{ user.email }}</strong>. Credits will be added to your account.</p>

  <form method="post" action="/checkout/confirm">
    <input type="hidden" name="plan" value="{{ plan.id }}">
    <div class="ck-grid">
      <div class="ck-card">
        <div class="stripe-shim">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/></svg>
          <span><strong>Test mode</strong> · Stripe is being wired up. For now, click <em>Complete purchase</em> and credits will be added to your account immediately.</span>
        </div>

        <div class="ck-section">
          <h2>
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>
            Contact
          </h2>
          <div class="ck-row full">
            <label>Email</label>
            <input type="email" value="{{ user.email }}" readonly>
          </div>
        </div>

        <div class="ck-section">
          <h2>
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="5" width="20" height="14" rx="2"/><line x1="2" y1="10" x2="22" y2="10"/></svg>
            Card details
          </h2>
          <div class="ck-row full">
            <label>Card number</label>
            <input type="text" placeholder="4242 4242 4242 4242 — Stripe coming soon" disabled>
          </div>
          <div class="ck-row">
            <div>
              <label>Expiry</label>
              <input type="text" placeholder="MM / YY" disabled>
            </div>
            <div>
              <label>CVC</label>
              <input type="text" placeholder="123" disabled>
            </div>
          </div>
          <div class="ck-row full">
            <label>Cardholder name</label>
            <input type="text" placeholder="Full name as on card" disabled>
          </div>
        </div>
      </div>

      <div class="ck-summary">
        <h3>Order summary</h3>
        <div class="ck-line">
          <span class="lhs">{{ plan.name }} pack</span>
          <span class="rhs">${{ plan.price }}.00</span>
        </div>
        <div class="ck-line">
          <span class="lhs">Credits added</span>
          <span class="rhs">{{ plan.credits }} searches</span>
        </div>
        <div class="ck-line">
          <span class="lhs">Tax</span>
          <span class="rhs">$0.00</span>
        </div>
        <div class="ck-line major">
          <span class="lhs">Total</span>
          <span class="rhs">${{ plan.price }}.00</span>
        </div>
        <button type="submit" class="pay-btn">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>
          Complete purchase
        </button>
        <div class="ck-secure">
          <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>
          Powered by Stripe (coming soon)
        </div>
      </div>
    </div>
  </form>
</div>
</body>
</html>
"""

CHECKOUT_SUCCESS_PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Success · ShopifySift</title>
""" + SHARED_STYLE + r"""
<style>
  .success-wrap { max-width: 540px; margin: 6rem auto; padding: 0 1.5rem; text-align: center; }
  .success-icon {
    width: 72px; height: 72px; margin: 0 auto 1.5rem;
    border-radius: 50%; background: var(--accent-soft);
    border: 1px solid rgba(255,122,60,.3);
    display: grid; place-items: center; color: var(--accent);
  }
  .success-wrap h1 {
    font-size: clamp(28px, 4vw, 36px); margin: 0 0 .75rem;
    letter-spacing: -.025em; font-weight: 600;
  }
  .success-wrap > p { color: var(--text2); font-size: 15px; margin: 0 0 2rem; line-height: 1.55; }
  .receipt {
    background: var(--bg-card); border: 1px solid var(--line);
    border-radius: 12px; padding: 1.5rem; margin: 0 auto 2rem;
    text-align: left;
  }
  .receipt-row {
    display: flex; justify-content: space-between; padding: .55rem 0; font-size: 14px;
  }
  .receipt-row + .receipt-row { border-top: 1px solid var(--line); }
  .receipt-row .lhs { color: var(--text2); }
  .receipt-row .rhs { color: var(--text); font-weight: 500; }
  .receipt-row.total .rhs { color: var(--accent); font-weight: 700; font-family: 'Geist Mono', monospace; }
  .success-actions { display: flex; gap: .75rem; justify-content: center; flex-wrap: wrap; }
  .success-actions a {
    padding: .85rem 1.5rem; font-size: 14px; font-weight: 600;
    border-radius: 8px; transition: all .12s; text-decoration: none;
  }
  .success-actions .primary {
    background: var(--accent); color: #fff;
  }
  .success-actions .primary:hover { background: var(--accent-hover); }
  .success-actions .ghost {
    background: transparent; color: var(--text); border: 1px solid var(--line-strong);
  }
  .success-actions .ghost:hover { border-color: var(--text2); }
</style>
</head>
<body>
""" + NAV + r"""
<div class="success-wrap">
  <div class="success-icon">
    <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg>
  </div>
  <h1>Credits added.</h1>
  <p>Your account has been credited with <strong>{{ plan.credits }} searches</strong>. Time to sift some bios.</p>
  <div class="receipt">
    <div class="receipt-row">
      <span class="lhs">Plan</span><span class="rhs">{{ plan.name }} ({{ plan.credits }} credits)</span>
    </div>
    <div class="receipt-row">
      <span class="lhs">Email</span><span class="rhs">{{ user.email }}</span>
    </div>
    <div class="receipt-row">
      <span class="lhs">New balance</span><span class="rhs">{{ user.credits }} credits</span>
    </div>
    <div class="receipt-row total">
      <span class="lhs">Total charged</span><span class="rhs">${{ plan.price }}.00 (test mode)</span>
    </div>
  </div>
  <div class="success-actions">
    <a href="/app/dork" class="primary">Run a search →</a>
    <a href="/dashboard" class="ghost">Back to dashboard</a>
  </div>
</div>
</body>
</html>
"""


PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ShopifySift · Sift X bios</title>
<link rel="icon" type="image/svg+xml" href="/static/favicon.svg">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Geist:wght@400;500;600;700&family=Geist+Mono:wght@400;500&display=swap">
<style>
  :root {
    --bg: #16181c;
    --bg2: #1f2227;
    --panel: rgba(31, 34, 39, 0.6);
    --panel-solid: #1f2227;
    --panel2: #25282e;
    --line: rgba(255, 255, 255, 0.07);
    --line-strong: rgba(255, 255, 255, 0.14);
    --text: #f5f0eb;
    --text2: #c5beb6;
    --muted: #8a847d;
    --accent: #ff7a3c;
    --accent2: #ff7a3c;
    --green: #4ade80;
    --orange: #ff7a3c;
    --red: #f87171;
    --gradient: linear-gradient(135deg, #ff7a3c 0%, #ff9b66 100%);
    --gradient-soft: linear-gradient(135deg, rgba(255,122,60,.1), rgba(255,155,102,.04));
    color-scheme: dark;
  }
  * { box-sizing: border-box; }
  html, body { margin: 0; }
  body {
    font-family: 'Geist', -apple-system, BlinkMacSystemFont, system-ui, sans-serif;
    background: var(--bg);
    background-image:
      radial-gradient(700px circle at 100% 0%, rgba(255,122,60,.06), transparent 50%);
    color: var(--text);
    min-height: 100vh;
    font-size: 14px; line-height: 1.55;
    -webkit-font-smoothing: antialiased;
  }
  .wrap { max-width: 1320px; margin: 0 auto; padding: 1.25rem 2rem 5rem; }

  header.top {
    display: flex; align-items: center; justify-content: space-between;
    padding: .25rem 0 1.5rem;
  }
  .brand { display: flex; align-items: center; gap: .65rem; }
  .logo {
    width: 32px; height: 32px; border-radius: 8px;
    background: #0a0a0a;
    display: grid; place-items: center;
    position: relative;
    box-shadow: 0 4px 16px rgba(255,122,60,.18);
  }
  .logo::before {
    content: ''; position: absolute; inset: 7px;
    border: 2px solid transparent;
    border-top-color: white; border-left-color: white;
    width: 6px; height: 6px;
  }
  .logo::after {
    content: ''; width: 8px; height: 8px;
    border-radius: 50%; background: var(--accent);
  }
  .brand h1 { font-size: 16px; margin: 0; font-weight: 600; letter-spacing: -.01em; }
  .brand p { font-size: 12px; margin: 0; color: var(--muted); }
  .proxy-pill {
    font-size: 11px; padding: .3rem .65rem; border-radius: 999px;
    background: rgba(52, 211, 153, .1); color: var(--green); border: 1px solid rgba(52,211,153,.2);
    display: inline-flex; align-items: center; gap: .35rem;
  }
  .proxy-pill.off { background: rgba(255,255,255,.04); color: var(--muted); border-color: var(--line); }
  .proxy-pill .dot {
    width: 6px; height: 6px; border-radius: 50%; background: var(--green);
    box-shadow: 0 0 8px var(--green);
    animation: pulse 2s infinite;
  }
  .proxy-pill.off .dot { background: var(--muted); box-shadow: none; animation: none; }
  @keyframes pulse { 0%,100% { opacity: 1 } 50% { opacity: .4 } }

  nav.tabs {
    display: flex; gap: .25rem; padding: .25rem;
    background: var(--panel); backdrop-filter: blur(20px);
    border: 1px solid var(--line); border-radius: 10px;
    margin-bottom: 1.75rem; width: fit-content;
  }
  nav.tabs a {
    padding: .55rem 1rem; text-decoration: none; color: var(--muted);
    font-weight: 500; font-size: 13px; border-radius: 7px;
    transition: all .15s ease;
  }
  nav.tabs a:hover { color: var(--text2); background: rgba(255,255,255,.03); }
  nav.tabs a.active {
    color: var(--text); background: rgba(255,255,255,.06);
    box-shadow: 0 1px 0 rgba(255,255,255,.04) inset;
  }

  .hero {
    background: var(--bg-card); border: 1px solid var(--line);
    border-radius: 16px; padding: 2.5rem 2.75rem; position: relative; overflow: hidden;
    background-image: radial-gradient(900px circle at 90% -20%, rgba(255,122,60,.07), transparent 50%);
  }
  .hero > * { position: relative; }
  .hero .hero-eyebrow {
    font-family: 'Geist Mono', monospace; font-size: 12px;
    color: var(--accent); text-transform: uppercase; letter-spacing: .08em;
    font-weight: 600; margin-bottom: .9rem;
  }
  .hero h2 {
    margin: 0 0 .5rem; font-size: clamp(28px, 3.5vw, 40px);
    font-weight: 600; letter-spacing: -.025em; line-height: 1.1;
  }
  .hero p.lead { margin: 0 0 2rem; color: var(--text2); font-size: 15px; max-width: 560px; line-height: 1.5; }
  .hero #kw {
    font-size: 16px; padding: 1rem 1.15rem;
  }
  .hero label { font-size: 11px; }

  label { display: block; font-size: 11px; color: var(--muted); margin: 0 0 .35rem;
    text-transform: uppercase; letter-spacing: .06em; font-weight: 600; }
  input[type=text], input[type=number], textarea {
    background: rgba(0,0,0,.25); color: var(--text);
    border: 1px solid var(--line-strong); border-radius: 9px;
    padding: .7rem .9rem; font: inherit; transition: border-color .15s, box-shadow .15s;
    width: 100%;
  }
  input[type=text]:focus, input[type=number]:focus, textarea:focus {
    outline: none; border-color: var(--accent);
    box-shadow: 0 0 0 3px rgba(255,122,60,.15);
  }
  textarea { min-height: 120px; font: 13px ui-monospace, "JetBrains Mono", monospace; resize: vertical; }
  input[type=number] { width: 5.5rem; }
  .select {
    background: rgba(0,0,0,.25); color: var(--text);
    border: 1px solid var(--line-strong); border-radius: 9px;
    padding: .65rem .85rem; font: inherit; font-size: 13.5px; cursor: pointer;
  }
  .select:focus { outline: none; border-color: var(--accent); box-shadow: 0 0 0 3px rgba(255,122,60,.15); }

  .packs { display: flex; flex-wrap: wrap; gap: .4rem; align-items: center; margin-top: .65rem; }
  .packs-label { font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: .06em; font-weight: 600; margin-right: .25rem; }
  .pack-chip {
    background: rgba(255,122,60,.08); color: var(--accent);
    border: 1px solid rgba(255,122,60,.2); border-radius: 999px;
    padding: .35rem .8rem; font: inherit; font-size: 12px; font-weight: 500;
    cursor: pointer; transition: all .12s; text-transform: capitalize;
    display: inline-flex; align-items: center; gap: .35rem;
  }
  .pack-chip:hover { background: rgba(255,122,60,.16); border-color: var(--accent); }
  .pack-count { font-size: 10.5px; opacity: .7; }
  .pack-clear { color: var(--muted); background: transparent; border-color: var(--line-strong); }
  .pack-clear:hover { color: var(--text); background: rgba(255,255,255,.04); border-color: var(--text2); }
  .controls {
    display: grid;
    grid-template-columns: minmax(180px, 1fr) minmax(120px, auto) auto auto auto;
    gap: 1rem; align-items: end; margin-top: 1.25rem;
  }
  .controls > * { min-width: 0; }
  .controls .actions-row {
    grid-column: 1 / -1;
    display: flex; gap: .75rem; align-items: center; justify-content: flex-end;
    margin-top: .5rem; padding-top: 1.25rem;
    border-top: 1px solid var(--line);
  }
  .controls .actions-row > .primary { padding: .9rem 2rem; font-size: 14.5px; }
  @media (max-width: 900px) {
    .controls { grid-template-columns: 1fr 1fr; }
    .controls .actions-row { flex-wrap: wrap; }
  }
  .toggle {
    display: flex; align-items: center; gap: .55rem; font-size: 13px; color: var(--text2);
    user-select: none; cursor: pointer; padding: .65rem .85rem;
    background: rgba(0,0,0,.25); border: 1px solid var(--line-strong); border-radius: 9px;
  }
  .toggle input { accent-color: var(--accent); width: 14px; height: 14px; }
  .toggle:has(input:checked) { border-color: var(--accent); background: rgba(255,122,60,.06); }

  button.primary {
    padding: .75rem 1.5rem; font: inherit; font-weight: 600; cursor: pointer;
    background: var(--accent); color: #fff; border: 0; border-radius: 9px;
    box-shadow: 0 4px 18px rgba(255,122,60,.28);
    transition: transform .12s, box-shadow .15s, opacity .15s;
  }
  button.primary:hover { transform: translateY(-1px); background: #ff8b4f; box-shadow: 0 6px 24px rgba(255,122,60,.4); }
  button.primary:active { transform: translateY(0); }
  button.primary:disabled { opacity: .5; cursor: not-allowed; transform: none; box-shadow: none; }

  /* stats */
  .stats {
    display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem;
    margin: 1.75rem 0 1.25rem;
  }
  .stat {
    background: var(--panel); backdrop-filter: blur(20px);
    border: 1px solid var(--line); border-radius: 12px;
    padding: 1.1rem 1.25rem; position: relative; overflow: hidden;
  }
  .stat::after {
    content: ''; position: absolute; inset: 0; pointer-events: none;
    background: linear-gradient(135deg, transparent 0%, rgba(255,255,255,.02) 100%);
  }
  .stat .icon { font-size: 18px; margin-bottom: .55rem; opacity: .9; }
  .stat .num { font-size: 28px; font-weight: 700; letter-spacing: -.02em; line-height: 1; }
  .stat.hero-stat .num { color: var(--accent); }
  .stat .lbl { font-size: 11px; color: var(--muted); text-transform: uppercase;
    letter-spacing: .06em; margin-top: .4rem; font-weight: 600; }

  /* lead toolbar */
  .toolbar {
    display: flex; align-items: center; gap: .85rem; flex-wrap: wrap;
    margin: 1.5rem 0 1rem;
  }
  .pills { display: flex; gap: .35rem; padding: .25rem;
    background: var(--panel); border: 1px solid var(--line); border-radius: 999px; }
  .pills button {
    background: transparent; color: var(--muted); border: 0;
    padding: .4rem .85rem; font: inherit; font-size: 12.5px; font-weight: 500;
    border-radius: 999px; cursor: pointer; transition: all .15s;
  }
  .pills button:hover { color: var(--text2); }
  .pills button.on { background: rgba(255,255,255,.08); color: var(--text); }
  .toolbar .grow { flex: 1; }
  .toolbar .ghost {
    background: transparent; border: 1px solid var(--line-strong);
    color: var(--text2); padding: .45rem .85rem; font: inherit; font-size: 12.5px;
    border-radius: 8px; cursor: pointer; text-decoration: none; display: inline-flex;
    align-items: center; gap: .35rem; transition: all .15s;
  }
  .toolbar .ghost:hover { background: rgba(255,255,255,.04); color: var(--text); border-color: var(--accent); }
  .search-in {
    background: rgba(0,0,0,.25); color: var(--text);
    border: 1px solid var(--line-strong); border-radius: 8px;
    padding: .45rem .75rem; font: inherit; font-size: 13px; width: 200px;
  }

  /* lead list */
  .leads { display: flex; flex-direction: column; gap: .65rem; }
  .lead {
    display: grid; grid-template-columns: auto 1fr auto; gap: 1rem; align-items: center;
    padding: 1rem 1.15rem;
    background: var(--panel); backdrop-filter: blur(12px);
    border: 1px solid var(--line); border-radius: 12px;
    transition: transform .15s, border-color .15s, background .15s;
  }
  .lead:hover {
    transform: translateY(-1px); border-color: var(--line-strong);
    background: rgba(28,34,48,.7);
  }
  .lead.hit { border-left: 3px solid var(--green); }
  .lead.dormant { opacity: .55; }
  .avatar {
    width: 42px; height: 42px; border-radius: 50%; flex-shrink: 0;
    background: var(--gradient); display: grid; place-items: center;
    font-weight: 700; color: #07080c; font-size: 16px;
  }
  .lead-main { min-width: 0; }
  .row1 { display: flex; align-items: center; gap: .55rem; flex-wrap: wrap; margin-bottom: .25rem; }
  .row1 a.handle { color: var(--text); font-weight: 600; font-size: 14.5px; text-decoration: none; }
  .row1 a.handle:hover { color: var(--accent); }
  .copy {
    background: rgba(255,255,255,.05); color: var(--muted); border: 1px solid var(--line);
    border-radius: 5px; font-size: 10.5px; padding: .15rem .5rem; cursor: pointer;
    font-family: inherit; transition: all .15s;
  }
  .copy:hover { background: rgba(255,122,60,.14); color: var(--accent); border-color: rgba(255,122,60,.3); }
  .copy.copied { color: var(--green); border-color: var(--green); }
  .badges { display: flex; gap: .3rem; flex-wrap: wrap; }
  .badge {
    font-size: 10px; padding: .15rem .55rem; border-radius: 999px;
    text-transform: uppercase; letter-spacing: .05em; font-weight: 700;
    display: inline-flex; align-items: center; gap: .25rem;
  }
  .badge.active { background: rgba(74,222,128,.14); color: var(--green); border: 1px solid rgba(74,222,128,.28); }
  .badge.dormant { background: rgba(255,255,255,.04); color: var(--muted); border: 1px solid var(--line); }
  .badge.notshop { background: rgba(248,113,113,.12); color: var(--red); border: 1px solid rgba(248,113,113,.2); }
  .badge.newb { background: rgba(255,122,60,.14); color: var(--accent); border: 1px solid rgba(255,122,60,.28); }
  .badge.est { background: rgba(245,210,140,.14); color: #e8c181; border: 1px solid rgba(245,210,140,.28); }
  .bio {
    font-size: 13px; color: var(--text2); margin-top: .15rem;
    overflow: hidden; text-overflow: ellipsis; display: -webkit-box;
    -webkit-line-clamp: 2; -webkit-box-orient: vertical; line-clamp: 2;
  }
  .url-row { font: 11.5px ui-monospace, "JetBrains Mono", monospace; color: var(--muted); margin-top: .35rem; }
  .url-row a { color: var(--accent2); text-decoration: none; }
  .url-row a:hover { text-decoration: underline; }
  .signals { font: 11px ui-monospace, monospace; color: var(--muted); text-align: right; max-width: 180px; }

  /* empty + loading */
  .empty {
    background: var(--panel); border: 1px dashed var(--line-strong); border-radius: 14px;
    padding: 3rem 2rem; text-align: center; margin-top: 1.5rem;
  }
  .empty .e-icon {
    width: 48px; height: 48px; margin: 0 auto 1rem;
    border-radius: 12px; background: var(--gradient-soft);
    display: grid; place-items: center; font-size: 22px;
  }
  .empty h3 { margin: 0 0 .35rem; font-size: 16px; font-weight: 600; }
  .empty p { margin: 0; color: var(--muted); font-size: 13px; max-width: 380px; margin: 0 auto; }

  .err {
    background: rgba(248,113,113,.08); border: 1px solid rgba(248,113,113,.2);
    border-left-width: 3px; padding: .9rem 1.1rem; border-radius: 10px;
    margin: 1.25rem 0; color: #ffb1b1; font-size: 13px;
  }
  .warn {
    background: rgba(251,146,60,.08); border: 1px solid rgba(251,146,60,.2);
    border-left-width: 3px; padding: .9rem 1.1rem; border-radius: 10px;
    margin: 1.25rem 0; color: #ffd0a8; font-size: 13px;
  }

  /* loading overlay */
  .overlay {
    position: fixed; inset: 0; background: rgba(7,8,12,.85);
    backdrop-filter: blur(8px); z-index: 100;
    display: none; align-items: center; justify-content: center; flex-direction: column;
    gap: 1.25rem; opacity: 0; transition: opacity .2s;
  }
  .overlay.on { display: flex; opacity: 1; }
  .spinner {
    width: 56px; height: 56px; border-radius: 50%;
    background: conic-gradient(from 0deg, transparent, var(--accent), var(--accent2), transparent);
    -webkit-mask: radial-gradient(circle, transparent 56%, black 58%);
    mask: radial-gradient(circle, transparent 56%, black 58%);
    animation: spin 1s linear infinite;
  }
  @keyframes spin { to { transform: rotate(360deg); } }
  .overlay .label { font-size: 14px; color: var(--text2); font-weight: 500; }
  .overlay .sub { font-size: 12px; color: var(--muted); margin-top: -.65rem; }

  /* log panel */
  details.log-panel {
    background: var(--panel); border: 1px solid var(--line); border-radius: 10px;
    margin: 1rem 0; overflow: hidden;
  }
  details.log-panel summary {
    cursor: pointer; padding: .65rem 1rem; font-size: 12px; color: var(--muted);
    user-select: none; font-weight: 500;
    display: flex; align-items: center; gap: .5rem;
  }
  details.log-panel summary::-webkit-details-marker { display: none; }
  details.log-panel summary::before { content: '▶'; font-size: 9px; transition: transform .15s; }
  details.log-panel[open] summary::before { transform: rotate(90deg); }
  details.log-panel pre {
    margin: 0; padding: .25rem 1rem 1rem; font: 11.5px ui-monospace, monospace;
    color: var(--text2); white-space: pre-wrap; max-height: 240px; overflow-y: auto;
  }
  details.log-panel pre .err-line { color: var(--red); }

  @media (max-width: 720px) {
    .stats { grid-template-columns: repeat(2, 1fr); }
    .controls { grid-template-columns: 1fr; }
    .lead { grid-template-columns: auto 1fr; }
    .signals { display: none; }
  }
</style>
</head>
<body>

<div class="overlay" id="loadingOverlay">
  <div class="spinner"></div>
  <div class="label" id="loadingLabel">Searching X bios…</div>
  <div class="sub" id="loadingSub">Running dork queries through proxy</div>
</div>

<div class="wrap">

  <header class="top">
    <a href="/" style="text-decoration:none; color:inherit;">
      <div class="brand">
        <div class="logo">L</div>
        <div>
          <h1>ShopifySift</h1>
          <p>Sift X bios for active Shopify stores</p>
        </div>
      </div>
    </a>
    <div style="display:flex; gap:.6rem; align-items:center;">
      {% if user %}
        <span style="background: rgba(167,139,250,.1); color: var(--accent); border: 1px solid rgba(167,139,250,.2); padding: .35rem .8rem; border-radius: 999px; font-size: 12px; font-weight: 600;">
          {{ user.credits }} credits
        </span>
        <a href="/dashboard" style="color: var(--text2); font-size: 13px; padding: .35rem .65rem; text-decoration: none;">Dashboard</a>
        <a href="/logout" style="color: var(--muted); font-size: 13px; padding: .35rem .65rem; text-decoration: none;">Log out</a>
      {% endif %}
      <span class="proxy-pill {{ '' if proxy_on else 'off' }}">
        <span class="dot"></span>
        {{ 'proxy on' if proxy_on else 'no proxy' }}
      </span>
    </div>
  </header>

  <nav class="tabs">
    <a href="/app/dork" class="{{ 'active' if mode == 'dork' else '' }}">Bio dork</a>
    <a href="/tools/domains" class="{{ 'active' if mode == 'domains' else '' }}">Domain checker</a>
    <a href="/x" class="{{ 'active' if mode == 'x' else '' }}">X login</a>
  </nav>

  {% if mode == 'dork' %}
    <div class="hero">
      <div class="hero-eyebrow">// Bio dork search</div>
      <h2>Find Shopify stores hidden in X bios.</h2>
      <p class="lead">Type a niche or click a preset pack. We dork DDG + Brave for X bios containing Shopify URLs, verify each store is live, and stream results back in real time.</p>
      <form method="post" id="dorkForm">
        <label for="kw">Keywords (comma-separated, optional)</label>
        <input type="text" name="keywords" id="kw" placeholder="klaviyo, dtc, dropshipping, skincare, pet" value="{{ submitted_keywords or '' }}" autofocus>
        <div class="packs">
          <span class="packs-label">Quick fill:</span>
          {% for name, kws in keyword_packs.items() %}
            <button type="button" class="pack-chip" data-keywords="{{ kws|join(', ') }}">
              {{ name.replace('_', ' ') }} <span class="pack-count">{{ kws|length }}</span>
            </button>
          {% endfor %}
          <button type="button" class="pack-chip pack-clear" data-keywords="">clear</button>
        </div>
        <div class="controls">
          <span>
            <label>Search engine</label>
            <select name="engine" class="select" style="width: 100%;">
              <option value="ddg" {{ 'selected' if engine=='ddg' else '' }}>DuckDuckGo only</option>
              <option value="brave" {{ 'selected' if engine=='brave' else '' }} {{ '' if brave_ready else 'disabled' }}>Brave only{{ '' if brave_ready else ' — add BRAVE_API_KEY' }}</option>
              <option value="both" {{ 'selected' if engine=='both' or (brave_ready and not engine) else '' }} {{ '' if brave_ready else 'disabled' }}>Both (DDG + Brave) — recommended</option>
            </select>
          </span>
          <span>
            <label>Per query</label>
            <input type="number" name="per_query" min="5" max="200" value="{{ per_query or 30 }}">
          </span>
          <label class="toggle">
            <input type="checkbox" name="broad" value="1" {{ 'checked' if broad else '' }}>
            <span>Custom-domain stores</span>
          </label>
          <label class="toggle">
            <input type="checkbox" name="show_noise" value="1" {{ 'checked' if show_noise else '' }}>
            <span>Show non-Shopify hits</span>
          </label>
          <span></span>
          <div class="actions-row">
            <button type="button" class="ghost" id="resetBtn" style="padding:.7rem 1.2rem; font-size:13.5px;">Reset</button>
            <button type="button" class="ghost" id="cancelBtn" style="display:none; padding:.7rem 1.2rem; font-size:13.5px;">⊗ Cancel</button>
            <button type="submit" class="primary" id="goBtn">Search bios →</button>
          </div>
        </div>
      </form>
    </div>

    {% if dork_error %}<div class="err"><strong>Error:</strong> {{ dork_error }}</div>{% endif %}

    {% if search_log %}
      <details class="log-panel" {{ 'open' if dork_error else '' }}>
        <summary>Search log ({{ search_log|length }} lines)</summary>
        <pre>{% for line in search_log %}{{ line }}
{% endfor %}</pre>
      </details>
    {% endif %}

    {% if results is not none %}
      <div class="stats">
        <div class="stat hero-stat">
          <div class="icon">⚡</div>
          <div class="num">{{ active_count }}</div>
          <div class="lbl">Active leads</div>
        </div>
        <div class="stat">
          <div class="icon" style="color: var(--orange)">🌱</div>
          <div class="num">{{ newbs }}</div>
          <div class="lbl">Newbs (.myshopify.com)</div>
        </div>
        <div class="stat">
          <div class="icon" style="color: var(--accent2)">🏪</div>
          <div class="num">{{ established }}</div>
          <div class="lbl">Established brands</div>
        </div>
        <div class="stat">
          <div class="icon" style="color: var(--muted)">⏱</div>
          <div class="num">{{ '%.1f'|format(elapsed) }}<span style="font-size: 16px; color: var(--muted); font-weight: 500">s</span></div>
          <div class="lbl">Search time</div>
        </div>
      </div>

      {% if results|length == 0 %}
        <div class="empty">
          <div class="e-icon">🔍</div>
          <h3>No bios matched</h3>
          <p>Try fewer keywords, different keywords, or enable "Include custom-domain stores" for broader coverage.</p>
        </div>
      {% else %}
        <div class="toolbar">
          <div class="pills" id="pills">
            <button type="button" class="on" data-filter="active">Active <span style="opacity:.6">{{ active_count }}</span></button>
            <button type="button" data-filter="newb">Active newbs <span style="opacity:.6">{{ newbs }}</span></button>
            <button type="button" data-filter="established">Active est. <span style="opacity:.6">{{ established }}</span></button>
            <button type="button" data-filter="dormant">Dormant <span style="opacity:.6">{{ dormant_count }}</span></button>
            <button type="button" data-filter="all">All shopify <span style="opacity:.6">{{ total_shopify }}</span></button>
          </div>
          <input type="text" class="search-in" id="searchIn" placeholder="Filter handles, bios…">
          <select class="select" id="sortBy" style="font-size:12.5px; padding:.4rem .6rem;">
            <option value="default">Sort: relevance</option>
            <option value="active">Active first</option>
            <option value="alpha">A → Z</option>
            <option value="zalpha">Z → A</option>
            <option value="newb">Newbs first</option>
            <option value="signal">Signal strength</option>
          </select>
          <span class="grow"></span>
          <button type="button" class="ghost" id="copyAll">Copy @handles</button>
          <button type="button" class="ghost" id="saveSearch">★ Save</button>
          <button type="button" class="ghost" id="runAgain">↻ Run again</button>
          <a class="ghost" href="data:text/csv;charset=utf-8,{{ csv_url|urlencode }}" download="leads.csv">⬇ Export CSV</a>
        </div>

        <div class="leads" id="leads">
          {% for r in results %}
            <div class="lead {{ 'hit' if r.active else 'dormant' }}"
                 data-active="{{ '1' if r.active else '0' }}"
                 data-category="{{ r.category }}"
                 data-text="{{ (r.username + ' ' + r.bio_snippet)|lower }}">
              <div class="avatar">{{ r.username[0]|upper }}</div>
              <div class="lead-main">
                <div class="row1">
                  <a class="handle" href="{{ r.x_profile }}" target="_blank" rel="noopener">@{{ r.username }}</a>
                  <button type="button" class="copy" data-copy="@{{ r.username }}">copy</button>
                  <div class="badges">
                    {% if r.shopify and r.active %}
                      <span class="badge active">● active</span>
                    {% elif r.shopify %}
                      <span class="badge dormant">{{ r.active_reason }}</span>
                    {% else %}
                      <span class="badge notshop">not shopify</span>
                    {% endif %}
                    {% if r.shopify %}
                      <span class="badge {{ 'newb' if r.category == 'newb' else 'est' }}">{{ r.category }}</span>
                    {% endif %}
                  </div>
                </div>
                <div class="bio">{{ r.bio_snippet }}</div>
                <div class="url-row"><a href="{{ r.shopify_url }}" target="_blank" rel="noopener">{{ r.shopify_url }}</a></div>
              </div>
              <div class="signals">{{ r.signals or '' }}</div>
            </div>
          {% endfor %}
        </div>
      {% endif %}
    {% endif %}

    <script>
      const $ = s => document.querySelector(s);
      const $$ = s => document.querySelectorAll(s);

      $$('.pack-chip').forEach(btn => {
        btn.addEventListener('click', () => {
          const kws = btn.dataset.keywords;
          const input = $('#kw');
          if (!kws) { input.value = ''; input.focus(); return; }
          const existing = input.value.split(',').map(s => s.trim()).filter(Boolean);
          const adding = kws.split(',').map(s => s.trim()).filter(Boolean);
          // Merge unique
          const merged = [...new Set([...existing, ...adding])];
          input.value = merged.join(', ');
          input.focus();
        });
      });

      $$('.copy').forEach(btn => {
        btn.addEventListener('click', () => {
          navigator.clipboard.writeText(btn.dataset.copy);
          btn.classList.add('copied');
          btn.textContent = 'copied';
          setTimeout(() => { btn.classList.remove('copied'); btn.textContent = 'copy'; }, 1100);
        });
      });

      const copyAll = $('#copyAll');
      if (copyAll) copyAll.addEventListener('click', () => {
        const visible = [...$$('.lead')].filter(el => el.style.display !== 'none');
        const handles = visible.map(el => '@' + el.dataset.text.split(' ')[0]).join('\n');
        navigator.clipboard.writeText(handles);
        copyAll.textContent = `Copied ${visible.length}`;
        setTimeout(() => copyAll.textContent = 'Copy @handles', 1300);
      });

      const applyFilters = () => {
        const filter = $('#pills .on')?.dataset.filter || 'active';
        const search = ($('#searchIn')?.value || '').toLowerCase().trim();
        $$('.lead').forEach(el => {
          const a = el.dataset.active === '1';
          const c = el.dataset.category;
          const matchFilter =
               (filter === 'all')
            || (filter === 'active' && a)
            || (filter === 'newb' && a && c === 'newb')
            || (filter === 'established' && a && c === 'established')
            || (filter === 'dormant' && !a);
          const matchSearch = !search || el.dataset.text.includes(search);
          el.style.display = (matchFilter && matchSearch) ? '' : 'none';
        });
      };
      // Initial filter (default to Active)
      applyFilters();

      $$('#pills button').forEach(b => b.addEventListener('click', () => {
        $$('#pills button').forEach(x => x.classList.remove('on'));
        b.classList.add('on');
        applyFilters();
      }));
      $('#searchIn')?.addEventListener('input', applyFilters);

      const form = $('#dorkForm');
      const goBtn = $('#goBtn');
      const cancelBtn = $('#cancelBtn');
      const resetBtn = $('#resetBtn');
      const overlay = $('#loadingOverlay');
      const label = $('#loadingLabel');
      const sub = $('#loadingSub');

      // Cancel: close the EventSource + reset UI
      cancelBtn?.addEventListener('click', () => {
        if (eventSource) { eventSource.close(); eventSource = null; }
        cancelBtn.style.display = 'none';
        goBtn.style.display = '';
        goBtn.disabled = false;
        goBtn.textContent = 'Search bios';
        overlay.classList.remove('on');
        appendLog('✗ search cancelled');
      });

      // Reset form
      resetBtn?.addEventListener('click', () => {
        if (form) form.reset();
        $('#kw').focus();
      });

      // Sort dropdown
      $('#sortBy')?.addEventListener('change', e => {
        const mode = e.target.value;
        const container = $('#leads') || $('#liveLeads');
        if (!container) return;
        const rows = [...container.querySelectorAll('.lead')];
        const score = el => {
          const a = el.dataset.active === '1' ? 1 : 0;
          const c = el.dataset.category;
          const handle = (el.dataset.text || '').split(' ')[0];
          const sig = (el.querySelector('.signals')?.textContent || '').split(',').length;
          if (mode === 'active') return -a;
          if (mode === 'alpha') return handle;
          if (mode === 'zalpha') return -handle.charCodeAt(0);
          if (mode === 'newb') return c === 'newb' ? -1 : 0;
          if (mode === 'signal') return -sig;
          return 0;
        };
        rows.sort((a,b) => {
          const sa = score(a), sb = score(b);
          if (typeof sa === 'string') return sa.localeCompare(sb);
          return sa - sb;
        });
        rows.forEach(r => container.appendChild(r));
      });

      // Save search to localStorage
      $('#saveSearch')?.addEventListener('click', () => {
        const fd = new FormData(form);
        const data = Object.fromEntries(fd.entries());
        data.savedAt = new Date().toISOString();
        const saved = JSON.parse(localStorage.getItem('shopifysift.savedSearches') || '[]');
        saved.unshift(data);
        localStorage.setItem('shopifysift.savedSearches', JSON.stringify(saved.slice(0, 20)));
        const btn = $('#saveSearch');
        btn.textContent = '✓ Saved';
        setTimeout(() => btn.textContent = '★ Save', 1500);
      });

      // Re-run last search
      $('#runAgain')?.addEventListener('click', () => {
        if (form) form.dispatchEvent(new Event('submit', { cancelable: true }));
      });

      // Clear log button (injected by ensureLiveContainers)

      // Use SSE for live progress instead of synchronous POST
      let eventSource = null;
      let liveCounts = { active: 0, newbs: 0, established: 0, total: 0 };
      let queryDone = 0, queryTotal = 0;
      let verifyDone = 0, verifyTotal = 0;

      const ensureLiveContainers = () => {
        let log = $('#liveLog');
        if (!log) {
          // Inject below the form
          const card = document.querySelector('.hero');
          const html = `
            <div class="stats" id="liveStats">
              <div class="stat hero-stat"><div class="icon">⚡</div><div class="num" id="ls-active">0</div><div class="lbl">Active leads</div></div>
              <div class="stat"><div class="icon" style="color:var(--orange)">🌱</div><div class="num" id="ls-newbs">0</div><div class="lbl">Newbs</div></div>
              <div class="stat"><div class="icon" style="color:var(--accent2)">🏪</div><div class="num" id="ls-est">0</div><div class="lbl">Established</div></div>
              <div class="stat"><div class="icon" style="color:var(--muted)">🔎</div><div class="num" id="ls-found">0</div><div class="lbl">Candidates found</div></div>
              <div class="stat" style="grid-column: span 4"><div class="icon" style="color:var(--muted)">⏱</div><div class="num" id="ls-progress">Starting…</div><div class="lbl">Status</div></div>
            </div>
            <details class="log-panel" open>
              <summary id="liveLogSummary">Live log
                <button type="button" id="clearLog" class="ghost" style="font-size:10.5px; padding:.15rem .5rem; margin-left:auto;">clear</button>
              </summary>
              <pre id="liveLog"></pre>
            </details>
            <div class="leads" id="liveLeads"></div>
          `;
          card.insertAdjacentHTML('afterend', html);
        }
      };

      const appendLog = (msg) => {
        ensureLiveContainers();
        const pre = $('#liveLog');
        pre.textContent += msg + '\n';
        pre.scrollTop = pre.scrollHeight;
        const summary = $('#liveLogSummary');
        if (summary) summary.textContent = `Live log (${pre.textContent.split('\n').length - 1} lines)`;
      };

      let candidatesFound = 0;
      const updateStats = () => {
        if ($('#ls-active')) {
          $('#ls-active').textContent = liveCounts.active;
          $('#ls-newbs').textContent = liveCounts.newbs;
          $('#ls-est').textContent = liveCounts.established;
          if ($('#ls-found')) $('#ls-found').textContent = candidatesFound;
          let status;
          if (queryTotal === 0) {
            status = 'Starting…';
          } else if (queryDone < queryTotal) {
            const pct = Math.round((queryDone / queryTotal) * 100);
            status = `Searching · ${pct}% (${queryDone}/${queryTotal} dorks)`;
          } else if (candidatesFound === 0) {
            status = 'No candidates found';
          } else if (verifyDone < candidatesFound) {
            const pct = Math.round((verifyDone / candidatesFound) * 100);
            status = `Verifying stores · ${pct}% (${verifyDone}/${candidatesFound})`;
          } else {
            status = `Done · ${liveCounts.active} active leads`;
          }
          $('#ls-progress').textContent = status;
        }
      };

      const renderLead = (row) => {
        ensureLiveContainers();
        if (!row.shopify) return;  // skip noise rows live
        const leads = $('#liveLeads');
        const div = document.createElement('div');
        div.className = `lead ${row.active ? 'hit' : 'dormant'}`;
        div.dataset.active = row.active ? '1' : '0';
        div.dataset.category = row.category;
        const initial = (row.username || '?')[0].toUpperCase();
        const status = row.active
          ? '<span class="badge active">● active</span>'
          : `<span class="badge dormant">${row.active_reason || 'dormant'}</span>`;
        const cat = `<span class="badge ${row.category === 'newb' ? 'newb' : 'est'}">${row.category}</span>`;
        const bio = (row.bio_snippet || '').slice(0, 240);
        div.innerHTML = `
          <div class="avatar">${initial}</div>
          <div class="lead-main">
            <div class="row1">
              <a class="handle" href="${row.x_profile}" target="_blank" rel="noopener">@${row.username}</a>
              <button type="button" class="copy" data-copy="@${row.username}">copy</button>
              <div class="badges">${status}${cat}</div>
            </div>
            <div class="bio">${bio.replace(/[<>]/g, c => ({'<':'&lt;','>':'&gt;'})[c])}</div>
            <div class="url-row"><a href="${row.shopify_url}" target="_blank" rel="noopener">${row.shopify_url}</a></div>
          </div>
          <div class="signals">${row.signals || ''}</div>
        `;
        // Active rows go to top; dormant to bottom
        if (row.active) leads.insertBefore(div, leads.firstChild);
        else leads.appendChild(div);
        // Wire up copy button
        div.querySelector('.copy').addEventListener('click', e => {
          navigator.clipboard.writeText(div.querySelector('.copy').dataset.copy);
          div.querySelector('.copy').textContent = 'copied';
          setTimeout(() => div.querySelector('.copy').textContent = 'copy', 1100);
        });
      };

      if (form && goBtn && overlay) {
        form.addEventListener('submit', (e) => {
          e.preventDefault();
          if (eventSource) eventSource.close();

          // Reset live state
          liveCounts = { active: 0, newbs: 0, established: 0, total: 0 };
          queryDone = 0; queryTotal = 0; verifyDone = 0; verifyTotal = 0;
          $('#liveStats')?.remove();
          $('.log-panel')?.remove();
          $('#liveLeads')?.remove();
          ensureLiveContainers();

          $('#clearLog')?.addEventListener('click', e => {
            e.stopPropagation(); e.preventDefault();
            const pre = $('#liveLog'); if (pre) pre.textContent = '';
            const sum = $('#liveLogSummary');
            if (sum) sum.firstChild.textContent = 'Live log ';
          });

          // Show cancel, hide go
          cancelBtn.style.display = '';

          // Build query string from form
          const fd = new FormData(form);
          const params = new URLSearchParams();
          for (const [k, v] of fd.entries()) params.append(k, v);
          if (!params.has('broad')) params.append('broad', '0');
          else params.set('broad', '1');

          goBtn.disabled = true;
          goBtn.textContent = 'Searching…';
          const kw = $('#kw').value.trim();
          label.textContent = kw ? `Searching for "${kw.split(',')[0]}…"` : 'Searching X bios…';
          overlay.classList.add('on');
          let dots = 0;
          const subInterval = setInterval(() => {
            dots = (dots + 1) % 4;
            sub.textContent = `${verifyDone || queryDone} so far${'.'.repeat(dots)}`;
          }, 400);

          eventSource = new EventSource('/dork/stream?' + params.toString());

          eventSource.addEventListener('log', (ev) => {
            const data = JSON.parse(ev.data);
            appendLog(data.msg);
            // Parse query progress from log lines like "[3/162] DDG ..."
            const m = data.msg.match(/^\[(\d+)\/(\d+)\]/);
            if (m) {
              queryDone = parseInt(m[1]);
              queryTotal = parseInt(m[2]);
              updateStats();
            }
            // Hide overlay once first real log arrives — they can watch it stream
            overlay.classList.remove('on');
          });

          eventSource.addEventListener('queries', (ev) => {
            queryTotal = JSON.parse(ev.data).total;
            updateStats();
          });

          eventSource.addEventListener('candidate', (ev) => {
            candidatesFound = JSON.parse(ev.data).count;
            updateStats();
          });

          eventSource.addEventListener('phase', (ev) => {
            const data = JSON.parse(ev.data);
            if (data.name === 'verifying') {
              verifyTotal = data.total;
              updateStats();
            }
          });

          eventSource.addEventListener('lead', (ev) => {
            const data = JSON.parse(ev.data);
            verifyDone = data.done;
            verifyTotal = data.total;
            const row = data.row;
            if (row.shopify) {
              liveCounts.total++;
              if (row.active) liveCounts.active++;
              if (row.active && row.category === 'newb') liveCounts.newbs++;
              if (row.active && row.category === 'established') liveCounts.established++;
            }
            renderLead(row);
            updateStats();
          });

          eventSource.addEventListener('done', (ev) => {
            const data = JSON.parse(ev.data);
            appendLog(`✓ done in ${data.elapsed.toFixed(1)}s`);
            eventSource.close();
            eventSource = null;
            clearInterval(subInterval);
            overlay.classList.remove('on');
            goBtn.disabled = false;
            goBtn.textContent = 'Search again';
            cancelBtn.style.display = 'none';
          });

          eventSource.addEventListener('error', (ev) => {
            if (ev.data) {
              const data = JSON.parse(ev.data);
              appendLog(`ERROR: ${data.msg}`);
            }
            if (eventSource && eventSource.readyState === EventSource.CLOSED) {
              clearInterval(subInterval);
              overlay.classList.remove('on');
              goBtn.disabled = false;
              goBtn.textContent = 'Search';
            }
          });
        });
      }
    </script>

  {% elif mode == 'domains' %}
    <div class="hero">
      <h2>Bulk-check domains for Shopify</h2>
      <p class="lead">Paste domains, one per line. Each is probed for Shopify fingerprints.</p>
      <form method="post">
        <label>Domains</label>
        <textarea name="domains" placeholder="allbirds.com&#10;gymshark.com&#10;apple.com" autofocus>{{ submitted or '' }}</textarea>
        <div class="controls">
          <span></span><span></span>
          <button type="submit" class="primary">Detect</button>
        </div>
      </form>
    </div>
    {% if results %}
      <div class="leads" style="margin-top:1.5rem">
      {% for r in results %}
        <div class="lead {{ 'hit' if r.shopify else 'dormant' }}">
          <div class="avatar">{{ r.url.split('//')[-1][0]|upper }}</div>
          <div class="lead-main">
            <div class="row1">
              <a class="handle" href="{{ r.url }}" target="_blank">{{ r.url }}</a>
              <div class="badges">
                {% if r.shopify %}<span class="badge active">● shopify</span>{% else %}<span class="badge notshop">not shopify</span>{% endif %}
              </div>
            </div>
          </div>
          <div class="signals">{{ r.signals or '—' }}</div>
        </div>
      {% endfor %}
      </div>
    {% endif %}

  {% else %}
    <div class="hero">
      {% if not accounts_ok %}
        <div class="warn"><strong>No accounts.txt.</strong> The X-login mode needs throwaway X creds. The Bio dork tab works without them.</div>
      {% endif %}
      <h2>Logged-in X search</h2>
      <p class="lead">Searches X via twscrape with throwaway accounts. Use Bio dork tab if you'd rather skip credentials.</p>
      <form method="post">
        <label>Keywords (comma-separated)</label>
        <input type="text" name="keywords" placeholder="klaviyo, dtc founder" value="{{ submitted_keywords or '' }}">
        <div class="controls">
          <span><label>Per keyword</label><input type="number" name="per_keyword" min="1" max="500" value="{{ per_keyword or 25 }}"></span>
          <span></span>
          <button type="submit" class="primary" {{ '' if accounts_ok else 'disabled' }}>Search X</button>
        </div>
      </form>
    </div>
    {% if x_error %}<div class="err">{{ x_error }}</div>{% endif %}
    {% if results %}
      <div class="leads" style="margin-top:1.5rem">
      {% for r in results %}
        <div class="lead {{ 'hit' if r.shopify else 'dormant' }}">
          <div class="avatar">{{ r.username[0]|upper }}</div>
          <div class="lead-main">
            <div class="row1">
              <a class="handle" href="https://x.com/{{ r.username }}" target="_blank">@{{ r.username }}</a>
              <div class="badges">
                {% if r.shopify %}<span class="badge active">● shopify</span>{% endif %}
              </div>
            </div>
            <div class="bio">{{ r.bio[:200] }}</div>
            <div class="url-row">{{ r.website }}</div>
          </div>
          <div class="signals">{{ r.signals or '' }}</div>
        </div>
      {% endfor %}
      </div>
    {% endif %}
  {% endif %}

</div>
</body>
</html>
"""


def parse_domains(raw: str) -> list[str]:
    return [
        line.strip() for line in (raw or "").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


def run_checks(domains: list[str]) -> list[dict]:
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=min(128, max(1, len(domains)))) as pool:
        futures = [pool.submit(check, d) for d in domains]
        for fut in as_completed(futures):
            row = fut.result()
            if row is not None:
                results.append(row)
    results.sort(key=lambda r: (not r["shopify"], r["url"]))
    return results


def rows_to_csv(rows: list[dict]) -> str:
    buf = io.StringIO()
    fields = ["username", "x_profile", "bio_snippet", "shopify_url",
              "shopify", "active", "active_reason", "category", "signals"]
    writer = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


@app.route("/")
def landing():
    return render_template_string(LANDING_PAGE, user=current_user())


@app.route("/signup", methods=["GET", "POST"])
def signup_page():
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""
        if not EMAIL_RE.match(email):
            flash("Enter a valid email address.")
        elif len(password) < 8:
            flash("Password must be at least 8 characters.")
        elif db.get_user_by_email(email):
            flash("That email is already registered. Try logging in.")
        else:
            user_id = db.create_user(email, hash_password(password))
            session["user_id"] = user_id
            return redirect(url_for("dashboard"))
    return render_template_string(
        AUTH_PAGE, user=None, title="Create an account",
        sub="Start with 5 free searches. No credit card required.",
        cta="Sign up free", show_password=True,
        switch_text='Already have an account? <a href="/login">Log in</a>',
    )


@app.route("/login", methods=["GET", "POST"])
def login_page():
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""
        user = db.get_user_by_email(email) if EMAIL_RE.match(email) else None
        if user and verify_password(password, user.get("password_hash", "")):
            session["user_id"] = user["id"]
            nxt = request.args.get("next") or url_for("dashboard")
            return redirect(nxt)
        flash("Invalid email or password.")
    return render_template_string(
        AUTH_PAGE, user=None, title="Log in",
        sub="Welcome back — sign in to your dashboard.",
        cta="Log in", show_password=True,
        switch_text='New here? <a href="/signup">Sign up free</a>',
    )


@app.route("/account/delete", methods=["POST"])
@require_login
def delete_account():
    """GDPR-style account deletion. Removes user + all their data."""
    uid = session["user_id"]
    confirm = request.form.get("confirm", "").strip().lower()
    if confirm != "delete my account":
        flash("Type 'delete my account' exactly to confirm.")
        return redirect(url_for("settings_page"))
    db.delete_user(uid)
    session.clear()
    flash("Your account and all data have been deleted.")
    return redirect(url_for("landing"))


@app.route("/logout")
def logout():
    session.pop("user_id", None)
    return redirect(url_for("landing"))


def _render_app(active, page_title, body_template, **ctx):
    user = current_user()
    body_html = render_template_string(body_template, user=user, **ctx)
    return render_template_string(
        APP_SHELL, user=user, active=active, page_title=page_title, body=body_html,
    )


@app.route("/dashboard")
@require_login
def dashboard():
    user = current_user()
    stats = db.user_stats(user["id"])
    history = db.get_recent_searches(user["id"], limit=20)
    return _render_app("dashboard", "Dashboard", DASHBOARD_BODY,
                       stats=stats, history=history)


@app.route("/app/leads")
@require_login
def leads_page():
    user = current_user()
    stats = db.user_stats(user["id"])
    leads = db.get_recent_user_leads(user["id"], limit=500)
    active_count = sum(1 for l in leads if l.get("is_active"))
    return _render_app("leads", "Leads", LEADS_BODY,
                       stats=stats, leads=leads, active_count=active_count)


@app.route("/app/history")
@require_login
def history_page():
    user = current_user()
    history = db.get_recent_searches(user["id"], limit=200)
    return _render_app("history", "History", HISTORY_BODY, history=history)


@app.route("/app/settings")
@require_login
def settings_page():
    return _render_app("settings", "Settings", SETTINGS_BODY)


@app.route("/pricing")
def pricing():
    return render_template_string(
        PRICING_PAGE, user=current_user(), plans=list(PLANS.values()),
    )


@app.route("/checkout")
@require_login
def checkout():
    plan_id = request.args.get("plan", "pro")
    plan = PLANS.get(plan_id)
    if not plan:
        flash("Pick a plan first.")
        return redirect(url_for("pricing"))
    return render_template_string(
        CHECKOUT_PAGE, user=current_user(), plan=plan,
    )


@app.route("/checkout/confirm", methods=["POST"])
@require_login
def checkout_confirm():
    plan_id = request.form.get("plan", "")
    plan = PLANS.get(plan_id)
    if not plan:
        flash("Invalid plan.")
        return redirect(url_for("pricing"))
    user = current_user()

    # Real Stripe path — used in production once STRIPE_SECRET_KEY is set
    if stripe and STRIPE_SECRET_KEY:
        try:
            sess = stripe.checkout.Session.create(
                mode="payment",
                payment_method_types=["card"],
                line_items=[{
                    "price_data": {
                        "currency": "usd",
                        "product_data": {
                            "name": f"{plan['name']} pack — {plan['credits']} ShopifySift searches",
                            "description": plan.get("tagline", ""),
                        },
                        "unit_amount": int(plan["price"]) * 100,
                    },
                    "quantity": 1,
                }],
                success_url=url_for("checkout_success", plan=plan_id, _external=True)
                            + "&session_id={CHECKOUT_SESSION_ID}",
                cancel_url=url_for("pricing", _external=True),
                customer_email=user["email"],
                metadata={
                    "user_id": str(user["id"]),
                    "plan": plan_id,
                    "credits": str(plan["credits"]),
                },
            )
            return redirect(sess.url, code=303)
        except Exception as e:
            flash(f"Stripe error: {type(e).__name__}: {e}")
            return redirect(url_for("pricing"))

    # Test mode (no Stripe key) — credit the account directly
    db.add_credits(user["id"], plan["credits"])
    return redirect(url_for("checkout_success", plan=plan_id))


@app.route("/stripe/webhook", methods=["POST"])
def stripe_webhook():
    """Stripe -> us. Verify signature and credit on checkout.session.completed."""
    if not (stripe and STRIPE_WEBHOOK_SECRET):
        return ("Stripe not configured", 503)
    payload = request.data
    sig = request.headers.get("Stripe-Signature", "")
    try:
        event = stripe.Webhook.construct_event(payload, sig, STRIPE_WEBHOOK_SECRET)
    except Exception as e:
        return (f"Bad signature: {e}", 400)

    if event["type"] == "checkout.session.completed":
        sess = event["data"]["object"]
        meta = sess.get("metadata") or {}
        try:
            user_id = int(meta.get("user_id"))
            credits = int(meta.get("credits"))
        except (TypeError, ValueError):
            return ("Bad metadata", 400)
        db.add_credits(user_id, credits)
        return ("OK", 200)

    return ("ignored", 200)


LEGAL_PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{ title }} · ShopifySift</title>
""" + SHARED_STYLE + r"""
<style>
  .legal { max-width: 760px; margin: 4rem auto 6rem; padding: 0 1.5rem; }
  .legal h1 { font-size: clamp(28px, 4vw, 40px); margin: 0 0 .5rem; letter-spacing: -.025em; line-height: 1.1; font-weight: 600; }
  .legal p.meta { color: var(--text2); font-size: 13px; margin: 0 0 2.5rem; }
  .legal h2 { font-size: 18px; margin: 2rem 0 .65rem; font-weight: 600; }
  .legal p { color: var(--text2); font-size: 14.5px; line-height: 1.65; margin: 0 0 1rem; }
  .legal a { color: var(--accent); }
  .legal .stub-banner {
    background: rgba(251,146,60,.08); border: 1px solid rgba(251,146,60,.2);
    border-radius: 10px; padding: 1rem 1.25rem; margin-bottom: 2rem; color: #ffd0a8; font-size: 13px;
  }
</style>
</head>
<body>
""" + NAV + r"""
<div class="legal">
  <h1>{{ title }}</h1>
  <p class="meta">Last updated: {{ updated }}</p>
  <div class="stub-banner">
    <strong>Placeholder.</strong> Replace this with a real legal document before taking payments. We recommend <a href="https://termly.io" target="_blank">Termly.io</a> ($10/mo) or <a href="https://termsfeed.com" target="_blank">TermsFeed</a> for a generated GDPR/CCPA-compliant version.
  </div>
  {{ body|safe }}
</div>
</body>
</html>
"""

TERMS_BODY = r"""
<h2>1. Acceptance of terms</h2>
<p>By creating an account you agree to these terms.</p>
<h2>2. Service</h2>
<p>ShopifySift queries public search-engine indexes for X (Twitter) profiles whose bios contain Shopify store URLs. We verify each store is live before returning it. We do not scrape X directly.</p>
<h2>3. Acceptable use</h2>
<p>You agree not to use ShopifySift for spam, harassment, or any unlawful purpose. Cold outreach must comply with X's user-facing rules and applicable email/SMS laws (CAN-SPAM, GDPR, etc.).</p>
<h2>4. Credits and payment</h2>
<p>Credits are purchased in packs and never expire. Refunds are at our discretion within 14 days of purchase, for unused credits.</p>
<h2>5. Termination</h2>
<p>You may delete your account at any time from <a href="/app/settings">Settings</a>. We may terminate accounts that violate these terms.</p>
<h2>6. Liability</h2>
<p>ShopifySift is provided "as is". We're not responsible for outcomes of your outreach.</p>
<h2>7. Contact</h2>
<p>Questions: <a href="mailto:hi@shopifysift.app">hi@shopifysift.app</a></p>
"""

PRIVACY_BODY = r"""
<h2>1. What we collect</h2>
<p>Email address (for login), search history (keywords + counts), and Stripe customer ID if you purchase credits. We log basic request metadata (IP, user-agent, timestamps) for security.</p>
<h2>2. What we don't collect</h2>
<p>We do not store the X handles, bios, or Shopify URLs that searches return — those are streamed to you and saved per-search to your dashboard for your own reference. They never leave your account.</p>
<h2>3. Third-party services</h2>
<p>Stripe (payments), Resend (email), DataImpulse (proxy), DuckDuckGo + Brave (search APIs). Each has their own privacy policy.</p>
<h2>4. Your rights</h2>
<p>EU/CA users: you can export, correct, or delete your data anytime from <a href="/app/settings">Settings</a>. Account deletion is permanent and immediate.</p>
<h2>5. Cookies</h2>
<p>One session cookie for login. No tracking cookies.</p>
<h2>6. Contact</h2>
<p>Privacy questions: <a href="mailto:hi@shopifysift.app">hi@shopifysift.app</a></p>
"""


@app.route("/terms")
def terms_page():
    return render_template_string(LEGAL_PAGE, user=current_user(),
                                  title="Terms of Service",
                                  updated="2026-05-09", body=TERMS_BODY)


@app.route("/privacy")
def privacy_page():
    return render_template_string(LEGAL_PAGE, user=current_user(),
                                  title="Privacy Policy",
                                  updated="2026-05-09", body=PRIVACY_BODY)


@app.route("/checkout/success")
@require_login
def checkout_success():
    plan_id = request.args.get("plan", "pro")
    plan = PLANS.get(plan_id)
    if not plan:
        return redirect(url_for("dashboard"))
    return render_template_string(
        CHECKOUT_SUCCESS_PAGE, user=current_user(), plan=plan,
    )


@app.route("/tools/domains", methods=["GET", "POST"])
@require_login
def domain_checker():
    submitted = request.form.get("domains", "") if request.method == "POST" else ""
    results = None
    if request.method == "POST":
        domains = parse_domains(submitted)
        if domains:
            results = run_checks(domains)
    return render_template_string(
        PAGE, mode="domains", submitted=submitted, results=results,
        proxy_on=bool(PROXY_URL), user=current_user(),
    )


@app.route("/x", methods=["GET", "POST"])
def x_search():
    accounts_ok = Path(ACCOUNTS_FILE).exists()
    submitted_keywords = ""
    per_keyword = 25
    results = None
    x_error = None
    if request.method == "POST":
        submitted_keywords = request.form.get("keywords", "").strip()
        try:
            per_keyword = max(1, min(500, int(request.form.get("per_keyword", 25))))
        except ValueError:
            per_keyword = 25
        keywords = [k.strip() for k in submitted_keywords.split(",") if k.strip()]
        if not accounts_ok:
            x_error = "Add accounts.txt before running an X search."
        elif keywords:
            try:
                from x_scraper import detect_all, search_profiles  # lazy
                profiles = asyncio.run(search_profiles(keywords, per_keyword))
                rows = detect_all(profiles, workers=64)
                rows.sort(key=lambda r: (not r["shopify"], -r.get("followers", 0)))
                results = rows
            except Exception as e:
                x_error = f"{type(e).__name__}: {e}"
    return render_template_string(
        PAGE, mode="x", user=current_user(), accounts_ok=accounts_ok,
        submitted_keywords=submitted_keywords, per_keyword=per_keyword,
        results=results, x_error=x_error, proxy_on=bool(PROXY_URL),
    )


@app.route("/app/dork", methods=["GET", "POST"])
@app.route("/dork", methods=["GET", "POST"])  # legacy alias
@require_login
def dork():
    submitted_keywords = ""
    per_query = 50
    broad = False
    show_noise = False
    engine = "both" if BRAVE_API_KEY else "ddg"
    results = None
    elapsed = 0.0
    dork_error = None
    csv_url = ""
    active_count = newbs = established = total_shopify = dormant_count = 0
    search_log: list[str] = []

    if request.method == "POST":
        submitted_keywords = request.form.get("keywords", "").strip()
        broad = bool(request.form.get("broad"))
        engine = request.form.get("engine", "ddg")
        if engine not in ("ddg", "brave", "both"):
            engine = "ddg"
        try:
            per_query = max(5, min(200, int(request.form.get("per_query", 50))))
        except ValueError:
            per_query = 50
        keywords = [k.strip() for k in submitted_keywords.split(",") if k.strip()]
        show_noise = bool(request.form.get("show_noise"))
        try:
            t0 = time.time()
            queries = dork_queries(keywords, broad=broad)
            search_log = []
            by_user = search_dorks(queries, per_query, log=search_log, engine=engine)
            rows = list(by_user.values())
            if rows:
                search_log.append(f"Verifying {len(rows)} stores (Shopify + active)...")
            if rows:
                rows = verify_all(rows, workers=64)
                rows.sort(key=lambda r: (
                    not (r["shopify"] and r["active"]),
                    not r["shopify"],
                    r["username"],
                ))
            shopify_rows = [r for r in rows if r.get("shopify")]
            noise_rows = [r for r in rows if not r.get("shopify")]
            display_rows = rows if show_noise else shopify_rows

            active_count = sum(1 for r in shopify_rows if r.get("active"))
            dormant_count = sum(1 for r in shopify_rows if not r.get("active"))
            total_shopify = len(shopify_rows)
            newbs = sum(1 for r in shopify_rows if r.get("active") and r.get("category") == "newb")
            established = sum(
                1 for r in shopify_rows
                if r.get("active") and r.get("category") == "established"
            )
            results = display_rows
            csv_url = rows_to_csv(shopify_rows)
            elapsed = time.time() - t0
            search_log.append(
                f"Verification complete: {active_count} active / {total_shopify} shopify / {len(rows)} total candidates "
                f"({len(noise_rows)} noise rows hidden)"
            )
        except Exception as e:
            import traceback; traceback.print_exc()
            dork_error = f"{type(e).__name__}: {e}"

    return render_template_string(
        PAGE, mode="dork", user=current_user(),
        submitted_keywords=submitted_keywords, per_query=per_query, broad=broad,
        show_noise=show_noise, engine=engine,
        brave_ready=bool(BRAVE_API_KEY),
        keyword_packs=KEYWORD_PACKS,
        results=results, elapsed=elapsed, dork_error=dork_error,
        active_count=active_count, newbs=newbs, established=established,
        total_shopify=total_shopify, dormant_count=dormant_count,
        csv_url=csv_url, proxy_on=bool(PROXY_URL), search_log=search_log,
    )


@app.route("/app/dork/stream")
@app.route("/dork/stream")  # legacy alias
@require_login
def dork_stream():
    """Server-sent events: streams live log + leads as the search progresses."""
    from bio_dork import dork_queries, search_dorks, verify_one

    submitted_keywords = request.args.get("keywords", "").strip()
    broad = request.args.get("broad", "0") == "1"
    engine = request.args.get("engine", "ddg")
    if engine not in ("ddg", "brave", "both"):
        engine = "ddg"
    try:
        per_query = max(5, min(200, int(request.args.get("per_query", 30))))
    except ValueError:
        per_query = 30

    keywords = [k.strip() for k in submitted_keywords.split(",") if k.strip()]
    q: queue.Queue = queue.Queue()

    user = current_user()

    # Quota check + credit deduction up-front so spamming refresh can't
    # bypass it.
    if not db.use_credit(user["id"]):
        def gen_no_credit():
            yield "retry: 5000\n\n"
            yield (
                'event: error\ndata: ' +
                json.dumps({"msg": "Out of credits. Email aubrey for more."}) +
                '\n\n'
            )
        return Response(gen_no_credit(), mimetype="text/event-stream")

    search_id = db.insert_search_start(user["id"], submitted_keywords, engine, broad)

    def push(event_type: str, **payload):
        q.put((event_type, payload))

    def runner():
        verify_pool = ThreadPoolExecutor(max_workers=128)
        seen_users: set = set()
        verify_count = {"submitted": 0, "done": 0, "active": 0}

        def on_candidate(profile):
            if profile["username"] in seen_users:
                return
            seen_users.add(profile["username"])
            verify_count["submitted"] += 1
            push("candidate", count=verify_count["submitted"])

            def task(p=profile):
                from bio_dork import verify_one
                return verify_one(p)

            fut = verify_pool.submit(task)

            def done_cb(f):
                try:
                    row = f.result()
                except Exception as ex:
                    q.put(("log", {"msg": f"verify error: {ex}"}))
                    return
                verify_count["done"] += 1
                if row.get("active"):
                    verify_count["active"] += 1
                # Persist lead so it's not lost if the user closes the tab
                try:
                    db.add_search_lead(search_id, user["id"], row)
                except Exception as ex:
                    print(f"DB persist error: {ex}", file=sys.stderr)
                q.put(("lead", {
                    "row": row,
                    "done": verify_count["done"],
                    "submitted": verify_count["submitted"],
                }))
            fut.add_done_callback(done_cb)

        try:
            t0 = time.time()
            queries = dork_queries(keywords, broad=broad)
            push("log", msg=f"Generated {len(queries)} dork queries from {len(keywords) or 'broad'} keyword(s)")
            push("queries", total=len(queries))

            search_dorks(
                queries, per_query, engine=engine,
                on_log=lambda msg: q.put(("log", {"msg": msg})),
                on_candidate=on_candidate,
            )
            push("log", msg=f"Search phase done. Waiting for {verify_count['submitted'] - verify_count['done']} pending verifications...")

            verify_pool.shutdown(wait=True)
            push("log", msg=f"Verification complete: {verify_count['done']} candidates verified")
            db.update_search_results(
                user["id"], search_id,
                leads_found=verify_count["done"],
                active_leads=verify_count["active"],
            )
            db.update_search_status(search_id, user["id"], "done")
            push("done", elapsed=time.time() - t0)
        except Exception as e:
            push("error", msg=f"{type(e).__name__}: {e}")
            verify_pool.shutdown(wait=False, cancel_futures=True)
            try:
                db.update_search_status(search_id, user["id"], "error")
            except Exception:
                pass
        finally:
            q.put(None)

    threading.Thread(target=runner, daemon=True).start()

    @stream_with_context
    def gen():
        yield "retry: 5000\n\n"
        while True:
            item = q.get()
            if item is None:
                break
            event_type, payload = item
            yield f"event: {event_type}\ndata: {json.dumps(payload)}\n\n"

    return Response(gen(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache",
                             "X-Accel-Buffering": "no"})


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False, threaded=True)
