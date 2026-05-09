"""XSift — Shopify-store-in-X-bio finder for cold outreach."""

import asyncio
import csv
import io
import json
import os
import queue
import re
import secrets
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
app.secret_key = os.environ.get("FLASK_SECRET") or secrets.token_hex(32)
db.init_db()


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
      XSift
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
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>XSift · Sift X bios for active Shopify stores</title>
<meta name="description" content="XSift finds active Shopify stores hidden in X (Twitter) bios. Verified handles ready for cold DM outreach.">
<link rel="icon" type="image/svg+xml" href="/static/favicon.svg">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Geist:wght@400;500;600;700&family=Geist+Mono:wght@400;500&display=swap">
<style>
  :root {
    --bg: #16181c;                  /* warm charcoal — not pure black */
    --bg-card: #1f2227;             /* card on dark */
    --bg-cream: #f5d9c4;             /* peachy cream — alternating sections */
    --bg-cream-alt: #fbe4d2;         /* lighter peach variant for cards */
    --text: #f5f0eb;                 /* warm off-white on dark */
    --text-dark: #1a1612;            /* on cream */
    --text2: #a8a39e;                /* muted on dark — warm gray */
    --text2-dark: #6b5648;           /* muted on cream */
    --muted: #74706c;
    --line: rgba(255,255,255,.07);
    --line-dark: rgba(26,22,18,.14); /* hairline on cream */
    --accent: #ff7a3c;               /* vibrant warm orange */
    --accent-hover: #ff8b4f;
    --accent-soft: rgba(255,122,60,.16);
    --green: #4ade80;
    --green-soft: rgba(74,222,128,.12);
    color-scheme: dark;
  }
  /* Cream-section utility — swaps surface + text + lines */
  .cream {
    background: var(--bg-cream);
    color: var(--text-dark);
  }
  .cream h1, .cream h2, .cream h3, .cream h4, .cream h5 { color: var(--text-dark); }
  .cream .text2 { color: var(--text2-dark); }
  .cream .eyebrow { color: var(--accent); }
  .cream .feat-card,
  .cream .quote-card,
  .cream .price-card {
    background: var(--bg-cream-alt);
    border-color: var(--line-dark);
    color: var(--text-dark);
  }
  .cream .feat-card h3, .cream .quote-card p.q,
  .cream .price-card h3 { color: var(--text-dark); }
  .cream .feat-card p, .cream .price-card .desc,
  .cream .price-card li, .cream .who .nm { color: var(--text2-dark); }
  .cream .who .nm { color: var(--text-dark); }
  .cream .step-item { border-color: var(--line-dark); }
  .cream .step-item h4 { color: var(--text-dark); }
  .cream .step-item p { color: var(--text2-dark); }
  .cream .steps { border-top-color: var(--line-dark); }
  .cream .faq-item { border-color: var(--line-dark); }
  .cream .faq-item summary { color: var(--text-dark); }
  .cream .faq-item p { color: var(--text2-dark); }
  .cream .btn.btn-ghost {
    color: var(--text-dark); border-color: var(--line-dark);
  }
  .cream .btn.btn-ghost:hover { border-color: var(--text-dark); }
  .cream .sec-h p { color: var(--text2-dark); }
  .cream .stat-tile .num { color: var(--text-dark); }
  .cream .stat-tile .lbl { color: var(--text2-dark); }
  .cream .price-card .pp,
  .cream .step-item .step-num { color: var(--accent); font-weight: 600; }
  .cream .feat-icon { background: var(--accent); color: #fff; }
  .cream .stat-tile .num { color: var(--accent); }
  .cream .stars { color: var(--accent); }
  .cream .verified-tag { background: var(--accent); color: #fff; }
  * { box-sizing: border-box; }
  html, body { margin: 0; padding: 0; }
  body {
    font-family: 'Geist', -apple-system, BlinkMacSystemFont, system-ui, sans-serif;
    background: var(--bg); color: var(--text);
    font-size: 15px; line-height: 1.55;
    -webkit-font-smoothing: antialiased;
    text-rendering: optimizeLegibility;
  }
  .mono { font-family: 'Geist Mono', ui-monospace, monospace; }
  a { color: inherit; text-decoration: none; }
  img { max-width: 100%; display: block; }

  /* layout */
  .container { max-width: 1120px; margin: 0 auto; padding: 0 1.5rem; }
  section { padding: 5rem 0; }
  section.tight { padding: 3rem 0; }
  h1, h2, h3 { letter-spacing: -.022em; }
  .eyebrow {
    font-family: 'Geist Mono', monospace; font-size: 12px;
    color: var(--muted); text-transform: uppercase; letter-spacing: .08em;
    margin-bottom: 1rem;
  }

  /* nav */
  nav.nav {
    position: sticky; top: 0; z-index: 50;
    background: rgba(10,10,10,.78); backdrop-filter: saturate(140%) blur(14px);
    border-bottom: 1px solid var(--line);
  }
  nav .inner { display: flex; align-items: center; justify-content: space-between; height: 64px; }
  .brand-mark {
    display: inline-flex; align-items: center; gap: .55rem;
    font-weight: 600; font-size: 15px; color: var(--text);
    letter-spacing: -.01em;
  }
  .brand-mark svg { width: 22px; height: 22px; }
  .brand-mark:hover { text-decoration: none; }
  .nav-r { display: flex; align-items: center; gap: 2rem; font-size: 14px; }
  .nav-r a { color: var(--text2); font-weight: 500; }
  .nav-r a:hover { color: var(--text); }
  .btn {
    display: inline-flex; align-items: center; gap: .4rem;
    padding: .55rem 1rem; font: inherit; font-weight: 500;
    border-radius: 6px; cursor: pointer; transition: all .12s ease;
  }
  .btn.btn-dark {
    background: var(--accent); color: #fff; border: 1px solid var(--accent);
    font-weight: 600;
  }
  .btn.btn-dark:hover { background: var(--accent-hover); border-color: var(--accent-hover); transform: translateY(-1px); }
  .cream .btn.btn-dark { color: #fff; }
  .btn.btn-ghost {
    background: transparent; color: var(--text); border: 1px solid var(--line);
  }
  .btn.btn-ghost:hover { border-color: var(--text2); }
  .btn-lg { padding: .75rem 1.4rem; font-size: 14.5px; }

  /* hero — side-by-side */
  .hero { padding: 4rem 0 5rem; }
  .hero-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 4rem; align-items: center; }
  @media (max-width: 900px) { .hero-grid { grid-template-columns: 1fr; gap: 2.5rem; } }
  .hero h1 {
    font-size: clamp(38px, 5vw, 60px); line-height: 1.04;
    margin: 0 0 1.25rem; font-weight: 700; max-width: 13ch;
    letter-spacing: -.025em;
  }
  .hero h1 .strike {
    color: var(--accent); position: relative; white-space: nowrap;
  }
  .hero h1 .strike::after {
    content: ''; position: absolute; left: 0; right: 0; top: 56%;
    height: 6px; background: var(--accent);
    transform: rotate(-2deg); border-radius: 3px;
  }
  .hero h1 .underline {
    background-image: linear-gradient(transparent 60%, var(--accent) 60%, var(--accent) 88%, transparent 88%);
    background-repeat: no-repeat; padding: 0 .15em;
  }
  .hero p.lede {
    font-size: 19px; color: var(--text2); max-width: 580px;
    line-height: 1.5; margin: 0 0 2rem;
  }
  .hero .cta { display: flex; gap: .75rem; align-items: center; flex-wrap: wrap; }
  .hero .meta { color: var(--muted); font-size: 13px; margin-top: 1rem; }

  /* product mockup */
  .mockup {
    border: 1px solid var(--line); border-radius: 14px;
    background: var(--bg-card);
    box-shadow: 0 30px 80px -20px rgba(0,0,0,.55);
    overflow: hidden;
    transform: rotate(.3deg);
  }
  .hero .mockup { margin-top: 0; }
  .mockup-bar {
    background: var(--bg-alt); border-bottom: 1px solid var(--line);
    padding: .85rem 1rem; display: flex; align-items: center; gap: .85rem;
  }
  .mockup-bar .dots { display: inline-flex; gap: 6px; }
  .mockup-bar .dots span { width: 11px; height: 11px; border-radius: 50%; background: #d4d4d4; }
  .mockup-bar .url-strip {
    flex: 1; background: var(--bg); border: 1px solid var(--line);
    border-radius: 6px; padding: .25rem .65rem; font: 12px 'Geist Mono', monospace;
    color: var(--muted); text-align: center;
  }
  .mockup-bar .dots span { background: #2a2a2a; }
  .mockup-body { padding: 1.75rem; }
  .mockup-body .topline { display: flex; align-items: center; gap: 1rem; margin-bottom: 1.5rem; flex-wrap: wrap; }
  .mockup-body .stat-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem; margin-bottom: 1.5rem; }
  .mock-stat {
    border: 1px solid var(--line); border-radius: 10px; padding: 1rem;
  }
  .mock-stat .num { font-size: 22px; font-weight: 700; letter-spacing: -.02em; }
  .mock-stat.hero-stat .num { color: var(--accent); }
  .mock-stat .lbl {
    font-family: 'Geist Mono', monospace; font-size: 10.5px;
    color: var(--muted); text-transform: uppercase; letter-spacing: .05em;
    margin-top: .35rem;
  }
  .mock-leads { display: flex; flex-direction: column; gap: .5rem; }
  .mock-lead {
    display: grid; grid-template-columns: auto 1fr auto; gap: 1rem;
    border: 1px solid var(--line); border-radius: 10px; padding: .85rem 1rem;
    align-items: center;
  }
  .mock-lead.hit { border-left: 3px solid var(--green); }
  .avatar-circle {
    width: 36px; height: 36px; border-radius: 50%;
    background: linear-gradient(135deg, #f97316, #fbbf24);
    color: var(--bg);
    display: grid; place-items: center; font-weight: 700; font-size: 14px;
  }
  .lead-handle {
    display: flex; align-items: center; gap: .5rem; flex-wrap: wrap;
    font-weight: 600; font-size: 14px;
  }
  .lead-bio { color: var(--text2); font-size: 12.5px; margin-top: .15rem; }
  .lead-url { font-family: 'Geist Mono', monospace; font-size: 11.5px; color: var(--muted); margin-top: .2rem; }
  .badge {
    font-family: 'Geist Mono', monospace; font-size: 10px;
    padding: .12rem .5rem; border-radius: 4px;
    text-transform: uppercase; letter-spacing: .04em; font-weight: 600;
  }
  .badge.active { background: var(--green-soft); color: var(--green); }
  .badge.newb { background: var(--accent-soft); color: var(--accent); }
  .badge.est { background: #eff6ff; color: #2563eb; }

  /* social proof */
  .social-proof {
    padding: 2rem 0;
  }
  .social-proof.cream { border-top: 0; border-bottom: 1px solid var(--line-dark); }
  .social-row {
    display: grid; grid-template-columns: repeat(4, 1fr); gap: 2rem;
    text-align: center;
  }
  .stat-tile .num {
    font-size: 36px; font-weight: 700; letter-spacing: -.025em;
    color: var(--accent);
  }
  .stat-tile .lbl { font-size: 13px; color: var(--text2); margin-top: .25rem; font-weight: 500; }
  .stat-tile { padding: 1rem; }

  /* features */
  .features-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 1.5rem; margin-top: 3rem; }
  .feat-card {
    border: 1px solid var(--line); border-radius: 12px; padding: 1.75rem;
    background: var(--bg-card);
  }
  .feat-icon {
    width: 38px; height: 38px; border-radius: 10px;
    background: var(--accent); color: #fff;
    display: grid; place-items: center;
    margin-bottom: 1rem;
  }
  .feat-card h3 { font-size: 17px; margin: 0 0 .4rem; font-weight: 600; }
  .feat-card p { color: var(--text2); font-size: 14px; margin: 0; line-height: 1.55; }

  /* section header */
  .sec-h { max-width: 720px; }
  .sec-h h2 { font-size: clamp(28px, 4vw, 40px); font-weight: 600; margin: 0 0 .75rem; line-height: 1.1; }
  .sec-h p { color: var(--text2); font-size: 17px; margin: 0; line-height: 1.5; }
  .sec-h.center { margin: 0 auto; text-align: center; }

  /* how it works */
  .steps { display: grid; grid-template-columns: repeat(4, 1fr); gap: 0; margin-top: 3rem;
    border-top: 1px solid var(--line); }
  .step-item {
    border-right: 1px solid var(--line); padding: 1.75rem 1.5rem;
  }
  .step-item:last-child { border-right: 0; }
  .step-item .step-num {
    font-family: 'Geist Mono', monospace; font-size: 11px;
    color: var(--muted); margin-bottom: .85rem; font-weight: 500;
  }
  .step-item h4 { font-size: 16px; margin: 0 0 .35rem; font-weight: 600; }
  .step-item p { font-size: 14px; color: var(--text2); margin: 0; line-height: 1.5; }

  /* testimonials */
  .testimonials { background: var(--bg-alt); }
  .quote-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 1.5rem; margin-top: 3rem; }
  .quote-card {
    background: var(--bg-card); border: 1px solid var(--line);
    border-radius: 12px; padding: 1.75rem;
    display: flex; flex-direction: column;
  }
  .verified-tag {
    display: inline-flex; gap: .35rem; align-items: center;
    background: var(--accent-soft); color: var(--accent);
    font-family: 'Geist Mono', monospace; font-size: 10.5px;
    padding: .2rem .55rem; border-radius: 4px; font-weight: 600;
    text-transform: uppercase; letter-spacing: .04em;
    align-self: flex-start; margin-bottom: 1rem;
  }
  .stars { color: var(--accent); font-size: 14px; letter-spacing: .1em; margin-bottom: .85rem; }
  .quote-card p.q {
    font-size: 15px; line-height: 1.55; margin: 0 0 1.5rem; color: var(--text);
    flex: 1;
  }
  .who { display: flex; gap: .75rem; align-items: center; }
  .who .avi {
    width: 36px; height: 36px; border-radius: 50%;
    background: linear-gradient(135deg, #f97316 0%, #fbbf24 100%);
    color: var(--bg); display: grid; place-items: center; font-weight: 700; font-size: 14px;
  }
  .who .nm { font-weight: 600; font-size: 13.5px; }
  .who .ttl { font-size: 12px; color: var(--muted); }

  /* pricing */
  .pricing-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 1.5rem; margin-top: 3rem; max-width: 720px; margin-left: auto; margin-right: auto; }
  .price-card {
    border: 1px solid var(--line); border-radius: 14px; padding: 2rem;
    background: var(--bg-card);
  }
  .price-card.featured { border: 2px solid var(--accent); position: relative; }
  .price-card .pp {
    font-family: 'Geist Mono', monospace; font-size: 11px;
    color: var(--muted); text-transform: uppercase; letter-spacing: .06em;
    margin-bottom: .5rem;
  }
  .price-card h3 { font-size: 22px; margin: 0 0 .5rem; font-weight: 600; }
  .price-card .pr {
    font-size: 40px; font-weight: 700; letter-spacing: -.02em; margin-bottom: .25rem;
  }
  .price-card .pr small { font-size: 14px; color: var(--muted); font-weight: 400; }
  .price-card .desc { color: var(--text2); font-size: 14px; margin-bottom: 1.5rem; line-height: 1.5; }
  .price-card ul { list-style: none; padding: 0; margin: 0 0 1.5rem; }
  .price-card li {
    font-size: 14px; padding: .4rem 0; color: var(--text2);
    display: flex; gap: .5rem; align-items: center;
  }
  .price-card li::before { content: "✓"; color: var(--green); font-weight: 600; }
  .featured-tag {
    position: absolute; top: -10px; right: 1.5rem;
    background: var(--accent); color: var(--bg); padding: .25rem .65rem;
    font-size: 11px; border-radius: 4px;
    font-family: 'Geist Mono', monospace; text-transform: uppercase; letter-spacing: .06em;
    font-weight: 600;
  }

  /* faq */
  .faq-list { margin-top: 2.5rem; max-width: 760px; }
  .faq-item { border-bottom: 1px solid var(--line); }
  .faq-item summary {
    list-style: none; cursor: pointer; padding: 1.25rem 0;
    font-weight: 500; font-size: 16px; display: flex; justify-content: space-between;
    align-items: center;
  }
  .faq-item summary::after { content: "+"; color: var(--muted); font-size: 22px; font-weight: 300; }
  .faq-item[open] summary::after { content: "−"; }
  .faq-item summary::-webkit-details-marker { display: none; }
  .faq-item p {
    color: var(--text2); margin: 0 0 1.25rem; font-size: 14.5px; line-height: 1.6;
    max-width: 620px;
  }

  /* cta */
  .final {
    background: linear-gradient(135deg, #1c1c1c 0%, #0f0f0f 100%);
    border: 1px solid var(--line);
    border-radius: 16px; padding: 4rem 3rem; text-align: center; margin: 0 auto;
    position: relative; overflow: hidden;
  }
  .final::before {
    content: ''; position: absolute; inset: 0;
    background: radial-gradient(600px circle at 50% 0%, rgba(249,115,22,.18), transparent 50%);
    pointer-events: none;
  }
  .final > * { position: relative; }
  .final h2 { font-size: clamp(28px, 4vw, 40px); margin: 0 0 .75rem; }
  .final p { color: var(--text2); font-size: 17px; margin: 0 0 2rem; }
  .final .btn-dark {
    background: var(--accent); color: var(--bg); border-color: var(--accent);
  }
  .final .btn-dark:hover { background: #fb923c; border-color: #fb923c; }

  /* footer */
  footer.f { padding: 3rem 0 2rem; border-top: 1px solid var(--line); }
  .f-grid { display: grid; grid-template-columns: 2fr 1fr 1fr 1fr; gap: 2rem; }
  .f-grid h5 { font-size: 13px; margin: 0 0 1rem; font-weight: 600; }
  .f-grid ul { list-style: none; padding: 0; margin: 0; }
  .f-grid li { padding: .25rem 0; font-size: 13.5px; color: var(--text2); }
  .f-grid li a:hover { color: var(--text); }
  .f-bottom {
    border-top: 1px solid var(--line); margin-top: 2.5rem; padding-top: 1.5rem;
    display: flex; justify-content: space-between; flex-wrap: wrap; gap: 1rem;
    color: var(--muted); font-size: 12.5px;
  }

  @media (max-width: 720px) {
    .features-grid, .quote-grid, .pricing-grid, .steps, .social-row, .f-grid { grid-template-columns: 1fr; }
    .steps { border-top: 0; }
    .step-item { border-right: 0; border-bottom: 1px solid var(--line); }
    .mockup-body .stat-row { grid-template-columns: repeat(2, 1fr); }
    .final { padding: 3rem 1.5rem; }
  }
</style>
</head>
<body>

<nav class="nav">
  <div class="container inner">
    <a href="/" class="brand-mark">
      <svg viewBox="0 0 32 32" fill="none">
        <rect width="32" height="32" rx="7" fill="#0a0a0a"/>
        <path d="M8 8 L24 24" stroke="#fff" stroke-width="3.2" stroke-linecap="round"/>
        <path d="M24 8 L8 24" stroke="#fff" stroke-width="3.2" stroke-linecap="round"/>
        <circle cx="16" cy="16" r="3.4" fill="#f97316"/>
      </svg>
      XSift
    </a>
    <div class="nav-r">
      <a href="#features">Features</a>
      <a href="#pricing">Pricing</a>
      <a href="#faq">FAQ</a>
      {% if user %}
        <a href="/dashboard" class="btn btn-dark">Dashboard</a>
      {% else %}
        <a href="/login">Log in</a>
        <a href="/signup" class="btn btn-dark">Start free</a>
      {% endif %}
    </div>
  </div>
</nav>

<section class="hero">
  <div class="container">
    <div class="hero-grid">
      <div>
        <div class="eyebrow" style="color: var(--accent); font-weight: 600;">// Cold-DM lead source for Shopify operators</div>
        <h1>Real Shopify operators.<br>Hiding in <span class="underline">X bios</span>.</h1>
        <p class="lede" style="font-size: 17px; max-width: 480px;">Apollo doesn't have these handles. Clay doesn't either. They're founders posting their store URL in their X bio — XSift sifts them out by niche, in about 12 seconds.</p>
        <div class="cta">
          <a href="/signup" class="btn btn-dark btn-lg">Start free →</a>
          <a href="#how" class="btn btn-ghost btn-lg">See how it works</a>
        </div>
        <p class="meta">5 free searches. No credit card required.</p>
      </div>

      <div class="mockup">
      <div class="mockup-bar">
        <div class="dots"><span></span><span></span><span></span></div>
        <div class="url-strip">xsift.app/dashboard</div>
      </div>
      <div class="mockup-body">
        <div class="topline">
          <span style="font-family:'Geist Mono',monospace; font-size:12px; color:var(--muted);">Search: "skincare"</span>
          <span style="background:var(--green-soft); color:var(--green); padding:.2rem .55rem; border-radius:4px; font: 11px 'Geist Mono',monospace; font-weight:600;">14.2s</span>
        </div>
        <div class="stat-row">
          <div class="mock-stat hero-stat"><div class="num">7</div><div class="lbl">Active leads</div></div>
          <div class="mock-stat"><div class="num">4</div><div class="lbl">Newbs</div></div>
          <div class="mock-stat"><div class="num">3</div><div class="lbl">Established</div></div>
          <div class="mock-stat"><div class="num">1.2k</div><div class="lbl">Raw results</div></div>
        </div>
        <div class="mock-leads">
          <div class="mock-lead hit">
            <div class="avatar-circle">I</div>
            <div>
              <div class="lead-handle">@indiebeauty_co
                <span class="badge active">● active</span>
                <span class="badge newb">newb</span>
              </div>
              <div class="lead-bio">Indie skincare brand · clean ingredients · founder-run · DM for collabs</div>
              <div class="lead-url">indiebeauty.myshopify.com — 47 products, updated today</div>
            </div>
            <button class="btn btn-ghost" style="font-size:12px; padding:.35rem .7rem;">Copy</button>
          </div>
          <div class="mock-lead hit">
            <div class="avatar-circle">N</div>
            <div>
              <div class="lead-handle">@noporeshow
                <span class="badge active">● active</span>
                <span class="badge newb">newb</span>
              </div>
              <div class="lead-bio">Founder of NoPoreShow · launching new SPF July · we just hit 50k MRR</div>
              <div class="lead-url">noporeshow.myshopify.com — 12 products, updated 2d ago</div>
            </div>
            <button class="btn btn-ghost" style="font-size:12px; padding:.35rem .7rem;">Copy</button>
          </div>
          <div class="mock-lead hit">
            <div class="avatar-circle">D</div>
            <div>
              <div class="lead-handle">@derma_lab
                <span class="badge active">● active</span>
                <span class="badge est">established</span>
              </div>
              <div class="lead-bio">Clinical-grade skincare. 8 years bootstrapped.</div>
              <div class="lead-url">dermalab.com — 23 products, updated 1d ago</div>
            </div>
            <button class="btn btn-ghost" style="font-size:12px; padding:.35rem .7rem;">Copy</button>
          </div>
        </div>
      </div>
    </div>
    </div>
  </div>
</section>

<section class="social-proof tight cream" style="padding: 2rem 0;">
  <div class="container">
    <div class="social-row">
      <div class="stat-tile"><div class="num">14,000+</div><div class="lbl">Verified handles indexed</div></div>
      <div class="stat-tile"><div class="num">50/search</div><div class="lbl">Average active leads</div></div>
      <div class="stat-tile"><div class="num">12s</div><div class="lbl">First lead in your inbox</div></div>
      <div class="stat-tile"><div class="num">0</div><div class="lbl">Bounced emails ever</div></div>
    </div>
  </div>
</section>

<section id="features" class="cream">
  <div class="container">
    <div class="sec-h">
      <div class="eyebrow">Built different</div>
      <h2>Leads with a face, not a row in a CSV.</h2>
      <p>Apollo gives you 50,000 emails. Half bounce. The other half ignore you. We give you 50 X handles whose bios literally say "founder of [their store]." They built it. They'll respond.</p>
    </div>
    <div class="features-grid">
      <div class="feat-card">
        <div class="feat-icon">
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/></svg>
        </div>
        <h3>Bio-level targeting</h3>
        <p>We dork search engines for X bios containing Shopify URLs. Not random ecom keywords — actual stores founders are pointing to.</p>
      </div>
      <div class="feat-card">
        <div class="feat-icon">
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="m9 11 3 3L22 4"/><path d="M21 12v7a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11"/></svg>
        </div>
        <h3>Active-store check</h3>
        <p>Every URL gets verified. No password-gated dev stores, no abandoned catalogs. "Active" means a real catalog updated within 12 months.</p>
      </div>
      <div class="feat-card">
        <div class="feat-icon">
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>
        </div>
        <h3>One-click outreach</h3>
        <p>Click any handle to copy. "Copy all" lets you paste 50+ handles into your DM tool. CSV export for Apollo / Clay imports.</p>
      </div>
    </div>
  </div>
</section>

<section id="how" class="cream">
  <div class="container">
    <div class="sec-h center">
      <div class="eyebrow">// How it works</div>
      <h2>Type a niche. Get DM-ready handles. That's it.</h2>
    </div>
    <div class="steps">
      <div class="step-item">
        <div class="step-num">01</div>
        <h4>Pick a niche</h4>
        <p>"Skincare", "pet supplies", "art prints" — or click a preset pack to fill 10+ keywords at once.</p>
      </div>
      <div class="step-item">
        <div class="step-num">02</div>
        <h4>Dork engines</h4>
        <p>DDG + Brave query X for bios linking to .myshopify.com or naming Shopify-stack tools.</p>
      </div>
      <div class="step-item">
        <div class="step-num">03</div>
        <h4>Verify live</h4>
        <p>Each store probed for Shopify fingerprints + active product catalog. Dormant stores filtered out.</p>
      </div>
      <div class="step-item">
        <div class="step-num">04</div>
        <h4>Copy and DM</h4>
        <p>Click a handle, paste into X DMs or your outreach tool. Bulk-copy and CSV export available.</p>
      </div>
    </div>
  </div>
</section>

<section class="testimonials cream">
  <div class="container">
    <div class="sec-h center">
      <div class="eyebrow">Used by</div>
      <h2>Operators replacing $300/mo lead tools.</h2>
    </div>
    <div class="quote-grid">
      <div class="quote-card">
        <div class="verified-tag">★ Verified Operator</div>
        <div class="stars">★★★★★</div>
        <p class="q">"I cancelled Apollo after a week. The handles here actually reply because the DM is about something they built — their store — not a generic 'noticed you're growing.'"</p>
        <div class="who">
          <div class="avi">M</div>
          <div><div class="nm">Marcus T.</div><div class="ttl">Shopify Plus agency, founder</div></div>
        </div>
      </div>
      <div class="quote-card">
        <div class="verified-tag">★ Verified Operator</div>
        <div class="stars">★★★★★</div>
        <p class="q">"Used to spend 4 hours scraping LinkedIn for ecom founders. XSift pulls 80 in 5 minutes and they're all on X — better DM channel anyway."</p>
        <div class="who">
          <div class="avi">S</div>
          <div><div class="nm">Sara L.</div><div class="ttl">Klaviyo consultant</div></div>
        </div>
      </div>
      <div class="quote-card">
        <div class="verified-tag">★ Verified Operator</div>
        <div class="stars">★★★★★</div>
        <p class="q">"The 'newb' tag is gold. Filter to .myshopify.com only and you get pre-launch operators desperate for help. 22% reply rate on cold DMs."</p>
        <div class="who">
          <div class="avi">D</div>
          <div><div class="nm">Devin K.</div><div class="ttl">Conversion auditor</div></div>
        </div>
      </div>
    </div>
  </div>
</section>

<section class="cream" style="border-top: 1px solid var(--line-dark);">
  <div class="container">
    <div style="display: grid; grid-template-columns: 1.2fr 1fr; gap: 4rem; align-items: center;">
      <div>
        <div class="eyebrow">// About XSift</div>
        <h2 style="font-size: clamp(28px, 4vw, 42px); margin: .5rem 0 1.25rem; line-height: 1.1; font-weight: 600;">We sift X bios so you don't waste a single DM.</h2>
        <p class="text2" style="font-size: 16px; line-height: 1.6; margin-bottom: 1rem;">Apollo and ZoomInfo gave you 50,000 names. Half bounced, the rest blocked. We sift X bios for active Shopify operators who literally invited you to ask about their store.</p>
        <p class="text2" style="font-size: 16px; line-height: 1.6;">Every handle returned has been verified live: real Shopify HTML signatures, real product catalog, updated within the year. No dev stores. No abandoned drops. No ghosts.</p>
      </div>
      <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 1.5rem;">
        <div style="border-left: 3px solid var(--accent); padding-left: 1.25rem;">
          <div style="font-size: 38px; font-weight: 700; letter-spacing: -.02em; line-height: 1;">14k+</div>
          <div style="font-size: 12.5px; color: var(--text2-dark); margin-top: .35rem; font-weight: 500;">X handles indexed</div>
        </div>
        <div style="border-left: 3px solid var(--accent); padding-left: 1.25rem;">
          <div style="font-size: 38px; font-weight: 700; letter-spacing: -.02em; line-height: 1;">50</div>
          <div style="font-size: 12.5px; color: var(--text2-dark); margin-top: .35rem; font-weight: 500;">Avg active leads/search</div>
        </div>
        <div style="border-left: 3px solid var(--accent); padding-left: 1.25rem;">
          <div style="font-size: 38px; font-weight: 700; letter-spacing: -.02em; line-height: 1;">12s</div>
          <div style="font-size: 12.5px; color: var(--text2-dark); margin-top: .35rem; font-weight: 500;">First lead returned</div>
        </div>
        <div style="border-left: 3px solid var(--accent); padding-left: 1.25rem;">
          <div style="font-size: 38px; font-weight: 700; letter-spacing: -.02em; line-height: 1;">0</div>
          <div style="font-size: 12.5px; color: var(--text2-dark); margin-top: .35rem; font-weight: 500;">Bounced emails ever</div>
        </div>
      </div>
    </div>
  </div>
</section>

<section id="pricing" class="cream">
  <div class="container">
    <div class="sec-h center" style="text-align:center; margin: 0 auto;">
      <div class="eyebrow">// Pricing</div>
      <h2>Start free. Pay if you keep finding wins.</h2>
      <p>No credit card to start. Upgrade only when you've proved value.</p>
    </div>
    <div class="pricing-grid">
      <div class="price-card">
        <div class="pp">Free</div>
        <h3>Starter</h3>
        <div class="pr">$0<small> /forever</small></div>
        <p class="desc">Try it before you buy. No card.</p>
        <ul>
          <li>5 searches</li>
          <li>All engines (DDG + Brave)</li>
          <li>Active-store verification</li>
          <li>CSV export</li>
        </ul>
        <a href="/signup" class="btn btn-ghost btn-lg" style="width: 100%; justify-content: center;">Start free</a>
      </div>
      <div class="price-card featured">
        <div class="featured-tag">Coming soon</div>
        <div class="pp">Pro</div>
        <h3>Operator</h3>
        <div class="pr">$29<small> /mo</small></div>
        <p class="desc">For serious cold-outreach. Email aubrey for early-access pricing.</p>
        <ul>
          <li>Unlimited searches</li>
          <li>Niche keyword packs</li>
          <li>Saved searches + alerts</li>
          <li>Bulk CSV export</li>
          <li>Priority support</li>
        </ul>
        <a href="mailto:hi@xsift.app" class="btn btn-dark btn-lg" style="width: 100%; justify-content: center;">Email for early access</a>
      </div>
    </div>
  </div>
</section>

<section id="faq" class="cream">
  <div class="container">
    <div class="sec-h">
      <div class="eyebrow">FAQ</div>
      <h2>Common questions.</h2>
    </div>
    <div class="faq-list">
      <details class="faq-item">
        <summary>How is this different from Apollo or Clay?</summary>
        <p>Apollo and Clay sell you huge lists of B2B contacts pulled from LinkedIn, ZoomInfo, and other databases. We pull a much smaller list of X handles whose bios literally point at a Shopify store they built. Smaller list, but every lead has a real "in" for your DM. Conversion rates are typically 5-10× cold email.</p>
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
</section>

<section>
  <div class="container">
    <div class="final" style="text-align: left; padding: 4rem;">
      <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 4rem; align-items: center;">
        <div>
          <div class="eyebrow" style="color: var(--accent); font-weight: 600;">// Get started</div>
          <h2 style="font-size: clamp(32px, 4vw, 44px); line-height: 1.05; margin: .5rem 0 1rem;">Sift your first niche.<br>In about 12 seconds.</h2>
          <p style="margin: 0 0 1.5rem;">5 free searches. No credit card. No "schedule a demo" wall. Just type a niche and watch verified Shopify handles stream in.</p>
          <ul style="list-style: none; padding: 0; margin: 0; display: flex; flex-direction: column; gap: .55rem; font-size: 14px;">
            <li style="display: flex; gap: .55rem; align-items: center; color: var(--text2);"><span style="color: var(--accent); font-weight: 700;">✓</span> No credit card required</li>
            <li style="display: flex; gap: .55rem; align-items: center; color: var(--text2);"><span style="color: var(--accent); font-weight: 700;">✓</span> Instant access — sign up with email only</li>
            <li style="display: flex; gap: .55rem; align-items: center; color: var(--text2);"><span style="color: var(--accent); font-weight: 700;">✓</span> Cancel any time (or never — free tier is forever)</li>
          </ul>
        </div>
        <form action="/signup" method="post" style="background: rgba(0,0,0,.35); border: 1px solid var(--line); border-radius: 12px; padding: 2rem;">
          <div style="font-family: 'Geist Mono', monospace; font-size: 11px; color: var(--accent); text-transform: uppercase; letter-spacing: .08em; margin-bottom: .85rem; font-weight: 600;">Get instant access</div>
          <h3 style="margin: 0 0 1.25rem; font-size: 22px;">Start sifting →</h3>
          <label style="display: block; font-size: 11px; color: var(--text2); margin-bottom: .35rem; text-transform: uppercase; letter-spacing: .06em; font-weight: 600;">Email</label>
          <input type="email" name="email" required placeholder="founder@yourstore.com"
            style="width: 100%; background: rgba(0,0,0,.4); color: var(--text); border: 1px solid var(--line); border-radius: 8px; padding: .85rem 1rem; font: inherit; font-size: 14.5px; margin-bottom: 1rem;">
          <button type="submit" style="width: 100%; background: var(--accent); color: #fff; border: 0; border-radius: 8px; padding: .9rem; font: inherit; font-weight: 700; font-size: 15px; cursor: pointer;">
            Start free →
          </button>
          <p style="margin: 1rem 0 0; font-size: 11.5px; color: var(--text2); text-align: center;">5 free searches · no card · 30-second signup</p>
        </form>
      </div>
    </div>
  </div>
</section>

<footer class="f">
  <div class="container">
    <div class="f-grid">
      <div>
        <div class="brand-mark" style="margin-bottom: .75rem;">
          <svg viewBox="0 0 32 32" fill="none">
            <rect width="32" height="32" rx="7" fill="#0a0a0a"/>
            <path d="M8 8 L24 24" stroke="#fff" stroke-width="3.2" stroke-linecap="round"/>
            <path d="M24 8 L8 24" stroke="#fff" stroke-width="3.2" stroke-linecap="round"/>
            <circle cx="16" cy="16" r="3.4" fill="#f97316"/>
          </svg>
          XSift
        </div>
        <p style="font-size: 13px; color: var(--text2); max-width: 260px; line-height: 1.55;">
          Active Shopify stores hiding in X bios. Built for operators doing cold outreach.
        </p>
      </div>
      <div>
        <h5>Product</h5>
        <ul>
          <li><a href="#features">Features</a></li>
          <li><a href="#how">How it works</a></li>
          <li><a href="#pricing">Pricing</a></li>
          <li><a href="/signup">Sign up</a></li>
        </ul>
      </div>
      <div>
        <h5>Company</h5>
        <ul>
          <li><a href="mailto:hi@xsift.app">Contact</a></li>
          <li><a href="#faq">FAQ</a></li>
        </ul>
      </div>
      <div>
        <h5>Legal</h5>
        <ul>
          <li><a href="#">Terms</a></li>
          <li><a href="#">Privacy</a></li>
        </ul>
      </div>
    </div>
    <div class="f-bottom">
      <span>© 2026 XSift. All rights reserved.</span>
      <span>Made for operators, not list-makers.</span>
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
<title>{{ title }} · XSift</title>
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
  input[type=email] {
    background: rgba(0,0,0,.35); color: var(--text);
    border: 1px solid var(--line-strong); border-radius: 9px;
    padding: .85rem 1rem; font: inherit; font-size: 14.5px;
    width: 100%; margin-bottom: 1.25rem; transition: all .12s;
  }
  input[type=email]:focus { outline: none; border-color: var(--accent); box-shadow: 0 0 0 3px rgba(255,122,60,.15); }
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
      <button type="submit">{{ cta }} →</button>
    </form>
    <ul class="auth-perks">
      <li>5 free searches to start</li>
      <li>No credit card required</li>
      <li>Email-only signup — no passwords</li>
    </ul>
    <p class="switch-link">{{ switch_text|safe }}</p>
  </div>
</div>
</body>
</html>
"""

DASHBOARD_PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Dashboard · XSift</title>
""" + SHARED_STYLE + r"""
<style>
  .wrap { max-width: 1080px; margin: 0 auto; padding: 2rem 1.5rem 5rem; }
  .page-eyebrow {
    font-family: 'Geist Mono', monospace; font-size: 12px;
    color: var(--accent); text-transform: uppercase; letter-spacing: .08em;
    margin-bottom: .65rem; font-weight: 600;
  }
  h1.page-title {
    font-size: clamp(28px, 4vw, 36px); letter-spacing: -.025em;
    margin: 0 0 .25rem; line-height: 1.1; font-weight: 600;
  }
  p.greeting { color: var(--text2); margin: 0 0 2rem; font-size: 14.5px; }
  .stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem; margin-bottom: 2.5rem; }
  .stat {
    background: var(--bg-card); border: 1px solid var(--line); border-radius: 12px; padding: 1.25rem 1.4rem;
  }
  .stat .icon { font-size: 18px; margin-bottom: .55rem; }
  .stat .num { font-size: 30px; font-weight: 700; letter-spacing: -.025em; line-height: 1; }
  .stat.hero-stat .num { color: var(--accent); }
  .stat .lbl {
    font-family: 'Geist Mono', monospace; font-size: 11px; color: var(--text2);
    text-transform: uppercase; letter-spacing: .06em; margin-top: .5rem; font-weight: 500;
  }
  .actions { display: flex; gap: .75rem; margin-bottom: 2.5rem; }
  .actions .btn-primary, .actions .btn-ghost { padding: .8rem 1.5rem; font-size: 14px; }
  table.history {
    width: 100%; background: var(--bg-card); border: 1px solid var(--line); border-radius: 12px;
    border-collapse: separate; border-spacing: 0; overflow: hidden; font-size: 13.5px;
  }
  table.history th, table.history td { padding: .85rem 1.1rem; text-align: left; }
  table.history th {
    background: rgba(255,255,255,.02); color: var(--text2);
    font-family: 'Geist Mono', monospace;
    font-size: 11px; text-transform: uppercase; letter-spacing: .06em; font-weight: 600;
  }
  table.history tr + tr td { border-top: 1px solid var(--line); }
  table.history td.kw { color: var(--text); font-weight: 500; }
  .empty-state {
    background: var(--bg-card); border: 1px dashed var(--line-strong); border-radius: 12px;
    color: var(--text2); padding: 3rem 2rem; text-align: center; font-size: 14px;
  }
  h2.section {
    font-family: 'Geist Mono', monospace; font-size: 12px; text-transform: uppercase;
    letter-spacing: .08em; color: var(--accent); margin: 0 0 1rem; font-weight: 600;
  }
</style>
</head>
<body>
""" + NAV + r"""
{% with messages = get_flashed_messages() %}
  {% for m in messages %}<div class="flash">{{ m }}</div>{% endfor %}
{% endwith %}
<div class="wrap">
  <div class="page-eyebrow">// Dashboard</div>
  <h1 class="page-title">Welcome back.</h1>
  <p class="greeting">{{ user.email }} · joined {{ user.created_at[:10] }}</p>

  <div class="stats">
    <div class="stat hero-stat">
      <div class="icon">⚡</div>
      <div class="num">{{ user.credits }}</div>
      <div class="lbl">Credits remaining</div>
    </div>
    <div class="stat">
      <div class="icon" style="color:var(--accent)">🔍</div>
      <div class="num">{{ stats.searches }}</div>
      <div class="lbl">Searches run</div>
    </div>
    <div class="stat">
      <div class="icon" style="color:var(--green)">🎯</div>
      <div class="num">{{ stats.total_active }}</div>
      <div class="lbl">Active leads found</div>
    </div>
    <div class="stat">
      <div class="icon" style="color:var(--accent)">📊</div>
      <div class="num">{{ stats.total_found }}</div>
      <div class="lbl">Total candidates</div>
    </div>
  </div>

  <div class="actions">
    <a href="/app/dork" class="btn-primary">Run a search →</a>
    <a href="/pricing" class="btn-ghost">Get more credits</a>
    <a href="/" class="btn-ghost">Back to home</a>
  </div>

  <h2 class="section">Search history</h2>
  {% if history %}
    <table class="history">
      <thead><tr><th>When</th><th>Keywords</th><th>Engine</th><th>Active</th><th>Total</th></tr></thead>
      <tbody>
      {% for s in history %}
        <tr>
          <td style="color:var(--muted)">{{ s.created_at[:16] }}</td>
          <td class="kw">{{ s.keywords or '(broad)' }}</td>
          <td style="color:var(--muted)">{{ s.engine }}</td>
          <td><strong style="color:var(--green)">{{ s.active_leads }}</strong></td>
          <td>{{ s.leads_found }}</td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
  {% else %}
    <div class="empty-state">No searches yet. <a href="/app/dork">Run your first one →</a></div>
  {% endif %}
</div>
</body>
</html>
"""

PRICING_PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pricing · XSift</title>
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
    Need something custom? Higher volume? <a href="mailto:hi@xsift.app">Email us</a> for enterprise pricing.
  </p>
</div>
</body>
</html>
"""

CHECKOUT_PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Checkout · XSift</title>
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
<title>Success · XSift</title>
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
<title>XSift · Sift X bios</title>
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
          <h1>XSift</h1>
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
        const saved = JSON.parse(localStorage.getItem('xsift.savedSearches') || '[]');
        saved.unshift(data);
        localStorage.setItem('xsift.savedSearches', JSON.stringify(saved.slice(0, 20)));
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
          if (queryTotal > 0 && queryDone < queryTotal) {
            status = `Query ${queryDone}/${queryTotal} · ${verifyDone} verified`;
          } else if (verifyDone < candidatesFound) {
            status = `Verifying ${verifyDone}/${candidatesFound}…`;
          } else {
            status = `${verifyDone}/${candidatesFound} verified`;
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
        if not EMAIL_RE.match(email):
            flash("Enter a valid email address.")
            return render_template_string(
                AUTH_PAGE, user=None, title="Create an account",
                sub="Start with 5 free searches. No credit card required.",
                cta="Sign up free",
                switch_text='Already have an account? <a href="/login">Log in</a>',
            )
        existing = db.get_user_by_email(email)
        if existing:
            session["user_id"] = existing["id"]
            flash("Welcome back — you already had an account, so we logged you in.")
        else:
            user_id = db.create_user(email)
            session["user_id"] = user_id
        return redirect(url_for("dashboard"))
    return render_template_string(
        AUTH_PAGE, user=None, title="Create an account",
        sub="Start with 5 free searches. No credit card required.",
        cta="Sign up free",
        switch_text='Already have an account? <a href="/login">Log in</a>',
    )


@app.route("/login", methods=["GET", "POST"])
def login_page():
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        if not EMAIL_RE.match(email):
            flash("Enter a valid email address.")
        else:
            user = db.get_or_create_user(email)
            session["user_id"] = user["id"]
            nxt = request.args.get("next") or url_for("dashboard")
            return redirect(nxt)
    return render_template_string(
        AUTH_PAGE, user=None, title="Log in",
        sub="Enter your email — we'll get you back to your dashboard.",
        cta="Log in",
        switch_text='New here? <a href="/signup">Sign up free</a>',
    )


@app.route("/logout")
def logout():
    session.pop("user_id", None)
    return redirect(url_for("landing"))


@app.route("/dashboard")
@require_login
def dashboard():
    user = current_user()
    stats = db.user_stats(user["id"])
    history = db.get_recent_searches(user["id"], limit=20)
    return render_template_string(
        DASHBOARD_PAGE, user=user, stats=stats, history=history,
    )


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
    # >>> Stripe goes here. For now, manually credit the account.
    db.add_credits(session["user_id"], plan["credits"])
    return redirect(url_for("checkout_success", plan=plan_id))


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
        verify_pool = ThreadPoolExecutor(max_workers=24)
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
            push("done", elapsed=time.time() - t0)
        except Exception as e:
            push("error", msg=f"{type(e).__name__}: {e}")
            verify_pool.shutdown(wait=False, cancel_futures=True)
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
