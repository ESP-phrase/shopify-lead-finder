"""Web UI for the Shopify lead-finder pipeline."""

import asyncio
import csv
import io
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from flask import Flask, Response, render_template_string, request

from scraper import check
from bio_dork import (
    dork_queries, search_dorks, verify_all, PROXY_URL,
    BRAVE_API_KEY, GOOGLE_API_KEY, GOOGLE_CSE_ID,
)

# x_scraper pulls heavy deps (twscrape) — lazy-import inside the /x route
ACCOUNTS_FILE = "accounts.txt"

app = Flask(__name__)

PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Lead Finder · Shopify on X</title>
<link rel="preconnect" href="https://rsms.me/">
<link rel="stylesheet" href="https://rsms.me/inter/inter.css">
<style>
  :root {
    --bg: #07080c;
    --bg2: #0c0e14;
    --panel: rgba(22, 26, 36, 0.6);
    --panel-solid: #161a24;
    --panel2: #1c2230;
    --line: rgba(255, 255, 255, 0.06);
    --line-strong: rgba(255, 255, 255, 0.12);
    --text: #f0f3f8;
    --text2: #c1c8d4;
    --muted: #6b7382;
    --accent: #a78bfa;
    --accent2: #22d3ee;
    --green: #34d399;
    --orange: #fb923c;
    --red: #f87171;
    --gradient: linear-gradient(135deg, #a78bfa 0%, #22d3ee 100%);
    --gradient-soft: linear-gradient(135deg, rgba(167,139,250,.12), rgba(34,211,238,.08));
    color-scheme: dark;
  }
  * { box-sizing: border-box; }
  html, body { margin: 0; }
  body {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, system-ui, sans-serif;
    font-feature-settings: 'cv11', 'ss01', 'ss03';
    background: var(--bg);
    background-image:
      radial-gradient(800px circle at 0% 0%, rgba(167,139,250,.08), transparent 40%),
      radial-gradient(900px circle at 100% 0%, rgba(34,211,238,.06), transparent 40%);
    color: var(--text);
    min-height: 100vh;
    font-size: 14px; line-height: 1.55;
    -webkit-font-smoothing: antialiased;
  }
  @supports (font-variation-settings: normal) {
    body { font-family: 'Inter var', sans-serif; }
  }
  .wrap { max-width: 1180px; margin: 0 auto; padding: 1.25rem 1.5rem 5rem; }

  header.top {
    display: flex; align-items: center; justify-content: space-between;
    padding: .25rem 0 1.5rem;
  }
  .brand { display: flex; align-items: center; gap: .65rem; }
  .logo {
    width: 32px; height: 32px; border-radius: 8px;
    background: var(--gradient);
    display: grid; place-items: center;
    font-weight: 800; color: #07080c; font-size: 14px;
    box-shadow: 0 4px 16px rgba(167,139,250,.25);
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
    background: var(--panel); backdrop-filter: blur(24px);
    border: 1px solid var(--line); border-radius: 16px;
    padding: 1.6rem; position: relative; overflow: hidden;
  }
  .hero::before {
    content: ''; position: absolute; inset: 0;
    background: var(--gradient-soft); opacity: .8; pointer-events: none;
  }
  .hero > * { position: relative; }
  .hero h2 { margin: 0 0 .25rem; font-size: 20px; font-weight: 600; letter-spacing: -.015em; }
  .hero p.lead { margin: 0 0 1.25rem; color: var(--text2); font-size: 13.5px; }

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
    box-shadow: 0 0 0 3px rgba(167,139,250,.15);
  }
  textarea { min-height: 120px; font: 13px ui-monospace, "JetBrains Mono", monospace; resize: vertical; }
  input[type=number] { width: 5.5rem; }
  .select {
    background: rgba(0,0,0,.25); color: var(--text);
    border: 1px solid var(--line-strong); border-radius: 9px;
    padding: .65rem .85rem; font: inherit; font-size: 13.5px; cursor: pointer;
  }
  .select:focus { outline: none; border-color: var(--accent); box-shadow: 0 0 0 3px rgba(167,139,250,.15); }
  .controls { display: flex; flex-wrap: wrap; gap: 1rem; align-items: end; margin-top: 1rem; }
  .controls > * { min-width: 0; }
  .controls > .primary { margin-left: auto; }
  .toggle {
    display: flex; align-items: center; gap: .55rem; font-size: 13px; color: var(--text2);
    user-select: none; cursor: pointer; padding: .65rem .85rem;
    background: rgba(0,0,0,.25); border: 1px solid var(--line-strong); border-radius: 9px;
  }
  .toggle input { accent-color: var(--accent); width: 14px; height: 14px; }

  button.primary {
    padding: .75rem 1.5rem; font: inherit; font-weight: 600; cursor: pointer;
    background: var(--gradient); color: #07080c; border: 0; border-radius: 9px;
    box-shadow: 0 4px 18px rgba(167,139,250,.3);
    transition: transform .12s, box-shadow .15s, opacity .15s;
  }
  button.primary:hover { transform: translateY(-1px); box-shadow: 0 6px 24px rgba(167,139,250,.45); }
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
  .stat.hero-stat .num {
    background: var(--gradient); -webkit-background-clip: text; background-clip: text;
    color: transparent;
  }
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
  .copy:hover { background: rgba(255,255,255,.08); color: var(--text2); }
  .copy.copied { color: var(--green); border-color: var(--green); }
  .badges { display: flex; gap: .3rem; flex-wrap: wrap; }
  .badge {
    font-size: 10px; padding: .15rem .55rem; border-radius: 999px;
    text-transform: uppercase; letter-spacing: .05em; font-weight: 700;
    display: inline-flex; align-items: center; gap: .25rem;
  }
  .badge.active { background: rgba(52,211,153,.12); color: var(--green); border: 1px solid rgba(52,211,153,.25); }
  .badge.dormant { background: rgba(255,255,255,.04); color: var(--muted); border: 1px solid var(--line); }
  .badge.notshop { background: rgba(248,113,113,.12); color: var(--red); border: 1px solid rgba(248,113,113,.2); }
  .badge.newb { background: rgba(251,146,60,.12); color: var(--orange); border: 1px solid rgba(251,146,60,.25); }
  .badge.est { background: rgba(34,211,238,.12); color: var(--accent2); border: 1px solid rgba(34,211,238,.25); }
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
    <div class="brand">
      <div class="logo">L</div>
      <div>
        <h1>Lead Finder</h1>
        <p>Active Shopify stores in X bios — for cold outreach</p>
      </div>
    </div>
    <span class="proxy-pill {{ '' if proxy_on else 'off' }}">
      <span class="dot"></span>
      {{ 'proxy on' if proxy_on else 'no proxy' }}
    </span>
  </header>

  <nav class="tabs">
    <a href="/dork" class="{{ 'active' if mode == 'dork' else '' }}">Bio dork</a>
    <a href="/" class="{{ 'active' if mode == 'domains' else '' }}">Domain checker</a>
    <a href="/x" class="{{ 'active' if mode == 'x' else '' }}">X login</a>
  </nav>

  {% if mode == 'dork' %}
    <div class="hero">
      <h2>Find Shopify stores hidden in X bios</h2>
      <p class="lead">DDG-dorks <code>x.com</code>, extracts shop URLs, verifies each store is active. No login.</p>
      <form method="post" id="dorkForm">
        <label for="kw">Keywords (comma-separated, optional)</label>
        <input type="text" name="keywords" id="kw" placeholder="klaviyo, dtc, dropshipping, skincare, pet" value="{{ submitted_keywords or '' }}" autofocus>
        <div class="controls">
          <span>
            <label>Search engine</label>
            <select name="engine" class="select">
              <option value="ddg" {{ 'selected' if engine=='ddg' else '' }}>DuckDuckGo (free)</option>
              <option value="brave" {{ 'selected' if engine=='brave' else '' }} {{ '' if brave_ready else 'disabled' }}>Brave{{ '' if brave_ready else ' — add BRAVE_API_KEY' }}</option>
              <option value="google" {{ 'selected' if engine=='google' else '' }} {{ '' if google_ready else 'disabled' }}>Google CSE{{ '' if google_ready else ' — add GOOGLE_API_KEY+CSE_ID' }}</option>
              <option value="both" {{ 'selected' if engine=='both' else '' }} {{ '' if brave_ready else 'disabled' }}>DDG + Brave</option>
              <option value="all" {{ 'selected' if engine=='all' else '' }} {{ '' if (brave_ready and google_ready) else 'disabled' }}>All engines (max coverage)</option>
            </select>
          </span>
          <span>
            <label>Per query</label>
            <input type="number" name="per_query" min="5" max="200" value="{{ per_query or 30 }}">
          </span>
          <label class="toggle">
            <input type="checkbox" name="broad" value="1" {{ 'checked' if broad else '' }}>
            <span>Include custom-domain stores</span>
          </label>
          <label class="toggle">
            <input type="checkbox" name="show_noise" value="1" {{ 'checked' if show_noise else '' }}>
            <span>Show non-Shopify hits</span>
          </label>
          <button type="submit" class="primary" id="goBtn">Search</button>
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
          <span class="grow"></span>
          <button type="button" class="ghost" id="copyAll">Copy @handles</button>
          <a class="ghost" href="data:text/csv;charset=utf-8,{{ csv_url|urlencode }}" download="leads.csv">Export CSV</a>
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
      const overlay = $('#loadingOverlay');
      const label = $('#loadingLabel');
      const sub = $('#loadingSub');
      if (form && goBtn && overlay) {
        form.addEventListener('submit', () => {
          goBtn.disabled = true;
          goBtn.textContent = 'Searching…';
          overlay.classList.add('on');
          const kw = $('#kw').value.trim();
          label.textContent = kw ? `Searching X bios for "${kw}"…` : 'Searching X bios (broad sweep)…';
          let dots = 0;
          const phrases = ['Querying DuckDuckGo via proxy', 'Extracting Shopify URLs from snippets', 'Verifying stores are active'];
          let phraseIdx = 0;
          setInterval(() => {
            dots = (dots + 1) % 4;
            sub.textContent = phrases[Math.floor(Date.now() / 3000) % phrases.length] + '.'.repeat(dots);
          }, 400);
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


@app.route("/", methods=["GET", "POST"])
def index():
    submitted = request.form.get("domains", "") if request.method == "POST" else ""
    results = None
    if request.method == "POST":
        domains = parse_domains(submitted)
        if domains:
            results = run_checks(domains)
    return render_template_string(
        PAGE, mode="domains", submitted=submitted, results=results,
        proxy_on=bool(PROXY_URL),
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
        PAGE, mode="x", accounts_ok=accounts_ok,
        submitted_keywords=submitted_keywords, per_keyword=per_keyword,
        results=results, x_error=x_error, proxy_on=bool(PROXY_URL),
    )


@app.route("/dork", methods=["GET", "POST"])
def dork():
    submitted_keywords = ""
    per_query = 50
    broad = False
    show_noise = False
    engine = "ddg"
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
        if engine not in ("ddg", "brave", "google", "both", "all"):
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
        PAGE, mode="dork",
        submitted_keywords=submitted_keywords, per_query=per_query, broad=broad,
        show_noise=show_noise, engine=engine,
        brave_ready=bool(BRAVE_API_KEY),
        google_ready=bool(GOOGLE_API_KEY and GOOGLE_CSE_ID),
        results=results, elapsed=elapsed, dork_error=dork_error,
        active_count=active_count, newbs=newbs, established=established,
        total_shopify=total_shopify, dormant_count=dormant_count,
        csv_url=csv_url, proxy_on=bool(PROXY_URL), search_log=search_log,
    )


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)
