"""
scraper.py — Async Playwright scraper with automatic Shopify detection.

Strategy per target:
  1. Check if <base_url>/products.json returns valid Shopify JSON.
  2. If yes  → fetch all products via the JSON API (fast, structured, no JS needed).
  3. If no   → fall back to headless Playwright with stealth (for JS-rendered pages).

Snapshot content format:
  - Shopify targets  : JSON string  — list of {title, variants:[{title, price}]}
  - Playwright targets: plain text  — extracted visible text of the pricing section
"""

import asyncio
import json
import logging
import random
import re
import urllib.parse
import urllib.robotparser

import requests
from playwright.async_api import async_playwright, Browser, Page, TimeoutError as PWTimeout
from playwright_stealth import Stealth

logger = logging.getLogger(__name__)

_stealth = Stealth()  # singleton — safe to reuse across pages

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NAVIGATION_TIMEOUT_MS = 60_000  # Playwright page load timeout (ms)
SHOPIFY_TIMEOUT_S = 15           # requests timeout for Shopify API calls (s)
SHOPIFY_PAGE_LIMIT = 250         # max products per API page (Shopify maximum)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) "
    "Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) "
    "Gecko/20100101 Firefox/125.0",
]

BLOCK_PHRASES = [
    "just a moment",
    "enable javascript",
    "checking your browser",
    "ddos-guard",
    "access denied",
    "403 forbidden",
    "captcha",
]


# ---------------------------------------------------------------------------
# Shopify detection & scraping
# ---------------------------------------------------------------------------

def _base_url(url: str) -> str:
    """Return scheme + netloc only, e.g. 'https://gumus.eg'."""
    parsed = urllib.parse.urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def _is_shopify(base: str) -> bool:
    """
    Return True if <base>/products.json looks like a Shopify products endpoint.
    Uses a lightweight probe (limit=1) to avoid downloading the full catalogue.
    """
    probe_url = f"{base}/products.json?limit=1"
    try:
        resp = requests.get(
            probe_url,
            timeout=SHOPIFY_TIMEOUT_S,
            headers={"User-Agent": random.choice(USER_AGENTS)},
            allow_redirects=True,
        )
        if resp.status_code != 200:
            return False
        data = resp.json()
        return isinstance(data, dict) and "products" in data
    except Exception:
        return False


def _fetch_shopify_products(base: str) -> list[dict]:
    """
    Page through <base>/products.json and return every product dict.
    Stops when a page returns fewer than SHOPIFY_PAGE_LIMIT products.
    """
    all_products: list[dict] = []
    page = 1

    while True:
        url = f"{base}/products.json?limit={SHOPIFY_PAGE_LIMIT}&page={page}"
        try:
            resp = requests.get(
                url,
                timeout=SHOPIFY_TIMEOUT_S,
                headers={"User-Agent": random.choice(USER_AGENTS)},
            )
            resp.raise_for_status()
            batch = resp.json().get("products", [])
        except Exception as exc:
            logger.error("Shopify API error on page %d for %s: %s", page, base, exc)
            break

        all_products.extend(batch)
        logger.debug("Shopify page %d: %d products (total so far: %d)", page, len(batch), len(all_products))

        if len(batch) < SHOPIFY_PAGE_LIMIT:
            break  # last page
        page += 1

    return all_products


def _extract_shopify_snapshot(products: list[dict]) -> str:
    """
    Convert raw Shopify product dicts into a compact, deterministic JSON string
    suitable for storage and diffing.

    Output schema (list, sorted by product title):
      [
        {
          "title": "Product Name",
          "variants": [
            {"title": "Size / Color", "price": "299.00"},
            ...
          ]
        },
        ...
      ]

    Only title and price are captured — fields like inventory, images, and
    timestamps are excluded to keep snapshots stable and noise-free.
    """
    snapshot: list[dict] = []

    for product in products:
        title = (product.get("title") or "").strip()
        variants = []
        for v in product.get("variants", []):
            variant_title = (v.get("title") or "").strip()
            price = (v.get("price") or "").strip()
            if price:  # skip variants with no price data
                variants.append({"title": variant_title, "price": price})
        if title and variants:
            snapshot.append({"title": title, "variants": variants})

    # Sort by product title so ordering differences don't create false diffs
    snapshot.sort(key=lambda p: p["title"].lower())

    return json.dumps(snapshot, ensure_ascii=False, indent=2)


def scrape_shopify(url: str) -> str | None:
    """
    Fetch all products from a Shopify store and return a structured JSON string.
    Returns None on failure.
    """
    base = _base_url(url)
    logger.info("Shopify API detected — scraping %s via products.json", base)

    products = _fetch_shopify_products(base)
    if not products:
        logger.warning("No products returned from Shopify API for %s", base)
        return None

    snapshot = _extract_shopify_snapshot(products)
    logger.info("Shopify: %d products extracted from %s (%d chars)", len(products), base, len(snapshot))
    return snapshot


# ---------------------------------------------------------------------------
# Playwright fallback (non-Shopify / JS-rendered pages)
# ---------------------------------------------------------------------------

def _clean_text(raw: str) -> str:
    """Normalise whitespace in extracted page text."""
    text = re.sub(r"\n{3,}", "\n\n", raw)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


def _is_blocked(title: str, body: str) -> bool:
    """Return True if the page looks like a bot-challenge page."""
    combined = (title + " " + body[:500]).lower()
    return any(phrase in combined for phrase in BLOCK_PHRASES)


async def _extract_pricing_text(page: Page) -> str:
    """
    Pull visible text from the page, preferring pricing sections.
    Falls back to full body text.
    """
    pricing_selectors = [
        "[class*='pricing']", "[id*='pricing']",
        "[class*='plan']", "[class*='price']",
        "main", "article",
    ]
    for selector in pricing_selectors:
        try:
            element = await page.query_selector(selector)
            if element:
                text = await element.inner_text()
                if len(text.strip()) > 100:
                    return _clean_text(text)
        except Exception:
            continue

    body_text = await page.inner_text("body")
    return _clean_text(body_text)


async def scrape_page_playwright(browser: Browser, url: str) -> str | None:
    """
    Navigate to *url* with a stealthy headless browser and return extracted text.
    Returns None on failure or when a bot-challenge is detected.
    """
    user_agent = random.choice(USER_AGENTS)
    context = await browser.new_context(
        user_agent=user_agent,
        viewport={"width": 1280, "height": 900},
        locale="en-US",
        timezone_id="Africa/Cairo",
    )
    page = await context.new_page()
    await _stealth.apply_stealth_async(page)

    try:
        logger.info("Playwright scraping %s", url)
        await page.goto(url, wait_until="networkidle", timeout=NAVIGATION_TIMEOUT_MS)

        title = await page.title()
        body_snippet = await page.inner_text("body")

        if _is_blocked(title, body_snippet):
            logger.warning("BLOCKED/CHALLENGE at %s (title: %s) — skipping", url, title)
            return None

        text = await _extract_pricing_text(page)
        if not text:
            logger.warning("No text extracted from %s", url)
            return None

        logger.info("Playwright: extracted %d chars from %s", len(text), url)
        return text

    except PWTimeout:
        logger.error("Timeout navigating to %s", url)
        return None
    except Exception as exc:
        logger.error("Error scraping %s: %s", url, exc)
        return None
    finally:
        await context.close()


# ---------------------------------------------------------------------------
# robots.txt helper
# ---------------------------------------------------------------------------

def is_allowed(url: str) -> bool:
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url(_base_url(url) + "/robots.txt")
    rp.read()
    return rp.can_fetch("*", url)


# ---------------------------------------------------------------------------
# Public orchestration
# ---------------------------------------------------------------------------

async def scrape_all(targets: list[dict]) -> dict[str, str | None]:
    """
    Scrape every target. Shopify stores are fetched via the JSON API;
    everything else uses Playwright. Returns url → content (or None).
    """
    results: dict[str, str | None] = {}

    # Separate targets so we only launch a browser when actually needed
    shopify_targets = []
    playwright_targets = []

    for target in targets:
        url = target["url"]
        base = _base_url(url)
        if _is_shopify(base):
            logger.info("[%s] Shopify store detected", target["name"])
            shopify_targets.append(target)
        else:
            logger.info("[%s] Non-Shopify — will use Playwright", target["name"])
            playwright_targets.append(target)

    # --- Shopify targets (synchronous requests, no browser needed) ---
    for target in shopify_targets:
        url = target["url"]
        results[url] = scrape_shopify(url)
        delay = random.uniform(1, 3)
        await asyncio.sleep(delay)

    # --- Playwright targets ---
    if playwright_targets:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            for target in playwright_targets:
                url = target["url"]
                results[url] = await scrape_page_playwright(browser, url)
                delay = random.uniform(2, 5)
                logger.debug("Waiting %.1fs before next request", delay)
                await asyncio.sleep(delay)
            await browser.close()

    return results
