#!/usr/bin/env python3
"""
email_extractor_verifier.py

Extract emails from website URLs and verify them using:
- Syntax check
- MX record check
- Optional SMTP ping (no email sent)

Output: Excel (.xlsx)

100% open-source, no Selenium, no Google Maps scraping.
"""

import re
import time
import socket
import requests
import pandas as pd
import dns.resolver
import smtplib
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import deque

# ================= CONFIG =================

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; EmailExtractor/1.0)"
}

TIMEOUT = 15
RATE_LIMIT = 1
MAX_PAGES = 30
CRAWL_DEPTH = 2
SMTP_TIMEOUT = 8

EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[A-Za-z]{2,}"
)

# ================= HELPERS =================

def fetch(url):
    try:
        time.sleep(RATE_LIMIT)
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return r.text
    except Exception:
        return None

def extract_emails(html):
    return set(EMAIL_REGEX.findall(html or ""))

def same_domain(a, b):
    return urlparse(a).netloc == urlparse(b).netloc

# ================= CRAWLER =================
def is_skippable_html(html):
    if not html:
        return True
    # Extremely short or binary-like responses
    if len(html) < 200:
        return True
    return False

def crawl_site(start_url):
    visited = set()
    queue = deque([(start_url, 0)])
    results = []

    while queue:
        url, depth = queue.popleft()
        if url in visited or depth > CRAWL_DEPTH or len(visited) > MAX_PAGES:
            continue

        visited.add(url)
        html = fetch(url)

        # Skip broken / invalid pages
        if is_skippable_html(html):
            print(f"[!] Skipping bad page: {url}")
            continue

        # Extract emails safely
        try:
            emails = extract_emails(html)
            for email in emails:
                results.append((email, url))
        except Exception:
            print(f"[!] Email extraction failed: {url}")
            continue

        # Safe parsing with fallback
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception:
            try:
                soup = BeautifulSoup(html, "html.parser")
            except Exception:
                print(f"[!] Skipping unparsable HTML: {url}")
                continue

        for a in soup.find_all("a", href=True):
            try:
                link = urljoin(url, a["href"])
                if link.startswith("http") and same_domain(start_url, link):
                    queue.append((link, depth + 1))
            except Exception:
                continue

    return results


# ================= VERIFICATION =================

def syntax_valid(email):
    return bool(EMAIL_REGEX.fullmatch(email))

def mx_valid(domain):
    try:
        dns.resolver.resolve(domain, "MX", lifetime=5)
        return True
    except Exception:
        return False

def smtp_check(email):
    domain = email.split("@")[1]
    try:
        records = dns.resolver.resolve(domain, "MX")
        mx = str(records[0].exchange).rstrip(".")
        server = smtplib.SMTP(mx, 25, timeout=SMTP_TIMEOUT)
        server.helo()
        server.mail("test@example.com")
        code, _ = server.rcpt(email)
        server.quit()

        if 200 <= code < 300:
            return "Valid"
        elif 400 <= code < 500:
            return "Risky"
        else:
            return "Invalid"
    except Exception:
        return "Risky"

# ================= MAIN =================

def process_urls(urls):
    rows = []

    for site in urls:
        print(f"[+] Crawling {site}")
        found = crawl_site(site)

        if not found:
            rows.append({
                "Website URL": site,
                "Page URL": "",
                "Extracted Email": "",
                "Syntax Valid": "No",
                "MX Valid": "No",
                "SMTP Check": "Skipped",
                "Final Status": "Invalid"
            })
            continue

        for email, page in found:
            syntax = syntax_valid(email)
            mx = mx_valid(email.split("@")[1]) if syntax else False
            smtp = smtp_check(email) if mx else "Skipped"

            if syntax and mx and smtp == "Valid":
                status = "Valid"
            elif syntax and mx:
                status = "Risky"
            else:
                status = "Invalid"

            rows.append({
                "Website URL": site,
                "Page URL": page,
                "Extracted Email": email,
                "Syntax Valid": "Yes" if syntax else "No",
                "MX Valid": "Yes" if mx else "No",
                "SMTP Check": smtp,
                "Final Status": status
            })

    return rows

def load_urls(file):
    with open(file, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def main():
    urls = load_urls("urls.txt")
    data = process_urls(urls)
    df = pd.DataFrame(data)
    df.to_excel("emails_verified.xlsx", index=False)
    print("\n✅ Saved: emails_verified.xlsx")

if __name__ == "__main__":
    main()
