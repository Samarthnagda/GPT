#!/usr/bin/env python3
"""
gmaps_email_verifier.py
Google Maps → Website → Email extraction → Verification
Selenium fixed using webdriver-manager (ChromeDriver auto sync)
"""

import argparse
import re
import time
import logging
import socket
import sys
from urllib.parse import urlparse, urljoin
from collections import deque

import requests
from bs4 import BeautifulSoup
import pandas as pd
import dns.resolver
import smtplib
import urllib.robotparser as robotparser

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Selenium (FIXED)
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager


# ================= CONFIG =================
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
HEADERS = {"User-Agent": USER_AGENT}
REQUEST_TIMEOUT = 15
RATE_LIMIT_SECONDS = 2
CRAWL_PAGE_LIMIT = 40
CRAWL_DEPTH_LIMIT = 2
SMTP_TIMEOUT = 10
SMTP_PORT = 25

EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+\-']+@[a-zA-Z0-9.\-]+\.[A-Za-z]{2,}",
    re.I
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("gmaps_email_verifier")


# ================= REQUEST SESSION =================
session = requests.Session()
retries = Retry(total=3, backoff_factor=0.6, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))
session.mount("http://", HTTPAdapter(max_retries=retries))


def safe_get(url):
    try:
        time.sleep(RATE_LIMIT_SECONDS)
        r = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.text
    except Exception:
        return None


# ================= ROBOTS =================
def is_allowed_by_robots(url):
    try:
        p = urlparse(url)
        rp = robotparser.RobotFileParser()
        rp.set_url(f"{p.scheme}://{p.netloc}/robots.txt")
        rp.read()
        return rp.can_fetch(USER_AGENT, url)
    except Exception:
        return True


# ================= GOOGLE MAPS (SELENIUM FIXED) =================
def get_website_from_google_maps(url):
    logger.info("Using Selenium for Google Maps extraction")

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument(f"user-agent={USER_AGENT}")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    business_name = None
    website = None

    try:
        driver.get(url)
        time.sleep(5)

        try:
            business_name = driver.find_element(By.CSS_SELECTOR, "h1").text
        except Exception:
            pass

        try:
            website = driver.find_element(
                By.CSS_SELECTOR,
                'a[data-item-id="authority"]'
            ).get_attribute("href")
        except Exception:
            pass

    finally:
        driver.quit()

    if website:
        logger.info("Found website: %s", website)
    else:
        logger.warning("Website not found on Maps page")

    return business_name, website


# ================= EMAIL EXTRACTION =================
def extract_emails_from_html(html):
    found = set()
    if not html:
        return found

    soup = BeautifulSoup(html, "html.parser")

    for a in soup.select('a[href^="mailto:"]'):
        mail = a.get("href").split("mailto:")[1].split("?")[0]
        found.add(mail)

    for m in EMAIL_REGEX.findall(html):
        found.add(m)

    return found


def same_domain(u1, u2):
    return urlparse(u1).netloc == urlparse(u2).netloc


def crawl_site_for_emails(start_url):
    if not is_allowed_by_robots(start_url):
        return set(), {}

    visited = set([start_url])
    queue = deque([(start_url, 0)])
    found = set()
    sources = {}

    while queue and len(visited) <= CRAWL_PAGE_LIMIT:
        url, depth = queue.popleft()
        html = safe_get(url)
        if not html:
            continue

        emails = extract_emails_from_html(html)
        for e in emails:
            found.add(e)
            sources.setdefault(e, url)

        if depth >= CRAWL_DEPTH_LIMIT:
            continue

        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            nxt = urljoin(url, a["href"])
            if nxt.startswith("http") and same_domain(start_url, nxt):
                if nxt not in visited:
                    visited.add(nxt)
                    queue.append((nxt, depth + 1))

    return found, sources


# ================= EMAIL VERIFICATION =================
def validate_syntax(email):
    return bool(EMAIL_REGEX.fullmatch(email))


def check_mx(domain):
    try:
        answers = dns.resolver.resolve(domain, "MX")
        return [str(r.exchange).rstrip(".") for r in answers]
    except Exception:
        return []


def smtp_verify(email):
    domain = email.split("@")[1]
    mxs = check_mx(domain)
    if not mxs:
        return "Invalid", "No MX"

    for mx in mxs:
        try:
            smtp = smtplib.SMTP(mx, SMTP_PORT, timeout=SMTP_TIMEOUT)
            smtp.helo()
            smtp.mail("verify@example.com")
            code, _ = smtp.rcpt(email)
            smtp.quit()

            if 200 <= code < 300:
                return "Valid", "SMTP OK"
            if 400 <= code < 500:
                return "Risky", "Temp failure"
        except Exception:
            continue

    return "Invalid", "Rejected"


# ================= MAIN PROCESS =================
def process_gmaps_url(url):
    rows = []
    name, site = get_website_from_google_maps(url)

    if not site:
        rows.append({
            "Business Name": name or "Unknown",
            "Website URL": "",
            "Extracted Email": "",
            "Email Validity Status": "Invalid",
            "Verification Method Used": "None",
            "Source URL": url
        })
        return rows

    emails, sources = crawl_site_for_emails(site)

    if not emails:
        rows.append({
            "Business Name": name or "",
            "Website URL": site,
            "Extracted Email": "",
            "Email Validity Status": "Invalid",
            "Verification Method Used": "None",
            "Source URL": site
        })
        return rows

    for e in emails:
        if not validate_syntax(e):
            status, method = "Invalid", "Syntax"
        else:
            status, method = smtp_verify(e)

        rows.append({
            "Business Name": name or "",
            "Website URL": site,
            "Extracted Email": e,
            "Email Validity Status": status,
            "Verification Method Used": method,
            "Source URL": sources.get(e, site)
        })

    return rows


# ================= CLI =================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", required=True)
    parser.add_argument("-o", "--output", default="gmaps_emails_verified.xlsx")
    args = parser.parse_args()

    with open(args.input) as f:
        urls = [l.strip() for l in f if l.strip()]

    all_rows = []
    for u in urls:
        all_rows.extend(process_gmaps_url(u))

    df = pd.DataFrame(all_rows)
    df.to_excel(args.output, index=False)
    logger.info("Saved output to %s", args.output)


if __name__ == "__main__":
    main()
