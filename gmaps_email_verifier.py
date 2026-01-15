#!/usr/bin/env python3
"""
gmaps_email_verifier.py
Purpose.
Given one or more Google Maps business URLs, extract business website(s), crawl linked site(s) for email addresses,
verify those emails using syntax check, MX lookup, and SMTP RCPT check (no message sent), and write result to Excel.
Output Excel columns.
- Business Name
- Website URL
- Extracted Email
- Email Validity Status (Valid, Invalid, Risky)
- Verification Method Used (Syntax, MX, SMTP)
- Source URL (where the email was found)
Dependencies.
pip install requests beautifulsoup4 pandas openpyxl dnspython selenium urllib3
Notes.
- Selenium fallback requires a WebDriver, e.g., chromedriver. Put it in PATH or specify path in SELENIUM_DRIVER_PATH.
- SMTP verification attempts to open a connection to MX hosts. Some servers block verification or give inconclusive responses.
- Respect site robots.txt rules. The script checks robots for the domain before crawling.
- Use responsibly and ethically. Do not use against terms of service you do not control.
Author. Senior Python developer, web scraping and verification specialist.
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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib.robotparser as robotparser
# Optional Selenium fallback.
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    SELENIUM_AVAILABLE = True
except Exception:
    SELENIUM_AVAILABLE = False
# ========== Configuration ==========
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " \
             "(KHTML, like Gecko) Chrome/117.0 Safari/537.36"
HEADERS = {"User-Agent": USER_AGENT}
REQUEST_TIMEOUT = 15  # seconds
RATE_LIMIT_SECONDS = 1.2  # delay between HTTP requests
CRAWL_PAGE_LIMIT = 40  # max pages to crawl per site
CRAWL_DEPTH_LIMIT = 2  # how many link levels to traverse
SELENIUM_DRIVER_PATH = None  # if None, rely on PATH for chromedriver
SMTP_TIMEOUT = 10  # seconds for smtp operations
SMTP_PORT = 25  # standard SMTP port. Some MXs accept on 25 only.
# Email regex for extraction and basic syntax validation. Careful, strict enough for practical use.
EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+\-']+@[a-zA-Z0-9.\-]+\.[A-Za-z]{2,}", re.I)
# Configure logging.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("gmaps_email_verifier")
# Session with retries.
session = requests.Session()
retries = Retry(total=3, backoff_factor=0.6, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))
session.mount("http://", HTTPAdapter(max_retries=retries))
# ========== Utilities ==========
def safe_get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT):
    """HTTP GET with session, headers, timeout, rate limiting. Returns response.text or None."""
    logger.debug("GET %s", url)
    try:
        time.sleep(RATE_LIMIT_SECONDS)
        resp = session.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        logger.debug("GET failed for %s. %s", url, e)
        return None
def is_allowed_by_robots(url, user_agent=USER_AGENT):
    """Check robots.txt for domain and path. Returns True if allowed or robots not available."""
    try:
        p = urlparse(url)
        base = f"{p.scheme}://{p.netloc}"
        robots_url = urljoin(base, "/robots.txt")
        rp = robotparser.RobotFileParser()
        rp.set_url(robots_url)
        rp.read()
        allowed = rp.can_fetch(user_agent, url)
        logger.debug("Robots check for %s returned %s", url, allowed)
        return allowed
    except Exception as e:
        logger.debug("Robots check failed for %s. Assuming allowed. Error: %s", url, e)
        return True
# ========== Google Maps extraction ==========
def extract_website_from_gmaps_html(html):
    """
    Attempt to find the business website link within Google Maps HTML.
    Google Maps markup is dynamic, but sometimes the server-side HTML includes a link such as:
    - <a href="https://example.com" ...>Website</a>
    - JSON-LD or structured data may be present.
    Returns (business_name, website_url) or (None, None).
    """
    if not html:
        return None, None
    soup = BeautifulSoup(html, "html.parser")
    # Attempt 1: find anchor with text 'Website' or 'website' or 'Visit website'
    a_tags = soup.find_all("a")
    for a in a_tags:
        text = (a.get_text() or "").strip().lower()
        href = a.get("href")
        if not href:
            continue
        if "website" in text or text in {"visit website", "visit site"}:
            # Some links may be proxied via /url?q=...
            if href.startswith("/url?q="):
                m = re.search(r"/url\?q=(https?://[^&]+)", href)
                if m:
                    return None, m.group(1)
            if href.startswith("http"):
                return None, href
    # Attempt 2: search for JSON-LD structured data with "url"
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            import json
            data = json.loads(script.string or "{}")
            if isinstance(data, dict):
                url = data.get("url")
                name = data.get("name")
                if url:
                    return name, url
            elif isinstance(data, list):
                for entry in data:
                    if isinstance(entry, dict) and entry.get("url"):
                        return entry.get("name"), entry.get("url")
        except Exception:
            continue
    # Attempt 3: search for attributes or meta tags
    metas = soup.find_all("meta")
    for m in metas:
        if m.get("property") in ("og:url", "og:site_name") or m.get("name") in ("og:url", "og:site_name"):
            # not always helpful for website link, skip
            pass
    return None, None
def get_website_from_google_maps(url):
    """
    Try to extract the business website from a Google Maps URL.
    First try plain requests. If unsuccessful and Selenium is available, use Selenium to fully render the page.
    Returns (business_name, website_url) tuple.
    """
    html = safe_get(url)
    name, site = extract_website_from_gmaps_html(html)
    if site:
        logger.info("Found site via static parse: %s", site)
        return name, site
    if SELENIUM_AVAILABLE:
        logger.info("Falling back to Selenium to fetch dynamic Google Maps content.")
        try:
            options = Options()
            options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-gpu")
            options.add_argument(f"user-agent={USER_AGENT}")
            driver = webdriver.Chrome(executable_path=SELENIUM_DRIVER_PATH) if SELENIUM_DRIVER_PATH else webdriver.Chrome(options=options)
            driver.set_page_load_timeout(30)
            driver.get(url)
            time.sleep(3)  # allow JS to render
            page_html = driver.page_source
            driver.quit()
            name, site = extract_website_from_gmaps_html(page_html)
            if site:
                logger.info("Found site via Selenium: %s", site)
                return name, site
        except Exception as e:
            logger.warning("Selenium fetch failed. %s", e)
    logger.info("Could not find website URL on Google Maps page.")
    return None, None
# ========== Crawling & Email extraction ==========
def extract_emails_from_html(html):
    """Return a set of email addresses found in the HTML via mailto and regex."""
    found = set()
    if not html:
        return found
    soup = BeautifulSoup(html, "html.parser")
    # mailto
    for a in soup.select('a[href^="mailto:"]'):
        href = a.get("href")
        if not href:
            continue
        mail = href.split("mailto:")[1].split("?")[0]
        if mail:
            found.add(mail.strip())
    # regex
    for m in EMAIL_REGEX.findall(html):
        found.add(m.strip())
    return found
def same_domain(url1, url2):
    """Return True if both URLs share the same domain (netloc)."""
    try:
        p1 = urlparse(url1)
        p2 = urlparse(url2)
        return p1.netloc.lower() == p2.netloc.lower()
    except Exception:
        return False
def crawl_site_for_emails(start_url, max_pages=CRAWL_PAGE_LIMIT, depth_limit=CRAWL_DEPTH_LIMIT):
    """
    Breadth-first crawl starting from start_url, limited to same domain, extracting emails.
    Respects robots.txt and crawl limits. Returns set of found emails and map of email->source_url.
    """
    if not is_allowed_by_robots(start_url):
        logger.warning("Crawling disallowed by robots.txt for %s. Skipping crawl.", start_url)
        return set(), {}
    found_emails = set()
    email_sources = {}
    visited = set()
    queue = deque()
    queue.append((start_url, 0))
    visited.add(start_url)
    while queue and len(visited) <= max_pages:
        current_url, depth = queue.popleft()
        try:
            html = safe_get(current_url)
            if not html:
                continue
            emails = extract_emails_from_html(html)
            for em in emails:
                found_emails.add(em)
                if em not in email_sources:
                    email_sources[em] = current_url
            if depth < depth_limit:
                soup = BeautifulSoup(html, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = a.get("href")
                    if href.startswith("mailto:"):
                        continue
                    # normalize
                    next_url = urljoin(current_url, href)
                    parsed = urlparse(next_url)
                    if parsed.scheme not in ("http", "https"):
                        continue
                    # only same domain
                    if same_domain(start_url, next_url) and next_url not in visited:
                        if is_allowed_by_robots(next_url):
                            visited.add(next_url)
                            queue.append((next_url, depth + 1))
        except Exception as e:
            logger.debug("Error crawling %s. %s", current_url, e)
            continue
    return found_emails, email_sources
# ========== Verification ==========
def validate_syntax(email):
    """Simple RFC-lite syntax check. Returns True/False."""
    if not email:
        return False
    return bool(EMAIL_REGEX.fullmatch(email))
def check_mx_records(domain):
    """Return list of MX hostnames for domain, empty list if none or error."""
    try:
        answers = dns.resolver.resolve(domain, 'MX', lifetime=10.0)
        mx_hosts = [str(r.exchange).rstrip('.') for r in answers]
        # sort by preference if available using tuple (preference, host)
        return mx_hosts
    except Exception as e:
        logger.debug("MX lookup failed for %s. %s", domain, e)
        return []
def smtp_verify(email, from_address="validator@example.com"):
    """
    Perform SMTP RCPT check against MX hosts. Returns tuple (status, smtp_message).
    status in {"valid", "invalid", "risk"}.
    Note. Many mail servers will not allow RCPT verification or will accept every address. Treat ambiguous responses as 'risk'.
    """
    domain = email.split("@", 1)[1]
    mx_hosts = check_mx_records(domain)
    if not mx_hosts:
        return "invalid", "No MX records found"
    last_exception = None
    for mx in mx_hosts:
        try:
            logger.debug("Connecting to MX %s for email %s", mx, email)
            # Resolve MX host to IP, then try SMTP
            # socket.gethostbyname(mx)  # not necessary, smtplib handles it
            smtp = smtplib.SMTP(timeout=SMTP_TIMEOUT)
            smtp.connect(mx, SMTP_PORT)
            smtp.helo(socket.gethostname())
            smtp.mail(from_address)
            code, message = smtp.rcpt(email)
            smtp.quit()
            code = int(code)
            msg_text = message.decode() if isinstance(message, bytes) else str(message)
            logger.debug("MX %s responded %s %s", mx, code, msg_text)
            if 200 <= code < 300:
                return "valid", f"SMTP {code} {msg_text}"
            if 400 <= code < 500:
                # greylisting or temp failure. Consider risky.
                return "risk", f"SMTP {code} {msg_text}"
            # 500-range means rejected.
            if 500 <= code < 600:
                # rejected on this MX. Try next.
                last_exception = f"SMTP {code} {msg_text}"
                continue
        except smtplib.SMTPServerDisconnected as e:
            last_exception = f"SMTP disconnect {e}"
            continue
        except (socket.timeout, socket.error, Exception) as e:
            last_exception = str(e)
            logger.debug("SMTP attempt to %s failed. %s", mx, e)
            continue
    # If we reached here, either all MX rejects or had errors.
    if last_exception:
        return "invalid", last_exception
    return "risk", "Unknown SMTP response"
def verify_email(email):
    """
    Full layered verification.
    Returns dict with keys:
    - status: 'Valid' / 'Invalid' / 'Risky'
    - method: 'Syntax' / 'MX' / 'SMTP' / combination
    - details: textual detail
    """
    res = {"status": "Invalid", "method": "None", "details": ""}
    if not validate_syntax(email):
        res.update({"status": "Invalid", "method": "Syntax", "details": "Syntax check failed"})
        return res
    # Syntax passed
    res["method"] = "Syntax"
    domain = email.split("@", 1)[1]
    mx_hosts = check_mx_records(domain)
    if not mx_hosts:
        res.update({"status": "Invalid", "method": "MX", "details": "No MX records"})
        return res
    res["method"] = "MX"
    # Attempt SMTP verification
    smtp_status, detail = smtp_verify(email)
    if smtp_status == "valid":
        res.update({"status": "Valid", "method": "SMTP", "details": detail})
    elif smtp_status == "risk":
        res.update({"status": "Risky", "method": "SMTP", "details": detail})
    else:
        res.update({"status": "Invalid", "method": "SMTP", "details": detail})
    return res
# ========== Orchestration ==========
def process_gmaps_url(gmaps_url):
    """
    For a single Google Maps business URL, try to find website, crawl it, extract emails, verify them,
    and return list of rows for output.
    Each row is a dict matching output columns.
    """
    rows = []
    logger.info("Processing Google Maps URL: %s", gmaps_url)
    business_name, website = get_website_from_google_maps(gmaps_url)
    if not website:
        # If Google Maps doesn't have website, try to parse business name from URL and attempt a search URL fallback is out of scope.
        business_name = business_name or ""
        logger.warning("No website found for %s. Returning empty result.", gmaps_url)
        # Return a row with no email found.
        rows.append({
            "Business Name": business_name or "Unknown",
            "Website URL": website or "",
            "Extracted Email": "",
            "Email Validity Status": "Invalid",
            "Verification Method Used": "None",
            "Source URL": gmaps_url
        })
        return rows
    # Crawl website for emails.
    emails, sources = crawl_site_for_emails(website, max_pages=CRAWL_PAGE_LIMIT, depth_limit=CRAWL_DEPTH_LIMIT)
    if not emails:
        logger.info("No emails found on %s", website)
        rows.append({
            "Business Name": business_name or "",
            "Website URL": website,
            "Extracted Email": "",
            "Email Validity Status": "Invalid",
            "Verification Method Used": "None",
            "Source URL": gmaps_url
        })
        return rows
    # Verify each email and prepare rows.
    for em in sorted(emails):
        verification = verify_email(em)
        rows.append({
            "Business Name": business_name or "",
            "Website URL": website,
            "Extracted Email": em,
            "Email Validity Status": verification["status"].capitalize(),
            "Verification Method Used": verification["method"],
            "Source URL": sources.get(em, gmaps_url)
        })
    return rows
def save_results_to_excel(rows, output_file="gmaps_emails_verified.xlsx"):
    """Save list of dict rows to Excel using pandas."""
    if not rows:
        logger.warning("No rows to save.")
        return None
    df = pd.DataFrame(rows, columns=[
        "Business Name", "Website URL", "Extracted Email",
        "Email Validity Status", "Verification Method Used", "Source URL"
    ])
    df.to_excel(output_file, index=False)
    logger.info("Saved %d rows to %s", len(df), output_file)
    return output_file
# ========== CLI ==========
def parse_args():
    parser = argparse.ArgumentParser(description="Extract and verify emails from Google Maps business URLs.")
    parser.add_argument("--input", "-i", nargs="+", required=True,
                        help="One or more Google Maps business URLs, or path(s) to text files with URLs (one per line).")
    parser.add_argument("--output", "-o", default="gmaps_emails_verified.xlsx",
                        help="Output Excel filename.")
    parser.add_argument("--pages", type=int, default=CRAWL_PAGE_LIMIT, help="Max pages to crawl per site.")
    parser.add_argument("--depth", type=int, default=CRAWL_DEPTH_LIMIT, help="Crawl depth per site.")
    parser.add_argument("--verbose", action="store_true", help="Show debug logs.")
    return parser.parse_args()
def load_urls_from_args(inputs):
    """Inputs may be direct URLs or text files containing URLs."""
    urls = []
    for item in inputs:
        if item.lower().startswith("http"):
            urls.append(item.strip())
        else:
            # treat as file
            try:
                with open(item, "r", encoding="utf-8") as fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue
                        if line.startswith("http"):
                            urls.append(line)
            except Exception as e:
                logger.warning("Could not read input file %s. %s", item, e)
    return urls
def main():
    args = parse_args()
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    global CRAWL_PAGE_LIMIT, CRAWL_DEPTH_LIMIT
    CRAWL_PAGE_LIMIT = args.pages
    CRAWL_DEPTH_LIMIT = args.depth
    urls = load_urls_from_args(args.input)
    if not urls:
        logger.error("No valid URLs provided.")
        sys.exit(1)
    all_rows = []
    for url in urls:
        try:
            rows = process_gmaps_url(url)
            all_rows.extend(rows)
        except Exception as e:
            logger.exception("Failed processing %s. %s", url, e)
            all_rows.append({
                "Business Name": "",
                "Website URL": "",
                "Extracted Email": "",
                "Email Validity Status": "Invalid",
                "Verification Method Used": "None",
                "Source URL": url
            })
    save_results_to_excel(all_rows, args.output)
if __name__ == "__main__":
    main()