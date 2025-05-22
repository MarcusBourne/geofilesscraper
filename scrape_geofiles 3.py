#pip install urllib3
#pip install requests
#pip install BeautifulSoup

#scraper now finds external links and downloads the contents of that page.
#does not download anything with "map or research" in the file name
#attempts at speeding up the scraper include, asyncio, httpx, multiprocessing all still slow


import os
import re
import time
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup


BASE_URL        = "https://gis.gov.nl.ca/minesen/geofiles/"
DEFAULT_URL     = urljoin(BASE_URL, "default.asp")
DISPLAY_URL     = urljoin(BASE_URL, "display.asp")
EXTERNAL_PREFIX = "https://www.gov.nl.ca/iet/mines-geoscience-reports-maps-docs"

MAX_RETRIES = 3
RETRY_DELAY = 2
TIMEOUT     = 15


def request_with_retry(session, method, url, **kwargs):
    timeout = None if url in (DEFAULT_URL, DISPLAY_URL) else TIMEOUT
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.request(method, url, timeout=timeout, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as err:
            if attempt < MAX_RETRIES:
                print(f"Warning: {method.upper()} {url} attempt {attempt}/{MAX_RETRIES} failed: {err}. Retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"Error: {method.upper()} {url} failed after {MAX_RETRIES} attempts: {err}. Skipping.")
                return None


def download_file(session, url, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    fname = os.path.basename(url)
    path = os.path.join(out_dir, fname)
    if os.path.exists(path):
        return

    lower = fname.lower()
    if 'map' in lower or 'research' in lower:
        print(f"Skipping download of '{fname}' because filename contains 'map' or 'research'.")
        return

    print(f"Downloading: {fname}")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with session.get(url, stream=True, timeout=None) as resp:
                resp.raise_for_status()
                with open(path, "wb") as f:
                    for chunk in resp.iter_content(8192):
                        if chunk:
                            f.write(chunk)
            break
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES:
                print(f"Warning: download {url} attempt {attempt} failed: {e}. Retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"Error: download {url} failed after {MAX_RETRIES} attempts: {e}")


def scrape_external(session, external_url, out_dir):
    print(f"Scraping external page: {external_url}")
    resp = request_with_retry(session, 'get', external_url)
    if not resp:
        return
    soup = BeautifulSoup(resp.text, 'lxml')
    for a in soup.find_all('a', href=True):
        href = a['href']
        full = href if href.lower().startswith('http') else urljoin(external_url, href)
        if full.lower().endswith(('.pdf', '.zip')):
            download_file(session, full, out_dir)


def scrape_geofiles(title=None, out_dir="pdfs"):
    print("Starting Web Scraper... Please wait.")
    os.makedirs(out_dir, exist_ok=True)
    session = requests.Session()
    session.headers["User-Agent"] = "PDF-Scraper/1.0"


    # Load search form
    resp = request_with_retry(session, 'get', DEFAULT_URL)
    if not resp:
        print("Failed to load default.asp; aborting.")
        return
    
    soup = BeautifulSoup(resp.text, 'lxml')
    search_link = soup.find('a', class_='lg_link_blk', href=re.compile(r'submitForm', re.I))
    
    if not search_link:
        raise RuntimeError("Search link not found on default.asp")
    form = search_link.find_parent('form')
    if not form:
        raise RuntimeError("Search form not found on default.asp")


    # Build form payload
    payload = {inp["name"]: inp.get("value", "")
               for inp in form.find_all("input", type=["hidden", "text"]) if inp.get("name")}
    
    for sel in form.find_all("select"):
        name = sel.get("name")
        if not name:
            continue
        opt = sel.find("option", selected=True) or sel.find("option")
        payload[name] = opt.get("value", "") if opt else ""
        payload[f"{name}_txt"] = opt.get_text(strip=True) if opt else ""
    if title:
        payload["title"] = title


    # Submit search
    resp = request_with_retry(session, "post", DISPLAY_URL, data=payload, headers={"Referer": DEFAULT_URL})
    if resp is None:
        print("Failed to load display.asp; aborting.")
        return
    soup = BeautifulSoup(resp.text, "lxml")


    # Pagination
    pager = soup.find("form", {"name": "goSearch"})
    if not pager:
        raise RuntimeError("Paging form not found")
    
    pager_payload = {inp["name"]: inp.get("value", "")
                     for inp in pager.find_all("input", {"type": "hidden"}) if inp.get("name")}
    
    
    # Total pages
    last = soup.find('img', src=re.compile(r'last\.gif'))
    total = int(re.search(r'goPage\(\s*(\d+)', last.parent['href']).group(1)) if last else 1


    # Page loopings
    for page in range(1, total + 1):
        print(f"Page {page}/{total}")
        if page > 1:
            pp = pager_payload.copy()
            pp["pageCt"] = str(page)
            pp["PK"] = '0'
            resp = request_with_retry(session, "post", DISPLAY_URL, data=pp, headers={"Referer": DISPLAY_URL})
            if resp is None:
                print(f"Failed to load page {page}; skipping.")
                continue
            soup = BeautifulSoup(resp.text, "lxml")


        # Download PDFs and Follow External Links
        for a in soup.find_all("a", href=True):
            href = a["href"]
            full = href if href.lower().startswith("http") else urljoin(BASE_URL, href)
            
            if full.lower().endswith(".pdf"):
                download_file(session, full, out_dir)
                
            elif full.startswith(EXTERNAL_PREFIX):
                scrape_external(session, full, out_dir)


        # Download 'Digital Data' ZIPs
        for a in soup.find_all("a", string=lambda t: t and "Digital Data" in t):
            href = a["href"]
            full = href if href.lower().startswith("http") else urljoin(BASE_URL, href)
            download_file(session, full, out_dir)

    print('Finished Downloading. Files saved to', out_dir, "directory.")

if __name__ == '__main__':
    scrape_geofiles()
