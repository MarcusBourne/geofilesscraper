#pip install boto3
#pip install urllib3
#pip install requests
#pip install BeautifulSoup

#dropped storage on local disk
#scraper now automatically uploads downloaded documents onto aws s3 bucket
#looking into using Playwright and not POST/GET to speed things up


import os
import re
import time
import requests
import boto3
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
from urllib.parse import urljoin


BUCKET_NAME = "cna-webfiles"
S3_PREFIX   = "scrape_test"

creds_file = os.path.join(os.path.dirname(__file__), "creds.txt")
with open(creds_file) as f:
    ACCESS_KEY, SECRET_KEY = [v.strip() for v in f.readline().split(",")]
s3 = boto3.client("s3",aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY)


BASE_URL        = "https://gis.gov.nl.ca/minesen/geofiles/"
DEFAULT_URL      = urljoin(BASE_URL, "default.asp")
DISPLAY_URL     = urljoin(BASE_URL, "display.asp")
EXTERNAL_PREFIX = "https://www.gov.nl.ca/iet/mines-geoscience-reports-maps-docs"

MAX_RETRIES     = 3
RETRY_DELAY     = 2


def request_with_retry(session, method, url, timeout=None, **kwargs):
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.request(method, url, timeout=timeout, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.RequestException as err:
            if attempt < MAX_RETRIES - 1:
                print(f"Warning: {method.upper()} {url} failed ({err}), Retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"Error: {method.upper()} {url} failed after {MAX_RETRIES} attempts: {err}. Skipping.")
    return None


def upload_stream(stream, key):
    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=key)
        print(f"Skip: {key} already exists")
        return
    except ClientError as e:
        if e.response['Error']['Code'] != '404':
            print(f"S3 head_object error for {key}: {e}")
            return
    print(f"Uploading: {key}")
    s3.upload_fileobj(stream, BUCKET_NAME, key)


def download_file(session, url, prefix=S3_PREFIX):
    fname = os.path.basename(url)
    lname = fname.lower()
    if 'map' in lname or 'research' in lname:
        print(f"Skipping download of '{fname}' because filename contains filtered word.")
        return
    resp = request_with_retry(session, 'get', url, timeout=None, stream=True)
    if not resp:
        return
    resp.raw.decode_content = True
    key = f"{prefix}/{fname}"
    upload_stream(resp.raw, key)


def scrape_external(session, page_url, prefix=S3_PREFIX):
    print(f"Scraping external page: {page_url}")
    resp = request_with_retry(session, 'get', page_url, timeout=None)
    if not resp:
        return
    soup = BeautifulSoup(resp.text, 'lxml')
    for a in soup.find_all('a', href=True):
        link = a['href']
        full = link if link.startswith('http') else urljoin(page_url, link)
        if full.lower().endswith(('.pdf', '.zip')):
            download_file(session, full, prefix)


def scrape_to_s3(prefix=S3_PREFIX):
    session = requests.Session()
    session.headers['User-Agent'] = 'PDF-Scraper/1.0'
    print("Starting Web Scraper... Please wait.")


    # Load search form
    resp = request_with_retry(session, 'get', DEFAULT_URL, timeout=None)
    if not resp:
        print("Failed to load default.asp; aborting.")
        return
    
    soup = BeautifulSoup(resp.text, 'lxml')
    search_link = soup.find('a', class_='lg_link_blk', href=re.compile(r'javascript:onClick=submitForm', re.I))
    
    if not search_link:
        print("Search link not found on default.asp")
        return
    form = search_link.find_parent('form')
    if not form:
        print("Search form not found on default.asp")
        return


    # Build form payload
    payload = {i["name"]: i.get("value","")
               for i in form.find_all("input", type=["hidden","text"]) if i.get("name")}
    
    for sel in form.find_all("select"):
        name = sel.get("name")
        if not name:
            continue
        opt = sel.find("option", selected=True) or sel.find("option")
        payload[name] = opt.get("value","") if opt else ""
        payload[f"{name}_txt"] = opt.get_text(strip=True) if opt else ""


    # Submit search
    resp = request_with_retry(session, "post", DISPLAY_URL, timeout=None, data=payload, headers={"Referer": DEFAULT_URL})
    if not resp:
        print("Failed to load display.asp; aborting.")
        return
    soup = BeautifulSoup(resp.text, "lxml")


    # Pagination
    pager = soup.find("form", attrs={"name":"goSearch"})
    if not pager:
        print("Pagination form not found.")
        return
    
    pager_payload = {i["name"]: i.get("value","") 
           for i in pager.find_all("input", type="hidden") if i.get("name")}
    
    
    # Total Pages
    last = soup.find('img', src=re.compile(r'last\.gif'))
    pages = int(re.search(r'goPage\(\s*(\d+)', last.parent['href']).group(1)) if last else 1


    # Page loopings
    for page in range(1, pages+1):
        print(f"Page {page}/{pages}")
        if page > 1:
            params = pager_payload.copy()
            params.update({"pageCt": str(page), "PK": "0"})
            resp = request_with_retry(session, "post", DISPLAY_URL, timeout=None, data=params, headers={"Referer": DISPLAY_URL})
            if not resp:
                print(f"Failed to load page {page}.")
                continue
            soup = BeautifulSoup(resp.text, "lxml")


        # Download PDFs and Follow External Links
        for a in soup.find_all("a", href=True):
            url = a["href"]
            full = url if url.startswith("http") else urljoin(BASE_URL, url)
            
            if full.lower().endswith('.pdf'):
                download_file(session, full, prefix)
                
            elif full.startswith(EXTERNAL_PREFIX):
                scrape_external(session, full, prefix)


        # Digital Data ZIPs
        for a in soup.find_all("a", string=lambda t: t and "Digital Data" in t):
            url = a["href"]
            full = url if url.startswith("http") else urljoin(BASE_URL, url)
            download_file(session, full, prefix)

    print('Finished Downloading. Files saved to', BUCKET_NAME, "directory.")

if __name__ == '__main__':
    scrape_to_s3()