#pip install urllib3
#pip install requests
#pip install BeautifulSoup

#got rid of FullText
#swapped to lxml from html.parser to try and speed things up... still slow
#pagination added
#Need to work on getting to ex... https://www.gov.nl.ca/iet/mines-geoscience-reports-maps-docs-open-file-nfld-3468/ page for remainding file downloads.


import os
import re
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup


BASE_URL    = "https://gis.gov.nl.ca/minesen/geofiles/"
DEFAULT_URL = urljoin(BASE_URL, "default.asp")
DISPLAY_URL = urljoin(BASE_URL, "display.asp")


def download_file(session, url, out_dir):
    fname = os.path.basename(url)
    path = os.path.join(out_dir, fname)
    if os.path.exists(path):
        return
    print(f"Downloading: {fname}")
    r = session.get(url)
    r.raise_for_status()
    with open(path, "wb") as f:
        f.write(r.content)


def scrape_geofiles(title=None, out_dir="pdfs"):
    print("Starting Web Scraper....Please Wait.")
    os.makedirs(out_dir, exist_ok=True)
    session = requests.Session()
    session.headers["User-Agent"] = "PDF-Scraper/1.0"


    # Load search form
    r = session.get(DEFAULT_URL); 
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    form = soup.find("form", {"name":"searchForm","action":"display.asp"})
    if not form:
        raise RuntimeError("Search form not found on default.asp")


    # Build payload
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
    r = session.post(
        DISPLAY_URL, 
        data=payload, 
        headers={"Referer": DEFAULT_URL}
    )
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")


    # Pagination
    pager = soup.find("form", {"name":"goSearch"})
    if not pager:
        raise RuntimeError("Paging form not found")
    
    pager_payload = {inp["name"]: inp.get("value","")
        for inp in pager.find_all("input", {"type":"hidden"}) if inp.get("name")}


    # Total pages
    last = soup.find("img", {"src": re.compile(r"last\.gif")})
    if last and last.parent and last.parent.get("href"):
        m = re.search(r"goPage\(\s*(\d+)\s*,", last.parent["href"])
        total = int(m.group(1)) if m else 1
    else:
        total = 1


    # Page looping
    for page in range(1, total+1):
        print(f"Page {page}/{total}")
        if page > 1:
            pp = pager_payload.copy()
            pp["pageCt"] = str(page)
            pp["PK"] = "0"
            r = session.post(DISPLAY_URL, data=pp, headers={"Referer": DISPLAY_URL})
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")


        # Download PDFs
        for a in soup.find_all("a", href=True):
            href = a["href"]
            full = href if href.lower().startswith("http") else urljoin(BASE_URL, href)
            if full.lower().endswith(".pdf"):
                download_file(session, full, out_dir)


        # Download Digital Data ZIPs
        for a in soup.find_all("a", string=lambda t: t and "Digital Data" in t):
            href = a["href"]
            full = href if href.lower().startswith("http") else urljoin(BASE_URL, href)
            download_file(session, full, out_dir)

    print("Finished Downloading. Files saved to", out_dir, "directory.")

if __name__ == "__main__":
    scrape_geofiles()