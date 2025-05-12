import os
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup


BASE_URL = "https://gis.gov.nl.ca/minesen/geofiles/"
DEFAULT_URL = urljoin(BASE_URL, "default.asp")
DISPLAY_URL = urljoin(BASE_URL, "display.asp")


def download_file(session, url, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    fname = os.path.basename(url)
    path = os.path.join(out_dir, fname)
    if os.path.exists(path):
        return
    print(f"Downloading: {fname}")
    r = session.get(url)
    r.raise_for_status()
    with open(path, "wb") as f:
        f.write(r.content)


def scrape_geofiles(title=None, pdf_only=True, out_dir="pdfs"):
    """
    Scrape GeoFiles search results:
      1. Download PDFs
      2. Download "Digital Data" ZIPs
    """
    print("Starting Web Scraper....Please Wait.")
    session = requests.Session()
    session.headers["User-Agent"] = "PDF-Scraper/1.0"


    # Load search form
    r = session.get(DEFAULT_URL)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    form = soup.find("form", {"name": "searchForm", "action": "display.asp"})
    if not form:
        raise RuntimeError("Search form not found on default.asp")


    # Build payload
    payload = {inp.get("name"): inp.get("value", "")
               for inp in form.find_all("input", type=["hidden", "text"]) if inp.get("name")}
    if pdf_only:
        payload["FullText"] = "ON"
    for sel in form.find_all("select"):
        name = sel.get("name")
        if name:
            opt = sel.find("option", selected=True) or sel.find("option")
            payload[name] = opt.get("value", "") if opt else ""
    if title:
        payload["title"] = title


    # Submit search
    r = session.post(
        DISPLAY_URL,
        data=payload,
        headers={"Referer": DEFAULT_URL, "Origin": "https://gis.gov.nl.ca"}
    )
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")


    # Download PDFs
    for a in soup.select("a[href$='.pdf']"):
        download_file(session, urljoin(BASE_URL, a['href']), out_dir)


    # Download Digital Data ZIPs
    for a in soup.find_all("a", string=lambda s: s and "Digital Data" in s):
        download_file(session, urljoin(BASE_URL, a['href']), out_dir)

    print("Finished Downloading. Files saved to", out_dir, "directory.")


if __name__ == "__main__":
    scrape_geofiles()