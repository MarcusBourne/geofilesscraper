import os
import re
import asyncio
import threading
import queue
from urllib.parse import urljoin
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
    Error as PlaywrightError,
)
import tkinter as tk
from tkinter import font as tkfont
from tkinter import ttk, scrolledtext, messagebox

BASE_URL        = "https://gis.gov.nl.ca/minesen/geofiles/"
ENTRY_PATH      = "default.asp"
EXTERNAL_PREFIX = "https://www.gov.nl.ca/iet/mines-geoscience-reports-maps-docs"
USER_AGENT      = "PDF-Scraper/1.0"
SKIP_KEYWORDS   = ("map", "research")
REQUEST_TIMEOUT = 500
RESUME_FILE     = "resume.txt"
MISSING_FILE    = "filenames.txt"

log_queue   = queue.Queue()
pause_event = threading.Event()

def gui_log(msg):
    log_queue.put(msg)

def record_missing(name: str):
    with open(MISSING_FILE, "a") as f:
        f.write(f"{name}\n")

def get_resume_page():
    if os.path.exists(RESUME_FILE):
        try:
            return int(open(RESUME_FILE).read().split(',')[0].strip())
        except Exception:
            return 1
    return 1

def download_file(url: str):
    filename = os.path.basename(url)
    bold_name = f"**{filename}**"

    if any(kw in filename.lower() for kw in SKIP_KEYWORDS):
        gui_log(f"Filtered out: {bold_name}")
        return

    downloads_dir = "downloads"
    os.makedirs(downloads_dir, exist_ok=True)
    filepath = os.path.join(downloads_dir, filename)

    if os.path.exists(filepath):
        gui_log(f"Skipping {bold_name}, already downloaded.")
        return

    gui_log(f"Downloading: {bold_name}")
    r = requests.get(
        url,
        stream=True,
        headers={'User-Agent': USER_AGENT},
        timeout=REQUEST_TIMEOUT
    )
    r.raise_for_status()
    with open(filepath, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    gui_log(f"Saved: {bold_name} to {downloads_dir}/")

def process(html: str):
    soup = BeautifulSoup(html, 'lxml')
    for a in soup.find_all('a', href=True):
        href = a['href']
        full = href if href.startswith('http') else urljoin(BASE_URL, href)

        if full.lower().endswith(('.pdf', '.zip')):
            download_file(full)

        elif full.startswith(EXTERNAL_PREFIX):
            got_any = False
            try:
                resp = requests.get(
                    full,
                    headers={'User-Agent': USER_AGENT},
                    timeout=REQUEST_TIMEOUT
                )
                resp.raise_for_status()
                ext_soup = BeautifulSoup(resp.text, 'lxml')
                for b in ext_soup.find_all('a', href=True):
                    link = b['href']
                    u = link if link.startswith('http') else urljoin(full, link)
                    if u.lower().endswith(('.pdf', '.zip')):
                        download_file(u)
                        got_any = True
                if not got_any:
                    mn = os.path.basename(full.rstrip('/'))
                    gui_log(f"No downloads found on **{mn}**, logging.")
                    record_missing(mn)
            except Exception as e:
                gui_log(f"Error scraping external {full}: {e}")

async def scraper_main():
    open(MISSING_FILE, 'w').close()
    entry = urljoin(BASE_URL, ENTRY_PATH)
    gui_log("\n========================= ✓ Starting Scraper ✓ =========================\n")
    gui_log(f"🔗 Entering Webpage: {BASE_URL} 🔗\n")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        page = await browser.new_page(user_agent=USER_AGENT)

        await page.goto(entry)
        await page.wait_for_selector("form[name=searchForm]")
        await page.wait_for_timeout(2000)
        await page.evaluate("document.forms['searchForm'].submit()")
        await page.wait_for_load_state('networkidle')
        await page.wait_for_timeout(2000)

        resume_page = get_resume_page()
        if resume_page > 1:
            gui_log(f"🔄 Resuming from page {resume_page} 🔄")
            try:
                await page.evaluate(f"goPage({resume_page}, 'display.asp')")
            except PlaywrightError:
                gui_log(f"⚠️ goPage not available for resuming at page {resume_page}")
            await page.wait_for_load_state('networkidle')
            await page.wait_for_timeout(2000)

        soup0 = BeautifulSoup(await page.content(), 'lxml')
        last = soup0.select_one("img[src*='last.gif']")
        total = int(
            re.search(r"goPage\(\s*(\d+)", last.parent['href']).group(1)
            if last and last.parent and last.parent.has_attr('href') else 1
        )
        gui_log(f"📄 Total pages found: {total} 📄")

        for i in range(resume_page, total + 1):
            while pause_event.is_set():
                gui_log("⏸️ Paused. Waiting to resume…")
                await asyncio.sleep(1)

            gui_log(f"\nScraping Page: {i}/{total} \n")
            html = ""
            for attempt in range(2):
                try:
                    await page.wait_for_load_state('networkidle', timeout=15000)
                    html = await page.content()
                    break
                except (PlaywrightTimeoutError, PlaywrightError) as e:
                    if attempt == 0:
                        gui_log(f"⚠️ Issue fetching page {i}, retrying…")
                        await asyncio.sleep(2)
                    else:
                        gui_log(f"❌ Still failing on page {i}: {e}")

            process(html)

            now = datetime.now()
            with open(RESUME_FILE, 'w') as f:
                f.write(f"{i}, time: {now:%I:%M %p}, date: {now:%Y-%m-%d}")

            if i < total:
                next_page = i + 1
                try:
                    await page.evaluate(f"goPage({next_page}, 'display.asp')")
                    await page.wait_for_load_state('networkidle')
                    await page.wait_for_timeout(2000)
                    continue
                except PlaywrightError as e:
                    gui_log(f"⚠️ goPage eval failed for page {next_page}: {e}")
                gui_log(f"❌ Cannot navigate to page {next_page}, stopping.")
                break

        await browser.close()
    gui_log("======================= ✓ Scraping Complete ✓ =======================")

def run_scraper():
    asyncio.run(scraper_main())

def start_gui():
    root = tk.Tk()
    root.title("Mines and Energy Geofiles Scraper")
    root.geometry("800x600")

    frame = ttk.Frame(root, padding=10)
    frame.pack(fill=tk.BOTH, expand=True)

    logo_img = tk.PhotoImage(file="cna.png").subsample(2, 2)
    logo_label = tk.Label(frame, image=logo_img)
    logo_label.image = logo_img
    logo_label.pack(pady=(0, 10))

    tk.Label(frame, text="Geofiles Scraper", font=("Georgia", 30, "bold")).pack()

    btn_frame = ttk.Frame(frame)
    btn_frame.pack(pady=10)

    tk.Button(
        btn_frame, text="Start Scraper", font=("Georgia", 10),
        command=lambda: threading.Thread(target=run_scraper, daemon=True).start()
    ).pack(side=tk.LEFT, padx=5, ipadx=20, ipady=10)

    tk.Button(
        btn_frame, text="Exit Scraper", font=("Georgia", 10),
        command=root.destroy
    ).pack(side=tk.LEFT, padx=5, ipadx=20, ipady=10)

    log_widget = scrolledtext.ScrolledText(frame, state='disabled', wrap=tk.WORD)
    bold_font = tkfont.nametofont(log_widget.cget("font")).copy()
    bold_font.configure(weight="bold")
    log_widget.tag_configure('bold', font=bold_font)
    log_widget.pack(fill=tk.BOTH, expand=True)

    def update_log():
        while not log_queue.empty():
            msg = log_queue.get()
            parts = msg.split('**')
            log_widget.config(state='normal')
            for idx, part in enumerate(parts):
                tag = 'bold' if idx % 2 else None
                log_widget.insert(tk.END, part, tag)
            log_widget.insert(tk.END, '\n')
            log_widget.see(tk.END)
            log_widget.config(state='disabled')
        root.after(100, update_log)

    root.after(100, update_log)
    root.mainloop()

if __name__ == '__main__':
    start_gui()