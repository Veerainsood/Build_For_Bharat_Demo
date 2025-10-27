#!/usr/bin/env python3
"""
selenium_json_bulk_downloader.py

Usage:
  python selenium_json_bulk_downloader.py \
    --json-index path/to/sector_index.json \
    --family "Crop Development & Seed Production" \
    --download-dir ./manual_imd_downloads
"""

import argparse
import json
import time
import urllib.parse
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import os
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# Config
from urllib.parse import quote_plus
DOWNLOAD_WAIT_TIMEOUT = 5  # seconds to wait for a download to finish per file
INTER_FILE_PAUSE = 1.0       # seconds between items

def setup_driver(download_dir: str, headless: bool = False):
    chrome_options = Options()
    chrome_options.binary_location = "/usr/bin/google-chrome"  # ✅ point to your real Chrome

    prefs = {
        "download.default_directory": str(Path(download_dir).resolve()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_driver = os.environ.get("CHROMEDRIVER")
    
    if headless:
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
    
    driver = webdriver.Chrome(service=Service(chrome_driver), options=chrome_options)
    driver.set_page_load_timeout(60)
    return driver


def wait_for_download_completion(download_dir: Path, before_files: set, timeout=DOWNLOAD_WAIT_TIMEOUT):
    """
    Wait until a new file appears and no .crdownload temporary files remain.
    Returns (True, set_of_new_files) or (False, reason)
    """
    start = time.time()
    while True:
        now = time.time()
        if now - start > timeout:
            return False, "timeout"
        current_files = set(download_dir.iterdir())
        new_files = current_files - before_files
        # if any new file exists and there are no .crdownload temp files, assume complete
        crdownloads = [f for f in current_files if f.suffix == ".crdownload"]
        if new_files and not crdownloads:
            return True, new_files
        time.sleep(0.5)


def detect_captcha_or_form(driver):
    """
    Heuristic: detect inputs or images labelled 'captcha' or modal overlays.
    If True, caller should prompt user to solve the captcha manually.
    """
    try:
        # input or placeholder named or id includes 'captcha' or 'code'
        inputs = driver.find_elements(By.XPATH,
            "//input[contains(translate(@id,'CAPTCHA','captcha'),'captcha') or "
            "contains(translate(@name,'CAPTCHA','captcha'),'captcha') or "
            "contains(translate(@placeholder,'CAPTCHA','captcha'),'captcha') or "
            "contains(translate(@aria-label,'CAPTCHA','captcha'),'captcha') or "
            "contains(translate(@id,'CODE','code'),'code')]"
        )
        if inputs:
            return True
    except Exception:
        pass

    try:
        imgs = driver.find_elements(By.XPATH, "//img[contains(translate(@src,'CAPTCHA','captcha'),'captcha') or contains(translate(@alt,'CAPTCHA','captcha'),'captcha')]")
        if imgs:
            return True
    except Exception:
        pass

    # modal / overlay heuristics
    try:
        overlays = driver.find_elements(By.XPATH, "//div[contains(@class,'modal') or contains(@class,'overlay') or contains(@class,'captcha')]")
        for ov in overlays:
            if ov.is_displayed():
                return True
    except Exception:
        pass

    return False


def find_and_click_download_button(driver):
    """Click the exact download CSV button observed on data.gov.in pages."""
    selectors = [
        "a[title='Download File'][role='button']",
        "a[title='Download File']",
        "//a[@title='Download File']",
        "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'download')]",
        "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'download')]"
    ]
    for sel in selectors:
        try:
            if sel.startswith("//"):
                elems = driver.find_elements(By.XPATH, sel)
            else:
                elems = driver.find_elements(By.CSS_SELECTOR, sel)
            for e in elems:
                try:
                    if e.is_displayed() and e.is_enabled():
                        # try normal click; fallback to JS click if necessary
                        try:
                            e.click()
                        except WebDriverException:
                            driver.execute_script("arguments[0].click();", e)
                        return True
                except Exception:
                    continue
        except Exception:
            continue
    return False


def collect_titles_from_json(json_path: Path, family_name: str):
    with json_path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if family_name not in data:
        raise KeyError(f"Family '{family_name}' not found in JSON. Available keys: {list(data.keys())}")
    items = data[family_name]
    # items expected to be list of dict {id, title}
    titles = []
    for it in items:
        t = it.get("title") or it.get("name") or it.get("label")
        if t:
            titles.append(t)
    return titles

def fill_download_form(driver, name, email, mobile, usage="Non Commercial", purposes=["Academia"], capcha=""):
    """Fill the data.gov.in download form using stable selectors (name/value/text)."""
    wait = WebDriverWait(driver, 15)

    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "form")))

        # ---- Usage type (radio buttons) ----
        radios = driver.find_elements(By.NAME, "usagetype")
        for radio in radios:
            label = driver.find_element(By.CSS_SELECTOR, f"label[for='{radio.get_attribute('id')}']").text.lower()
            if usage.lower() in label:
                driver.execute_script("arguments[0].click();", radio)
                print(f"✅ Selected usage: {label}")
                break

        # ---- Purpose checkboxes ----
        boxes = driver.find_elements(By.NAME, "purpose[]")
        for box in boxes:
            label = driver.find_element(By.CSS_SELECTOR, f"label[for='{box.get_attribute('id')}']").text.strip()
            if label in purposes:
                driver.execute_script("arguments[0].click();", box)
                print(f"✅ Selected purpose: {label}")

        # ---- Text inputs (name, mobile, email) ----
        driver.find_element(By.NAME, "name").send_keys(name)
        driver.find_element(By.NAME, "mobile").send_keys(mobile)
        driver.find_element(By.NAME, "email").send_keys(email)
        driver.find_element(By.NAME, "form_captcha").send_keys(capcha)
        print("✅ Filled contact fields")

        # ---- Captcha focus ----
        captcha_input = driver.find_element(By.NAME, "form_captcha")
        captcha_input.click()
        print("⚠️ Waiting for manual captcha entry...")

    except Exception as e:
        print("⚠️ Could not fill form:", e)

def fetch_and_show_captcha(driver, save_dir="captchas", show=True):
    """
    Detects the CAPTCHA image in the data.gov.in download form,
    downloads it for clarity, optionally displays it, and returns the local file path.
    """
    wait = WebDriverWait(driver, 10)
    os.makedirs(save_dir, exist_ok=True)

    try:
        # Find the <img alt="Captcha Image"> reliably
        img_el = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "img[alt='Captcha Image']")))
        src = img_el.get_attribute("src")

        if not src or "image-captcha-generate" not in src:
            print("⚠️ Captcha image not detected or has invalid src.")
            return None
        import requests
        # Download
        r = requests.get(src, timeout=10)
        r.raise_for_status()

        # Save for record
        # ts = time.strftime("%Y%m%d_%H%M%S")
        # fpath = os.path.join(save_dir, f"captcha_{ts}.png")
        # with open(fpath, "wb") as f:
        #     f.write(r.content)

        # print(f"🖼️ Captcha image saved: {fpath}")
        import PIL.Image as Image
        from io import BytesIO
        # Optionally open   
        if show:
            img = Image.open(BytesIO(r.content))
            buf = BytesIO()
            img.save(buf, format="PNG")
            buf.seek(0)

            res = requests.post(
                "http://localhost:5001/predict",
                files={"image": ("captcha.png", buf, "image/png")},
            )
            return res.json()['prediction'].upper()
        return "Dont Know"

        # return fpath

    except Exception as e:
        print(f"⚠️ Error fetching captcha: {e}")
        return None

def download_for_titles(driver, titles, download_dir: Path, max_items=None):
    def twenty_adder(s: str) -> str:
        # replace spaces with %20 for URL
        s = s.replace(" ", "%20")
        return s

    download_dir.mkdir(parents=True, exist_ok=True)
    total = len(titles)
    for idx, title in enumerate(titles, start=1):
        if max_items and idx > max_items:
            break
        print(f"\n[{idx}/{total}] Title: {title}")
        search_url = (
            "https://www.data.gov.in/search?"
            f"title={twenty_adder(title)}"
            "&sortby=_score&type=resources&exact=1"
        )
        try:
            driver.get(search_url)
        except Exception as e:
            print("Page load error:", e)
            continue

        # small settling time
        wait = WebDriverWait(driver, 5)
        try:
        # wait for the exact match checkbox
            exact_box = wait.until(EC.presence_of_element_located((By.ID, "checkbox-1")))
            if not exact_box.is_selected():
                driver.execute_script("arguments[0].click();", exact_box)
                print("✅ Checked 'Exact Match' box")
            time.sleep(3)
        except Exception as e:
            print("⚠️ Could not enable exact match:", e)
        # # if captcha/form present before clicking, prompt user
        # if detect_captcha_or_form(driver):
        #     print("Captcha or form detected on search/result page. Please solve it in the browser.")
        #     input("After solving and ensuring the Download button is visible, press ENTER to continue...")

        # attempt to click the Download button on the result card
        clicked = find_and_click_download_button(driver)
        if not clicked:
            print("Could not find the download button automatically. Please click the CSV/Download button manually in the browser, then press ENTER.")
            input("Press ENTER after you clicked Download manually...")
        else:
            print("Clicked Download button (or attempted click).")

        # If a modal appears after clicking that requires captcha, wait for user again
        if detect_captcha_or_form(driver):
            capcha = fetch_and_show_captcha(driver)
            fill_download_form(
                driver,
                name="Veerain Sood",
                email="veerainsood1@gmail.com",
                mobile="7710449767",
                usage="Non Commercial",
                purposes=["Academia", "R&D"],
                capcha = capcha
            )
            print("Captcha detected after clicking. Please solve it now in the browser.")
            input("After solving the captcha and confirming the download, press ENTER to continue...")

        # wait for download to finish
        # before = set(download_dir.iterdir())
        # ok, info = wait_for_download_completion(download_dir, before, timeout=DOWNLOAD_WAIT_TIMEOUT)
        # if ok:
        #     print(f"✅ Download finished: {info}")
        # else:
        #     print(f"⚠️ Download did not finish: {info}. Check browser downloads.")

        time.sleep(INTER_FILE_PAUSE)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json-index", required=True, help="Path to the JSON index file containing family -> list of {id,title}")
    parser.add_argument("--family", required=True, help='Family key e.g. "Crop Development & Seed Production"')
    parser.add_argument("--download-dir", default="./manual_imd_downloads")
    parser.add_argument("--max", type=int, default=0, help="Max titles to process (0 = all)")
    parser.add_argument("--headless", action="store_true", help="Run headless (not recommended for captchas)")
    args = parser.parse_args()

    json_path = Path(args.json_index)
    if not json_path.exists():
        print("JSON index not found:", json_path)
        return

    # breakpoint()
    try:
        titles = collect_titles_from_json(json_path, args.family)
    except KeyError as e:
        print(e)
        return

    if not titles:
        print("No titles found for family.")
        return

    dl_dir = Path(args.download_dir).resolve()
    driver = setup_driver(str(dl_dir), headless=args.headless)
    try:
        download_for_titles(driver, titles, dl_dir, max_items=(args.max or None))
    finally:
        print("Done. Closing browser...")
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    import argparse
    main()
