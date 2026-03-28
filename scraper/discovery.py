"""
Discovery mode: Open Vahan report page, save HTML and screenshot for selector inspection.
Run this first to capture the page structure, then update config/scraping_config.py SELECTORS.
"""

import os
from pathlib import Path
import time

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

REPORT_URL = os.environ.get(
    "VAHAN_REPORT_URL",
    "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml",
).strip()
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output" / "discovery"


def run_discovery():
    """Open page, wait for load, save HTML and screenshot."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    # Run visible (not headless) so you can interact and inspect
    options.add_argument("--start-maximized")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )
    try:
        print(f"Opening {REPORT_URL} ...")
        driver.get(REPORT_URL)
        WebDriverWait(driver, 90).until(
            EC.presence_of_element_located((By.TAG_NAME, "form"))
        )
        time.sleep(5)  # Wait for JSF/JS to fully render

        # Save HTML
        html_path = OUTPUT_DIR / "reportview_page.html"
        html_path.write_text(driver.page_source, encoding="utf-8")
        print(f"Saved HTML: {html_path}")

        # Screenshot
        screenshot_path = OUTPUT_DIR / "reportview_screenshot.png"
        driver.save_screenshot(str(screenshot_path))
        print(f"Saved screenshot: {screenshot_path}")

        # List all form elements with id/name for quick reference
        ids = driver.find_elements(By.CSS_SELECTOR, "[id]")
        id_list = [e.get_attribute("id") for e in ids if e.get_attribute("id")]
        ids_path = OUTPUT_DIR / "element_ids.txt"
        ids_path.write_text("\n".join(sorted(set(id_list))), encoding="utf-8")
        print(f"Saved {len(id_list)} element IDs: {ids_path}")

        print("\nNext: Inspect reportview_page.html and element_ids.txt")
        print("Update config/scraping_config.py SELECTORS with actual IDs.")
        print("\nPress Enter to close browser...")
        input()
    finally:
        driver.quit()


if __name__ == "__main__":
    run_discovery()
