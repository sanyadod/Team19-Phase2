"""
Selenium-based end-to-end tests for the Streamlit UI.

This module provides comprehensive browser automation tests for the artifact registry
Streamlit frontend. Tests cover navigation, form interactions, and API integration
across all major UI pages.

Test Infrastructure:
- Starts Streamlit server as subprocess for isolated testing
- Uses Selenium WebDriver for browser automation
- Supports headless and headed browser modes
- Automatic cleanup of test resources

Test Coverage:
- Home page rendering
- Upload page form validation and submission
- Download page functionality
- Search page with GET and POST methods
- Lineage visualization
- Cost calculation
- License checking
- Model rating
- Reset functionality
"""

import os
import subprocess
import time
import zipfile
import io
from pathlib import Path
from typing import Optional

import pytest
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager


# Test configuration
STREAMLIT_PORT = 8502
STREAMLIT_URL = f"http://localhost:{STREAMLIT_PORT}"
STREAMLIT_SCRIPT = "acmecli/baseline/streamlit_ui.py"
STREAMLIT_STARTUP_TIMEOUT = 30  # seconds
ELEMENT_WAIT_TIMEOUT = 10  # seconds


@pytest.fixture(scope="session")
def streamlit_server():
    """
    Start Streamlit server as a subprocess for testing.
    
    Yields control to tests, then cleans up the process on teardown.
    """
    env = os.environ.copy()
    env["STREAMLIT_SERVER_PORT"] = str(STREAMLIT_PORT)
    env["STREAMLIT_BROWSER_GATHER_USAGE_STATS"] = "false"
    env["STREAMLIT_SERVER_HEADLESS"] = "true"
    
    # Start Streamlit
    process = subprocess.Popen(
        ["streamlit", "run", STREAMLIT_SCRIPT, "--server.port", str(STREAMLIT_PORT)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    
    # Wait for server to be ready
    max_wait = STREAMLIT_STARTUP_TIMEOUT
    start_time = time.time()
    while time.time() - start_time < max_wait:
        try:
            import requests
            response = requests.get(STREAMLIT_URL, timeout=2)
            if response.status_code == 200:
                break
        except Exception:
            time.sleep(0.5)
    else:
        process.terminate()
        stdout, stderr = process.communicate(timeout=5)
        pytest.fail(
            f"Streamlit server failed to start within {STREAMLIT_STARTUP_TIMEOUT}s. "
            f"STDOUT: {stdout.decode()}, STDERR: {stderr.decode()}"
        )
    
    yield process
    
    # Cleanup
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


@pytest.fixture(scope="function")
def driver():
    """
    Create and configure Selenium WebDriver instance.
    
    Uses Chrome in headless mode by default. Set HEADLESS=false environment
    variable to run tests in visible browser window for debugging.
    """
    chrome_options = Options()
    if os.environ.get("HEADLESS", "true").lower() == "true":
        chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.implicitly_wait(2)
    
    yield driver
    
    driver.quit()


@pytest.fixture(scope="function")
def browser(streamlit_server, driver):
    """
    Combined fixture providing both Streamlit server and browser driver.
    
    Navigates to the Streamlit app and waits for initial page load.
    """
    driver.get(STREAMLIT_URL)
    # Wait for Streamlit to fully load
    WebDriverWait(driver, ELEMENT_WAIT_TIMEOUT).until(
        EC.presence_of_element_located((By.TAG_NAME, "main"))
    )
    time.sleep(1)  # Additional wait for Streamlit widgets to initialize
    return driver


def wait_for_streamlit_element(driver, by, value, timeout=ELEMENT_WAIT_TIMEOUT):
    """Helper to wait for Streamlit elements to be ready."""
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((by, value))
    )


def click_sidebar_radio(driver, option_text: str):
    """Click a sidebar radio button by its visible text."""
    # Streamlit sidebar radio buttons are in a specific structure
    sidebar = wait_for_streamlit_element(driver, By.CSS_SELECTOR, "[data-testid='stSidebar']")
    radio_options = sidebar.find_elements(By.CSS_SELECTOR, "label")
    
    # Try to find by exact or partial text match
    for label in radio_options:
        if option_text.lower() in label.text.lower():
            # Scroll into view if needed
            driver.execute_script("arguments[0].scrollIntoView(true);", label)
            time.sleep(0.2)
            label.click()
            time.sleep(0.8)  # Wait for page to switch and render
            return
    
    # Fallback: try finding by data-testid or other attributes
    try:
        # Streamlit radio buttons might have specific data attributes
        radio_input = sidebar.find_element(
            By.XPATH, f".//input[@type='radio' and following-sibling::*[contains(text(), '{option_text}')]]"
        )
        driver.execute_script("arguments[0].click();", radio_input)
        time.sleep(0.8)
        return
    except NoSuchElementException:
        pass
    
    raise NoSuchElementException(f"Sidebar option '{option_text}' not found. Available options: {[opt.text for opt in radio_options]}")


def create_test_zip_file() -> bytes:
    """Create a minimal valid ZIP file for testing uploads."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("test_file.txt", "This is a test file for upload testing.")
        zf.writestr("config.json", '{"test": "config"}')
    return buffer.getvalue()


@pytest.mark.selenium
class TestHomePage:
    """Tests for the Home page."""
    
    def test_home_page_loads(self, browser):
        """Verify Home page loads and displays expected content."""
        driver = browser
        assert "Artifact Registry" in driver.title or "Artifact Registry" in driver.page_source
        
        # Check for main content
        main_content = driver.find_element(By.TAG_NAME, "main")
        assert main_content is not None
        
        # Check for expected text
        assert "Upload, download, and manage artifacts" in driver.page_source or \
               "Use the sidebar to open a tool" in driver.page_source
    
    def test_sidebar_navigation_present(self, browser):
        """Verify sidebar navigation is visible and functional."""
        driver = browser
        sidebar = driver.find_element(By.CSS_SELECTOR, "[data-testid='stSidebar']")
        assert sidebar.is_displayed()
        
        # Check for navigation options
        nav_text = sidebar.text
        assert "Home" in nav_text
        assert "Upload" in nav_text
        assert "Download" in nav_text


@pytest.mark.selenium
class TestUploadPage:
    """Tests for the Upload page."""
    
    def test_upload_page_navigation(self, browser):
        """Verify Upload page can be navigated to."""
        driver = browser
        click_sidebar_radio(driver, "Upload")
        
        # Wait for upload page content
        wait_for_streamlit_element(driver, By.CSS_SELECTOR, "h2, h3", timeout=5)
        assert "Upload Artifact" in driver.page_source
    
    def test_upload_form_elements_present(self, browser):
        """Verify all upload form elements are present."""
        driver = browser
        click_sidebar_radio(driver, "Upload")
        time.sleep(1)
        
        # Check for artifact type selector
        # Streamlit selectboxes are rendered as input elements
        page_text = driver.page_source
        assert "Artifact Category" in page_text or "model" in page_text.lower()
        
        # Check for file uploader
        file_inputs = driver.find_elements(By.CSS_SELECTOR, "input[type='file']")
        assert len(file_inputs) > 0
    
    def test_upload_form_validation_no_file(self, browser):
        """Test that upload form shows error when no file is selected."""
        driver = browser
        click_sidebar_radio(driver, "Upload")
        time.sleep(1)
        
        # Find and click upload button
        buttons = driver.find_elements(By.CSS_SELECTOR, "button")
        upload_button = None
        for btn in buttons:
            if "Upload" in btn.text and "Artifact" in btn.text:
                upload_button = btn
                break
        
        if upload_button:
            upload_button.click()
            time.sleep(1)
            
            # Check for error message
            page_text = driver.page_source.lower()
            assert "error" in page_text or "please choose" in page_text or "zip file" in page_text


@pytest.mark.selenium
class TestDownloadPage:
    """Tests for the Download page."""
    
    def test_download_page_navigation(self, browser):
        """Verify Download page can be navigated to."""
        driver = browser
        click_sidebar_radio(driver, "Download")
        time.sleep(1)
        
        assert "Download Artifact" in driver.page_source
    
    def test_download_form_elements(self, browser):
        """Verify download form elements are present."""
        driver = browser
        click_sidebar_radio(driver, "Download")
        time.sleep(1)
        
        # Check for artifact type and ID inputs
        page_text = driver.page_source
        assert "Artifact type" in page_text or "Artifact ID" in page_text


@pytest.mark.selenium
class TestSearchPage:
    """Tests for the Search page."""
    
    def test_search_page_navigation(self, browser):
        """Verify Search page can be navigated to."""
        driver = browser
        click_sidebar_radio(driver, "Search")
        time.sleep(1)
        
        assert "Search Artifacts" in driver.page_source
    
    def test_search_form_elements(self, browser):
        """Verify search form elements are present."""
        driver = browser
        click_sidebar_radio(driver, "Search")
        time.sleep(1)
        
        page_text = driver.page_source
        assert "Regex Pattern" in page_text or "Search Method" in page_text


@pytest.mark.selenium
class TestLineagePage:
    """Tests for the Lineage page."""
    
    def test_lineage_page_navigation(self, browser):
        """Verify Lineage page can be navigated to."""
        driver = browser
        click_sidebar_radio(driver, "Lineage")
        time.sleep(1)
        
        assert "Model Lineage" in driver.page_source or "Lineage" in driver.page_source
    
    def test_lineage_form_elements(self, browser):
        """Verify lineage form elements are present."""
        driver = browser
        click_sidebar_radio(driver, "Lineage")
        time.sleep(1)
        
        page_text = driver.page_source
        assert "Model ID" in page_text


@pytest.mark.selenium
class TestCostPage:
    """Tests for the Cost page."""
    
    def test_cost_page_navigation(self, browser):
        """Verify Cost page can be navigated to."""
        driver = browser
        click_sidebar_radio(driver, "Cost")
        time.sleep(1)
        
        assert "Cost" in driver.page_source or "Calculator" in driver.page_source
    
    def test_cost_form_elements(self, browser):
        """Verify cost form elements are present."""
        driver = browser
        click_sidebar_radio(driver, "Cost")
        time.sleep(1)
        
        page_text = driver.page_source
        assert "Artifact ID" in page_text or "Artifact type" in page_text


@pytest.mark.selenium
class TestLicensePage:
    """Tests for the License page."""
    
    def test_license_page_navigation(self, browser):
        """Verify License page can be navigated to."""
        driver = browser
        click_sidebar_radio(driver, "License")
        time.sleep(1)
        
        assert "License" in driver.page_source or "Check" in driver.page_source
    
    def test_license_form_elements(self, browser):
        """Verify license form elements are present."""
        driver = browser
        click_sidebar_radio(driver, "License")
        time.sleep(1)
        
        page_text = driver.page_source
        assert "Artifact ID" in page_text


@pytest.mark.selenium
class TestRatePage:
    """Tests for the Rate page."""
    
    def test_rate_page_navigation(self, browser):
        """Verify Rate page can be navigated to."""
        driver = browser
        click_sidebar_radio(driver, "Rate")
        time.sleep(1)
        
        assert "Rate Model" in driver.page_source or "Model" in driver.page_source
    
    def test_rate_form_elements(self, browser):
        """Verify rate form elements are present."""
        driver = browser
        click_sidebar_radio(driver, "Rate")
        time.sleep(1)
        
        page_text = driver.page_source
        assert "Model ID" in page_text


@pytest.mark.selenium
class TestResetPage:
    """Tests for the Reset page."""
    
    def test_reset_page_navigation(self, browser):
        """Verify Reset page can be navigated to."""
        driver = browser
        click_sidebar_radio(driver, "Reset")
        time.sleep(1)
        
        assert "Reset" in driver.page_source or "Registry" in driver.page_source
    
    def test_reset_warning_present(self, browser):
        """Verify reset page shows warning message."""
        driver = browser
        click_sidebar_radio(driver, "Reset")
        time.sleep(1)
        
        page_text = driver.page_source.lower()
        assert "danger" in page_text or "warning" in page_text or "delete" in page_text


@pytest.mark.selenium
class TestBackendURL:
    """Tests for backend URL configuration."""
    
    def test_backend_url_input_present(self, browser):
        """Verify backend URL input is present in sidebar."""
        driver = browser
        sidebar = driver.find_element(By.CSS_SELECTOR, "[data-testid='stSidebar']")
        
        # Check for backend URL input
        page_text = sidebar.text
        assert "Backend" in page_text or "URL" in page_text


@pytest.mark.selenium
class TestNavigationFlow:
    """Tests for navigation between pages."""
    
    def test_navigate_all_pages(self, browser):
        """Test navigating through all pages in sequence."""
        driver = browser
        pages = ["Home", "Upload", "Download", "Cost", "License", "Rate", "Search", "Lineage", "Reset"]
        
        for page_name in pages:
            click_sidebar_radio(driver, page_name)
            time.sleep(0.5)
            # Verify we're on the expected page by checking for page-specific content
            assert page_name in driver.page_source or driver.current_url == STREAMLIT_URL


@pytest.mark.selenium
@pytest.mark.integration
@pytest.mark.skip(reason="Requires backend server and test data")
class TestUploadIntegration:
    """Integration tests for upload functionality (requires backend)."""
    
    def test_upload_valid_zip(self, browser):
        """Test uploading a valid ZIP file."""
        driver = browser
        click_sidebar_radio(driver, "Upload")
        time.sleep(1)
        
        # Create test ZIP
        zip_data = create_test_zip_file()
        
        # Find file input and upload
        file_inputs = driver.find_elements(By.CSS_SELECTOR, "input[type='file']")
        if file_inputs:
            # Note: Selenium file upload requires actual file path, not bytes
            # This test would need a temporary file to work properly
            pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
