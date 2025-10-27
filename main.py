from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import os

chrome_driver = os.environ.get("CHROMEDRIVER")
chrome_binary = os.environ.get("CHROME_BIN")

print("Chromedriver:", chrome_driver)
print("Chrome binary:", chrome_binary)

opts = Options()
if chrome_binary:
    opts.binary_location = chrome_binary

driver = webdriver.Chrome(service=Service(chrome_driver), options=opts)
driver.get("https://www.puppy.com")

input("Press Enter to quit... ")
driver.quit()
