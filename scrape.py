import requests
from bs4 import BeautifulSoup


r = requests.get("https://bagong.pagasa.dost.gov.ph/automated-weather-station/")

soup = BeautifulSoup(r.text, "lxml")
