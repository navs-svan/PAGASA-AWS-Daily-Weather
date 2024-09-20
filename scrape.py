import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from sqlalchemy import URL, create_engine
import os
from dotenv import load_dotenv
from pathlib import Path


if __name__ == "__main__":

    # Scrape Data from web
    r = requests.get("https://bagong.pagasa.dost.gov.ph/automated-weather-station/")
    soup = BeautifulSoup(r.text, "lxml")
    site_table = soup.select("table.table tr")

    # Extract Header values
    cols = site_table[0].select("th")
    data_dict = {data.get_text():[] for data in cols}

    # Extract Table values
    for row in site_table:

        cols = row.select("td")
        for key, data in zip(data_dict.keys(),cols):
            data_dict[key].append(data.get_text())

    # Create pandas dataframe
    df = pd.DataFrame(data=data_dict)

    # Remove the value's unit
    for col in ("Temperature", "Humidity", "Wind Speed", "Precipitation"):
        df[col] = df[col].apply(lambda value : value.split(" ")[0])

    # Replace missing values
    df.replace("--", np.nan, inplace=True)

    # Column data types
    df["Last Updated"] = pd.to_datetime(df["Last Updated"], format="%B %d, %Y, %I:%M %p")

    col_dtypes = {"Site Name": str,
                "Temperature": float,
                "Humidity": float,
                "Wind Speed": float,
                "Wind Direction": str,
                "Precipitation": float,
                "Pressure": float,
                "Solar Radiation": float,
                }
    df = df.astype(col_dtypes)

    # PART 2

    # filepath = Path(__file__).resolve().parent
    # env_file = filepath / ".env"
    # load_dotenv(env_file)


    # print(filepath)
