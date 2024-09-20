import requests
import os

import pandas as pd
import numpy as np

from sqlalchemy import URL, create_engine
from dotenv import load_dotenv
from pathlib import Path
from bs4 import BeautifulSoup


if __name__ == "__main__":

    # Scrape Data from web
    r = requests.get("https://bagong.pagasa.dost.gov.ph/automated-weather-station/")
    soup = BeautifulSoup(r.text, "lxml")
    site_table = soup.select("table.table tr")

    # Extract Header values
    cols = site_table[0].select("th")
    data_dict = {data.get_text(): [] for data in cols}

    # Extract Table values
    for row in site_table:

        cols = row.select("td")
        for key, data in zip(data_dict.keys(), cols):
            data_dict[key].append(data.get_text())

    # Create pandas dataframe
    df = pd.DataFrame(data=data_dict)

    # Remove the value's unit
    for col in ("Temperature", "Humidity", "Wind Speed", "Precipitation"):
        df[col] = df[col].apply(lambda value: value.split(" ")[0])

    # Replace missing values
    df.replace("--", np.nan, inplace=True)

    # Column data types
    df["Last Updated"] = pd.to_datetime(
        df["Last Updated"], format="%B %d, %Y, %I:%M %p"
    )

    col_dtypes = {
        "Site Name": str,
        "Temperature": float,
        "Humidity": float,
        "Wind Speed": float,
        "Wind Direction": str,
        "Precipitation": float,
        "Pressure": float,
        "Solar Radiation": float,
    }
    df = df.astype(col_dtypes)

    # Store dataframe into Postgres database

    filepath = Path(__file__).resolve().parent
    env_file = filepath / ".env"
    load_dotenv(env_file)

    DBHOST = os.environ.get("DBHOST")
    DBPORT = os.environ.get("DBPORT")
    DBUSER = os.environ.get("DBUSER")
    DBNAME = os.environ.get("DBNAME")
    DBPASS = os.environ.get("DBPASS")

    engine_url = URL.create(
        drivername="postgresql+psycopg2",
        username=DBUSER,
        password=DBPASS,
        host=DBHOST,
        port=DBPORT,
        database=DBNAME,
    )

    engine = create_engine(url=engine_url)
    df.to_sql(name="data", con=engine, if_exists="append")
