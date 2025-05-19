import io
import logging
import os
import pandas as pd
import requests
import sqlite3
from datetime import datetime

# Колонки для запросов
COLUMNS_SEC_FUTURES = "SECID,SHORTNAME,LASTDELDATE,SECTYPE,ASSETCODE,PREVOPENPOSITION,LOTVOLUME,INITIALMARGIN,TIME"
COLUMNS_MD_FUTURES = "SYSTIME,SECID,SPREAD,LAST,OPENPOSITION,NUMTRADES,TIME"
COLUMNS_SEC_SHARES = "SECID,SHORTNAME,LOTSIZE"
COLUMNS_MD_SHARES = "SYSTIME,SECID,BID,OFFER,SPREAD,LAST,TIME,SYSTIME"

# Словарь замен
replacements = {
    "BELUGA": "BELU",
    "ISKJ": "ABIO",
    "GAZR": "GAZP",
    "MTSI": "MTSS",
    "NOTK": "NVTK",
    "SBRF": "SBER",
    "SBPR": "SBERP",
    "SNGR": "SNGS",
    "SNGP": "SNGSP",
    "TRNF": "TRNFP",
    "TATP": "TATNP",
}


def load_futures_data():
    """Загрузка данных по фьючерсам."""
    try:
        logging.info("Начало загрузки данных по фьючерсам.")
        
        if not os.path.exists("secid.txt"):
            raise FileNotFoundError("Файл secid.txt не найден.")
        with open("secid.txt", "r") as file:
            secids = file.read().strip().split(",")
        if not secids or all(not secid.strip() for secid in secids):
            raise ValueError("Файл secid.txt пуст или содержит некорректные данные.")

        logging.info(f"Список SECID из secid.txt: {secids}")

        url_futures = (
            f"https://iss.moex.com/iss/engines/futures/markets/forts/boards/rfud/securities.csv"
            f"?securities={','.join(secids)}"
            "&iss.only=securities,marketdata"
            f"&securities.columns={COLUMNS_SEC_FUTURES}"
            f"&marketdata.columns={COLUMNS_MD_FUTURES}"
        )

        response = requests.get(url_futures)
        if response.status_code != 200:
            raise ConnectionError(f"Ошибка при загрузке данных по фьючерсам: {response.status_code}")
        with open("temp_futures.csv", "wb") as file:
            file.write(response.content)

        logging.info("Данные по фьючерсам успешно загружены.")

        # Чтение данных в pandas
        with open("temp_futures.csv", "r") as file:
            data = file.read()
        parts = data.split("marketdata")
        if len(parts) < 2:
            raise ValueError("Некорректный формат данных в файле futures.csv.")

        # Обрабатываем первую таблицу (securities)
        securities_data = parts[0].strip()
        securities_data = securities_data.replace("securities", "").strip()
        securities_df = pd.read_csv(io.StringIO(securities_data), sep=";")
        
        # Обрабатываем вторую таблицу (marketdata)
        marketdata_data = parts[1].strip()
        marketdata_df = pd.read_csv(io.StringIO(marketdata_data), sep=";")
        
        # Объединяем
        futures = pd.merge(securities_df, marketdata_df, on="SECID")

        # Замены ASSETCODE
        for old, new in replacements.items():
            futures["ASSETCODE"] = futures["ASSETCODE"].replace(old, new)

        return futures

    except Exception as e:
        logging.error(f"Ошибка при загрузке данных по фьючерсам: {e}")
        exit(1)


def save_futures_to_db_and_csv(futures):
    """Сохранение данных по фьючерсам в CSV и SQLite."""
    try:
        logging.info("Начало сохранения данных по фьючерсам.")

        # Создание директории и сохранение в CSV и SQLite
        os.makedirs("data/futures", exist_ok=True)
        today = datetime.now().strftime("%d-%m-%y")
        futures.to_csv(f"data/futures/futures_{today}.csv", index=False)
        
        conn = sqlite3.connect("data/spread.db")
        futures.to_sql("futures", conn, if_exists="append", index=False)
        conn.close()

        logging.info("Данные по фьючерсам успешно сохранены в CSV и SQLite.")
        
    except Exception as e:
        logging.error(f"Ошибка при сохранении фьючерсов: {e}")
        exit(1)


def load_shares_data(set_asset):
    """Загрузка данных по акциям."""
    try:
        logging.info("Начало загрузки данных по акциям.")
        
        # Формирование URL для акций
        url_shares = (
            f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.csv"
            f"?securities={','.join(set_asset)}"
            "&iss.only=securities,marketdata"
            f"&securities.columns={COLUMNS_SEC_SHARES}"
            f"&marketdata.columns={COLUMNS_MD_SHARES}"
        )
        
        # Загрузка данных по акциям
        response = requests.get(url_shares)
        if response.status_code != 200:
            raise ConnectionError(f"Ошибка при загрузке данных по акциям: {response.status_code}")
        with open("temp_shares.csv", "wb") as file:
            file.write(response.content)

        logging.info("Данные по акциям успешно загружены.")
       
        # Чтение данных в pandas
        with open("temp_shares.csv", "r") as file:
            data = file.read()

        parts = data.split("marketdata")
        if len(parts) < 2:
            raise ValueError("Некорректный формат данных в файле shares.csv.")

        # Обрабатываем первую таблицу (securities)
        securities_data = parts[0].strip()
        securities_data = securities_data.replace("securities", "").strip()
        securities_df = pd.read_csv(io.StringIO(securities_data), sep=";")

        # Обрабатываем вторую таблицу (marketdata)
        marketdata_data = parts[1].strip()
        marketdata_df = pd.read_csv(io.StringIO(marketdata_data), sep=";")

        # Объединение таблиц
        shares = pd.merge(securities_df, marketdata_df, on="SECID")

        logging.info("Данные по акциям успешно обработаны.")
        return shares

    except Exception as e:
        logging.error(f"Ошибка при загрузке или обработке данных по акциям: {e}")
        exit(1)
