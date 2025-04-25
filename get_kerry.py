import os
import pandas as pd
import requests
from datetime import datetime
import io
import sqlite3
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s]: %(message)s",
    datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
    handlers=[
        logging.FileHandler("data/log.log"),  # Логи будут записываться в файл app.log
        logging.StreamHandler()  # Логи также будут выводиться в консоль
    ]
)


COLUMNS_SEC_FUTURES = "SECID,SHORTNAME,LASTDELDATE,SECTYPE,ASSETCODE,PREVOPENPOSITION,LOTVOLUME,INITIALMARGIN,TIME"
COLUMNS_MD_FUTURES = "SECID,SPREAD,LAST,OPENPOSITION,NUMTRADES,TIME"
COLUMNS_SEC_SHARES = "SECID,SHORTNAME,LOTSIZE"
COLUMNS_MD_SHARES = "SECID,BID,OFFER,SPREAD,LAST,TIME,SYSTIME"

# Словарь для хранения "неправильного" ASSETCODE
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
        
        # 1. Загрузка настроек из файла secid.txt
        if not os.path.exists("secid.txt"):
            raise FileNotFoundError("Файл secid.txt не найден.")
        with open("secid.txt", "r") as file:
            secids = file.read().strip().split(",")
        if not secids or all(not secid.strip() for secid in secids):
            raise ValueError("Файл secid.txt пуст или содержит некорректные данные.")
        
        logging.info(f"Список SECID из secid.txt: {secids}")
        
        # Формирование URL для фьючерсов
        url_futures = (
            f"https://iss.moex.com/iss/engines/futures/markets/forts/boards/rfud/securities.csv"
            f"?securities={','.join(secids)}"
            "&iss.only=securities,marketdata"
            f"&securities.columns={COLUMNS_SEC_FUTURES}"
            f"&marketdata.columns={COLUMNS_MD_FUTURES}"
        )
        
        # 2. Загрузка данных по фьючерсам
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
        
        # 3. Объединение таблиц
        futures = pd.merge(securities_df, marketdata_df, on="SECID")
        
        # Заменяем значения ASSETCODE
        for old, new in replacements.items():
            futures["ASSETCODE"] = futures["ASSETCODE"].replace(old, new)
        
        # Добавляем столбец с текущей датой
        current_date = datetime.now().strftime("%d.%m.%Y")  # Формат: ДД.ММ.ГГГГ
        futures["Date"] = current_date
        
        logging.info("Данные по фьючерсам успешно обработаны.")
        return futures
    
    except Exception as e:
        logging.error(f"Произошла ошибка на этапе загрузки данных по фьючерсам: {e}")
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
        logging.error(f"Произошла ошибка на этапе сохранения данных по фьючерсам: {e}")
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
        logging.error(f"Произошла ошибка на этапе загрузки данных по акциям: {e}")
        exit(1)


def calculate_total(futures, shares):
    """Формирование DataFrame total."""
    try:
        logging.info("Начало формирования DataFrame total.")
        
        # Текущая дата
        today_f = datetime.now()
        
        # Формирование датафрейма total
        futures_subset = futures[
            ["ASSETCODE", "SHORTNAME", "LAST", "LOTVOLUME", "LASTDELDATE", "TIME"]
        ]
        shares_subset = shares[
            ["SECID", "SHORTNAME", "LAST", "TIME"]
        ]
        total = pd.merge(
            futures_subset,
            shares_subset,
            left_on="ASSETCODE",
            right_on="SECID",
            suffixes=("_futures", "_shares"),
        )
        
        # Преобразуем LASTDELDATE в datetime
        total["LASTDELDATE"] = pd.to_datetime(total["LASTDELDATE"])
        total["LAST_shares"] = pd.to_numeric(total["LAST_shares"], errors="coerce")
        total["days_to_expiry"] = (total["LASTDELDATE"] - today_f).dt.days + 1  # Разница в днях
        
        # Вычисляем kerry и kerry_year
        total["kerry"] = (
                (total["LAST_futures"] - total["LAST_shares"] * total["LOTVOLUME"])
                / (total["LAST_shares"] * total["LOTVOLUME"])
                * 100
        )
        total["kerry_year"] = total["kerry"] / total["days_to_expiry"] * 365
        
        # Округляем kerry, kerry_year до второго знака после запятой
        total["kerry"] = total["kerry"].round(2)
        total["kerry_year"] = total["kerry_year"].round(2)
        
        # Сохранение total
        os.makedirs("data/total", exist_ok=True)
        today = datetime.now().strftime("%d-%m-%y")
        total.to_csv(f"data/total/total_{today}.csv", index=False)
        
        conn = sqlite3.connect("data/spread.db")
        total.to_sql("total", conn, if_exists="append", index=False)
        conn.close()
        
        logging.info("DataFrame total успешно сформирован и сохранен.")
        return total
    
    except Exception as e:
        logging.error(f"Произошла ошибка на этапе формирования total: {e}")
        exit(1)


def calculate_spread(total):
    """Формирование DataFrame spread."""
    try:
        logging.info("Начало формирования spread.")
        # Текущая дата
        today_f = datetime.now()
        # Вычисление kerry_spread и kerry_spread_y
        spread_data = []
        for assetcode, group in total.groupby("ASSETCODE"):
            if len(group) > 1:  # Проверяем, что в группе больше одного фьючерса
                sorted_group = group.sort_values(by="LASTDELDATE")  # Сортируем по дате экспирации
                for i in range(len(sorted_group) - 1):
                    # Формируем Name_spread
                    name_spread = f"{sorted_group.iloc[i]['SHORTNAME_futures']}-{sorted_group.iloc[i + 1]['SHORTNAME_futures']}"
                    
                    # Проверка условий для знаменателя
                    last_shares_zero = sorted_group.iloc[i]["LAST_shares"] == 0
                    last_futures_zero = sorted_group.iloc[i]["LAST_futures"] == 0
                    next_last_futures_zero = sorted_group.iloc[i + 1]["LAST_futures"] == 0
                    
                    if last_shares_zero or last_futures_zero or next_last_futures_zero:
                        # Логирование пропуска строки
                        if last_shares_zero:
                            logging.warning(
                                f"Пропущена строка для {name_spread}: не было сделок по базовому активу."
                            )
                        if last_futures_zero:
                            logging.warning(
                                f"Пропущена строка для {name_spread}: не было сделок по ближнему фчс."
                            )
                        if next_last_futures_zero:
                            logging.warning(
                                f"Пропущена строка для {name_spread}: не было сделок по дальнему фчс."
                            )
                        continue  # Пропускаем вычисления
                    
                    # Вычисляем kerry_spread
                    kerry_spread = (
                            (sorted_group.iloc[i + 1]["LAST_futures"] - sorted_group.iloc[i]["LAST_futures"])
                            / (sorted_group.iloc[i]["LAST_shares"] * sorted_group.iloc[i]["LOTVOLUME"])
                            * 100
                    )
                    
                    # Вычисляем kerry_spread_y
                    last_trade_date = pd.to_datetime(sorted_group.iloc[i + 1]["LASTDELDATE"])
                    days_to_expiry = (last_trade_date - today_f).days + 1  # Разница в днях
                    if days_to_expiry > 0:
                        kerry_spread_y = kerry_spread / days_to_expiry * 365
                    else:
                        kerry_spread_y = None
                        # Логирование пропуска kerry_spread_y
                        logging.warning(
                            f"Пропущено вычисление kerry_spread_y для {name_spread}: "
                            f"days_to_expiry <= 0."
                        )
                    
                   
                    
                    # Добавляем данные в список
                    spread_data.append(
                        {
                            "Name_spread": name_spread,
                            "kerry_spread": kerry_spread,
                            "kerry_spread_y": kerry_spread_y,
                        }
                    )
        
        # Создаем DataFrame spread
        spread = pd.DataFrame(spread_data)
        # Округляем kerry_spread, kerry_spread_y до второго знака после запятой
        spread["kerry_spread"] = spread["kerry_spread"].round(2)
        spread["kerry_spread_y"] = spread["kerry_spread_y"].round(2)
        
        # Сохранение spread
        os.makedirs("data/spread", exist_ok=True)
        today = datetime.now().strftime("%d-%m-%y")
        spread.to_csv(f"data/spread/spread_{today}.csv", index=False)
        conn = sqlite3.connect("data/spread.db")
        spread.to_sql("spread", conn, if_exists="append", index=False)
        conn.close()
        
        return spread
    
    except Exception as e:
        logging.error(f"Произошла ошибка на этапе формирования spread: {e}")
        exit(1)


def print_top_positions(total, spread):
    """Вывод топ-5 позиций из total и spread."""
    try:
        # Топ-5 позиций из total
        top_total = total.nlargest(5, "kerry_year")
        logging.info("Топ-5 позиций из total по kerry_year:")
        logging.info(top_total)
        
        # Топ-5 позиций из spread
        top_spread = spread.nlargest(5, "kerry_spread_y")
        logging.info("Топ-5 позиций из spread по Kerry_spread_y:")
        logging.info(top_spread)
        
        # Последние 5 позиций из spread
        last_spread = spread.sort_values(by="kerry_spread_y").head(5)
        logging.info("Последние 5 позиций из spread по kerry_spread_y:")
        logging.info(last_spread)
    
    except Exception as e:
        logging.error(f"Произошла ошибка на этапе вывода топ-позиций: {e}")
        exit(1)


if __name__ == "__main__":
    logging.info("Запуск программы.")
    
    # Загрузка данных по фьючерсам
    futures = load_futures_data()
    
    # Сохранение данных по фьючерсам
    save_futures_to_db_and_csv(futures)
    
    # Формирование множества ASSETCODE
    set_asset = set(futures["ASSETCODE"])
    
    # Загрузка данных по акциям
    shares = load_shares_data(set_asset)
    
    # Формирование DataFrame total
    total = calculate_total(futures, shares)
    
    # Формирование DataFrame spread
    spread = calculate_spread(total)
    
    # Вывод топ-5 позиций
    print_top_positions(total, spread)
    
    logging.info("Программа завершена.")