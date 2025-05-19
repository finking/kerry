import logging
import os
import pandas as pd
import sqlite3
from datetime import datetime


def calculate_total(futures, shares):
    """Формирование DataFrame total."""
    try:
        logging.info("Начало формирования DataFrame total.")
        
        # Получаем SYSTIME из futures (берем любое значение — оно одинаковое для всех строк)
        systime_str = futures.iloc[0]["SYSTIME"]
        today_f = pd.to_datetime(systime_str)  # Используем дату из SYSTIME
        
        # Формирование датафрейма total
        futures_subset = futures[
            ["SYSTIME", "ASSETCODE", "SHORTNAME", "LAST", "LOTVOLUME", "LASTDELDATE", "TIME"]
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
        
        # Сохранение total в csv и sql
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
        
        # Получаем текущую дату из SYSTIME
        systime_str = total.iloc[0]["SYSTIME"] if not total.empty else None
        today_f = pd.to_datetime(systime_str) if systime_str else datetime.now()
        
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
                            "System_date": systime_str,
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
