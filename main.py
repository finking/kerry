import asyncio
import logging
import os
import nest_asyncio
from telegram_bot.bot import run_telegram_bot

from dotenv import load_dotenv

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
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()
# Получаем токен
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')

if not TELEGRAM_BOT_TOKEN:
    logger.error("Токен Telegram не найден в .env")
    raise ValueError("Токен Telegram не задан")


if __name__ == "__main__":
    nest_asyncio.apply()
    asyncio.run(run_telegram_bot(TELEGRAM_BOT_TOKEN))
