import aiocron
import asyncio
import logging
from telegram import Update, Bot
from telegram.constants import ParseMode
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

user_states = {}


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    user_states[user_id] = True
    await update.message.reply_text("Вы успешно подписаны на уведомления.")


async def send_message_to_active_users(bot: Bot, message: str):
    """Отправка сообщения всем подписчикам"""
    for user_id in user_states:
        try:
            await bot.send_message(
                chat_id=user_id,
                text=message,
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logging.error(f"Не удалось отправить сообщение пользователю {user_id}: {e}")


def schedule_tasks(application):
    @aiocron.crontab("34 11,16,23 * * MON-FRI")  # 11:00, 17:00, 23:00 по будням
    async def scheduled_task():
        logging.info("Запущена фоновая задача по расписанию")
        from core.data_loader import load_futures_data, load_shares_data
        from core.data_processor import calculate_total, calculate_spread

        try:
            logging.info("Выполняется обновление данных...")
    
            futures = load_futures_data()
            set_asset = set(futures["ASSETCODE"])
            shares = load_shares_data(set_asset)
    
            total = calculate_total(futures, shares)
            spread = calculate_spread(total)

            # Формируем сообщения
            # Топ-5 позиций из total
            top_total = total.nlargest(5, "kerry_year")
            message_total = format_df_for_telegram(top_total, "📊 Топ-5 по Кэрри, % год:")
            await send_message_to_active_users(application.bot, message_total)
           
            # Топ-5 позиций из spread
            top_spread = spread.nlargest(5, "kerry_spread_y")
            message_spread = format_df_for_telegram_spread(top_spread, "📈 Топ-5 по Кэрри спреда, % год:")
            await send_message_to_active_users(application.bot, message_spread)

            logging.info("Сообщения отправлены по расписанию.")

        except Exception as e:
            logging.error(f"Ошибка при выполнении фоновой задачи: {e}")

        logging.info("Фоновая задача по расписанию добавлена")


# Форматирование для фчс
def format_df_for_telegram(df, title=""):
    if df.empty:
        return f"<b>{title}</b>\nНет данных для отображения."
    
    lines = [f"<b>{title}</b>"]
    for _, row in df.iterrows():
        line = (
            f"• <b>{row['SHORTNAME_futures']}:</b>\n"
            f"      Базовый актив: {row['SHORTNAME_shares']}\n"
            f"      Кэрри, % год: {row['kerry_year']}\n"
            f"      Кэрри, %: {row['kerry']}\n"
            f"      Последняя цена фьючерса: {row['LAST_futures']}\n"
            f"      Последняя цена акции: {row['LAST_shares']}\n"
            f"      Кол-во лотов в фчс: {row['LOTVOLUME']}\n"
            f"      Дней до истечения: {row['days_to_expiry']}\n"
        )
        lines.append(line)
    return "\n".join(lines)


# Форматирование для спреда фчс
def format_df_for_telegram_spread(df, title=""):
    if df.empty:
        return f"<b>{title}</b>\nНет данных для отображения."
    
    lines = [f"<b>{title}</b>"]
    for _, row in df.iterrows():
        line = (
            f"• <b> {row['Name_spread']}:</b>\n"
            f"      Кэрри, % год: {row['kerry_spread_y']}\n"
            f"      Кэрри, %: {row['kerry_spread']}\n"

        )
        lines.append(line)
    return "\n".join(lines)


async def run_telegram_bot(TOKEN):

    loop = asyncio.get_event_loop()
    application = ApplicationBuilder().token(TOKEN).build()
    application.add_handler(CommandHandler("start", start))

    # Добавляем задачу по расписанию
    schedule_tasks(application)
    logging.info("Бот запущен. Ожидание команды /start или запуск по расписанию...")
    loop.run_until_complete(application.run_polling())