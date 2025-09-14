import os
import asyncio
import logging
import gspread
import json
from aiogram import Bot, Dispatcher, types
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram import F
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from aiogram import BaseMiddleware
from typing import Callable, Dict, Any, Awaitable
import traceback

load_dotenv()
API_TOKEN = os.getenv("API_TOKEN")

# ссылки на экспертов
Expert_LINKS = {
    "Football": "https://t.me/assistantafrica",
    "Cybersport": "https://t.me/GS_Helps"
}

# Партнёрские ссылки на БК
BK_LINKS = {
    "Fonbet": "https://chibrick.github.io/Fonbet/",
    "1xbet": "https://chibrick.github.io/1xbet/",
    "Pari": "https://chibrick.github.io/Pari/"
}

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# инициализация
bot = Bot(
    token=API_TOKEN,
    default=DefaultBotProperties(parse_mode="HTML")
)
dp = Dispatcher()

# Устанавливаем часовой пояс МСК
MSK = timezone(timedelta(hours=3))

# Глобальные переменные для Google Sheets
client = None
sheet = None

async def init_google_sheets():
    """Инициализация Google Sheets"""
    global client, sheet
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive"
        ]

        creds_json = os.getenv("GOOGLE_CREDENTIALS")
        if not creds_json:
            logger.error("GOOGLE_CREDENTIALS не найдены в переменных окружения!")
            return False
            
        creds_dict = json.loads(creds_json)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        
        spreadsheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1HIrkvyNi0v_W3fW0Vbk4lmdspd1cwyCK4bSCwVuwIQQ/edit#gid=1138920267")
        sheet = spreadsheet.worksheet("Логи от бота")
        
        logger.info("Google Sheets успешно инициализированы!")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при инициализации Google Sheets: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

async def log_to_google_async(user: types.User, event_type: str, content: str):
    """Асинхронная функция для записи логов в Google Таблицу"""
    try:
        if not sheet:
            logger.error("Google Sheets не инициализированы!")
            return False
            
        now = datetime.now(MSK).strftime("%Y-%m-%d %H:%M:%S")
        
        # Выполняем синхронную операцию в отдельном потоке
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None, 
            sheet.append_row,
            [
                now,
                user.id,
                user.username or "",
                user.first_name or "",
                user.last_name or "",
                event_type,
                content
            ]
        )
        
        logger.info(f"Лог записан: {user.id} - {event_type} - {content[:50]}...")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при записи в Google Sheets: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

# === Middleware для логов ===
class LoggingMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[types.TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: types.TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        try:
            # Логируем событие
            if isinstance(event, types.Message):
                logger.info(f"Получено сообщение от {event.from_user.id}: {event.text}")
                asyncio.create_task(log_to_google_async(event.from_user, "MSG", event.text or ""))
                
            elif isinstance(event, types.CallbackQuery):
                logger.info(f"Получен callback от {event.from_user.id}: {event.data}")
                asyncio.create_task(log_to_google_async(event.from_user, "BTN", event.data or ""))
                
        except Exception as e:
            logger.error(f"Ошибка в LoggingMiddleware: {e}")
            
        # Продолжаем обработку события
        return await handler(event, data)

dp.update.middleware(LoggingMiddleware())

# === Шаг 1. Приветствие ===
@dp.message(Command("start"))
async def send_welcome(message: types.Message):
    logger.info(f"Команда /start от пользователя {message.from_user.id}")
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Забрать бонус", callback_data="bonus")],
        [InlineKeyboardButton(text="ℹ️ Почему мы это делаем?", callback_data="why_free")]
    ])
    await message.answer(
        "🔥 Добро пожаловать!\n\n"
        "Здесь ты получаешь:\n"
        "✅ Бесплатные бонусы от букмекеров\n"
        "✅ Прогнозы от экспертов, чтобы выигрывать чаще\n"
        "✅ Отзывы и отчеты, если еще не уверен\n\n"
        "Выбирай бонус и начинай зарабатывать уже сегодня ⬇️",
        reply_markup=keyboard
    )

# === Шаг 2. Почему бесплатно ===
@dp.callback_query(F.data == "why_free")
async def why_free(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Забрать бонус", callback_data="bonus")],
        [InlineKeyboardButton(text="⏭ Эксперт", callback_data="step_expert")]
    ])
    await callback.message.edit_text(
        "💡 Секрет простой:\n"
        "– Ты играешь и выигрываешь по нашим прогнозам\n"
        "– Мы договариваемся о матчах, но не можем ставить много, поэтому делимся и получаем бонус\n\n"
        "👉 Поэтому нам выгодно, чтобы ты выигрывал и оставался с нами 👍",
        reply_markup=keyboard
    )

# === Шаг 3. Регистрация в БК ===
@dp.callback_query(F.data.in_(["step_bk", "bonus"]))
async def step_bk(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
    	[InlineKeyboardButton(text="🔗 Fonbet - Бонус 1к", url=BK_LINKS["Fonbet"])],
    	[InlineKeyboardButton(text="🔗 1xBet - Бонус 2к", url=BK_LINKS["1xbet"])],
        [InlineKeyboardButton(text="🔗 Pari - Бонус 5к", url=BK_LINKS["Pari"])],
        [InlineKeyboardButton(text="⏭ Эксперты", callback_data="step_expert")]
    ])
    await callback.message.edit_text(
        "🚀 Переходи по ссылке, регистрируйся в БК и активируй бонус при пополнении.\n"
        "⚡ Вот несколько контор, которые ты можешь добавить.\n"
        "‼ Советуем распределить деньги по нескольким компаниям.\n\n"
        "⏭ Как закончишь, переходи к экспертам.",
        reply_markup=keyboard
    )

# === Шаг 4. Эксперт ===
@dp.callback_query(F.data == "step_expert")
async def step_expert(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Перейти к эксперту по футболу", url=Expert_LINKS["Football"])],
        [InlineKeyboardButton(text="📊 Перейти к эксперту по киберспорту", url=Expert_LINKS["Cybersport"])],
        [InlineKeyboardButton(text="🎁 Забрать бонус", callback_data="bonus")],
        [InlineKeyboardButton(text="ℹ️ Почему мы это делаем?", callback_data="why_free")],
        [InlineKeyboardButton(text="📌 Советы", callback_data="step_tips")]
    ])
    await callback.message.edit_text(
        "🎯 А вот и ссылка на экспертов.📊\n"
        "Перейди в канал и попроси доступ к прогнозам, чтобы получить сегодняшние рекомендации и первую стратегию.\n\n"
        "Иногда мы добавляем новые бонусы от букмекеров, поэтому ждем тебя снова.\n"
        "И не забудь прочитать советы перед тем как ставить.\n",
        reply_markup=keyboard
    )

# === Шаг 5. Советы ===
@dp.callback_query(F.data == "step_tips")
async def step_tips(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Понял", callback_data="step_expert")]
    ])
    await callback.message.edit_text(
        "⚠️ Перед тем как начать:\n"
        "1️. Ставь сначала маленькие суммы — важно привыкнуть к системе.\n"
        "2️. Для исключения блокировки аккаунта, иногда нужно специально проиграть ставку.\n"
        "3. Если вдруг что-то пойдёт не так — эксперт предложит компенсацию/совет, чтобы вернуть доверие\n\n"
        "👉 Твой плюс = наш плюс 💪",
        reply_markup=keyboard
    )

# Команда для тестирования логов
@dp.message(Command("test_log"))
async def test_log(message: types.Message):
    """Команда для тестирования записи логов"""
    result = await log_to_google_async(message.from_user, "TEST", "Тестирование логов")
    if result:
        await message.answer("✅ Лог успешно записан в Google Таблицу!")
    else:
        await message.answer("❌ Ошибка при записи лога в Google Таблицу!")

async def main():
    logger.info("Запуск бота...")
    
    # Инициализируем Google Sheets
    sheets_ok = await init_google_sheets()
    if not sheets_ok:
        logger.warning("Google Sheets не удалось инициализировать, логи не будут записываться!")
    
    logger.info("Начинаем polling...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
