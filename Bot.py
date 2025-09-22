import os
import asyncio
import logging
import gspread
import json
from aiogram import Bot, Dispatcher, types
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove
)
from aiogram import F
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from oauth2client.service_account import ServiceAccountCredentials
import traceback
from aiohttp import web

load_dotenv()
API_TOKEN = os.getenv("API_TOKEN")

# Часовой пояс
MSK = timezone(timedelta(hours=3))

# Логи
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Бот и диспетчер
bot = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

# Google Sheets
client = None
sheet_clients = None
sheet_logs = None

async def init_google_sheets():
    """Инициализация Google Sheets"""
    global client, sheet_clients, sheet_logs
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive"
        ]
        creds_json = os.getenv("GOOGLE_CREDENTIALS")
        creds_dict = json.loads(creds_json)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)

        spreadsheet = client.open_by_url(
            "https://docs.google.com/spreadsheets/d/1pGc9kdpdFggwZlc3wairUBunou1BW_fw-D-heBViHic/edit#gid=0"
        )
        sheet_clients = spreadsheet.worksheet("Клиенты - Партнерки")
        sheet_logs = spreadsheet.worksheet("Логи от бота")

        logger.info("Google Sheets успешно инициализированы!")
        return True
    except Exception as e:
        logger.error(f"Ошибка при инициализации Google Sheets: {e}")
        return False


async def update_client(user: types.User, phone="", location="", offer="", status=""):
    """Добавить или обновить клиента в таблице"""
    if not sheet_clients:
        return False
    try:
        loop = asyncio.get_event_loop()
        all_values = await loop.run_in_executor(None, sheet_clients.get_all_values)

        user_id = str(user.id)
        row_index = None
        for i, row in enumerate(all_values, start=1):
            if row and row[0] == user_id:
                row_index = i
                break

        if row_index:
            # обновляем
            if phone:
                sheet_clients.update_cell(row_index, 5, phone)
            if location:
                sheet_clients.update_cell(row_index, 6, location)
            if offer:
                prev = all_values[row_index-1][6] if len(all_values[row_index-1]) > 6 else ""
                new_offers = prev + ";" + offer if prev else offer
                sheet_clients.update_cell(row_index, 7, new_offers)
            if status:
                sheet_clients.update_cell(row_index, 8, status)
            sheet_clients.update_cell(row_index, 10, datetime.now(MSK).strftime("%Y-%m-%d %H:%M:%S"))
        else:
            # новый клиент
            row_data = [
                user_id,
                user.username or "",
                user.first_name or "",
                user.last_name or "",
                phone,
                location,
                offer,
                status or "новый",
                datetime.now(MSK).strftime("%Y-%m-%d %H:%M:%S"),
                "Регистрация"
            ]
            sheet_clients.append_row(row_data, value_input_option="USER_ENTERED")
        return True
    except Exception as e:
        logger.error(f"Ошибка при обновлении клиента: {e}")
        return False


async def log_event(user: types.User, event_type: str, content: str):
    """Логируем действие"""
    if not sheet_logs:
        return False
    try:
        now = datetime.now(MSK).strftime("%Y-%m-%d %H:%M:%S")
        row_data = [
            now,
            str(user.id),
            user.username or "",
            user.first_name or "",
            user.last_name or "",
            event_type,
            content[:200]
        ]
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: sheet_logs.append_row(row_data, value_input_option="USER_ENTERED"))
        return True
    except Exception as e:
        logger.error(f"Ошибка при записи лога: {e}")
        return False

# === Старт ===
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    ref = "без_метки"
    if len(message.text.split()) > 1:
        ref = message.text.split()[1]

    await update_client(message.from_user, status="новый")
    await log_event(message.from_user, "START", ref)

    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Отправить телефон", request_contact=True)]],
        resize_keyboard=True
    )
    await message.answer(
        "🔥 Добро пожаловать!\n\nДля продолжения отправь свой номер телефона:",
        reply_markup=kb
    )

# === Получение телефона ===
@dp.message(F.contact)
async def get_phone(message: types.Message):
    phone = message.contact.phone_number
    await update_client(message.from_user, phone=phone)
    await log_event(message.from_user, "PHONE", phone)

    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📍 Отправить геолокацию", request_location=True)]],
        resize_keyboard=True
    )
    await message.answer("✅ Спасибо! Теперь отправь геолокацию:", reply_markup=kb)

# === Получение геолокации ===
@dp.message(F.location)
async def get_location(message: types.Message):
    loc = f"{message.location.latitude},{message.location.longitude}"
    await update_client(message.from_user, location=loc)
    await log_event(message.from_user, "LOCATION", loc)

    await message.answer("✅ Отлично! Данные сохранены.", reply_markup=ReplyKeyboardRemove())

    # Главное меню
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Забрать бонус", callback_data="bonus")],
        [InlineKeyboardButton(text="ℹ️ Почему мы это делаем?", callback_data="why_free")]
    ])
    await message.answer(
        "Теперь ты в системе! Выбирай бонусы и офферы 👇",
        reply_markup=keyboard
    )

# === Пример кнопок офферов ===
@dp.callback_query(F.data == "bonus")
async def step_bonus(callback: types.CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Дебетовая карта", callback_data="offer_debit")],
        [InlineKeyboardButton(text="💳 Кредитная карта", callback_data="offer_credit")],
        [InlineKeyboardButton(text="🎲 Регистрация в БК", callback_data="offer_bk")]
    ])
    await callback.message.answer("Выбери категорию оффера:", reply_markup=kb)
    await log_event(callback.from_user, "BTN", "Выбор категории")

# === Заглушки для офферов ===
@dp.callback_query(F.data.startswith("offer_"))
async def choose_offer(callback: types.CallbackQuery):
    cat = callback.data.split("_", 1)[1]
    await update_client(callback.from_user, offer=cat, status="оффер выбран")
    await log_event(callback.from_user, "OFFER", cat)

    await callback.message.answer(f"✅ Ты выбрал категорию: {cat}. Дальше я выдам список офферов этой категории.")
    await callback.answer()

# === Healthcheck для Render ===
async def handle(request):
    return web.Response(text="I'm alive!")

async def start_web_server():
    port = int(os.environ.get("PORT", 10000))
    app = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logging.info(f"Веб-сервер запущен на порту {port}")

# === Main ===
async def main():
    logger.info("Запуск бота...")
    await init_google_sheets()
    asyncio.create_task(start_web_server())
    await dp.start_polling(bot, drop_pending_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
