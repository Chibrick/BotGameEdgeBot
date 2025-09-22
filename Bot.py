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
sheet_clients = None   # "Клиенты - Партнерки"
sheet_logs = None      # "Логи от бота"

# Константы кол-во колонок и индексы (A..Q)
NUM_COLUMNS = 17
IDX_CLIENT_NO = 1   # A
IDX_USER_ID = 2     # B
IDX_USERNAME = 3    # C
IDX_FIRST_NAME = 4  # D
IDX_PHONE = 5       # E
IDX_LOCATION = 6    # F
IDX_MARK = 7        # G
IDX_OFFER_NO = 8    # H
# I..O = 9..15 (чекбоксы офферов)
IDX_STATUS = 16     # P
IDX_DATE = 17       # Q

SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1pGc9kdpdFggwZlc3wairUBunou1BW_fw-D-heBViHic/edit#gid=0"


async def run_in_executor(fn, *args, **kwargs):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))


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
        if not creds_json:
            logger.error("GOOGLE_CREDENTIALS не найдены в переменных окружения!")
            return False
        creds_dict = json.loads(creds_json)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)

        spreadsheet = await run_in_executor(client.open_by_url, SPREADSHEET_URL)
        sheet_clients = spreadsheet.worksheet("Клиенты - Партнерки")
        sheet_logs = spreadsheet.worksheet("Логи от бота")

        logger.info("Google Sheets успешно инициализированы!")
        return True
    except Exception as e:
        logger.error(f"Ошибка при инициализации Google Sheets: {e}")
        logger.error(traceback.format_exc())
        return False


def _pad_row(row, length):
    """Pad list to exact length"""
    if row is None:
        row = []
    row = list(row)
    if len(row) < length:
        row += [""] * (length - len(row))
    else:
        row = row[:length]
    return row


async def find_user_row_by_id(user_id: str):
    """Ищет строку (номер) по user_id в колонке B. Возвращает None если не найдено"""
    if not sheet_clients:
        return None
    try:
        col_vals = await run_in_executor(sheet_clients.col_values, IDX_USER_ID)
        # col_values returns list with header as first element usually
        for i, v in enumerate(col_vals, start=1):
            if v == user_id:
                return i
        return None
    except Exception as e:
        logger.error(f"find_user_row_by_id error: {e}")
        return None


async def update_client(user: types.User, phone="", location="", offer="", status="", mark="", offer_no=""):
    """
    Добавляет или полностью обновляет строку клиента (A..Q).
    Если запись есть — загружаем её, обновляем поля и перезаписываем весь диапазон A{row}:Q{row}.
    Если нет — добавляем новую строку в A{next_row}:Q{next_row}.
    """
    if not sheet_clients:
        logger.error("sheet_clients не инициализирован")
        return False
    try:
        user_id = str(user.id)
        row_index = await find_user_row_by_id(user_id)

        if row_index:
            # обновление существующей строки
            row_vals = await run_in_executor(sheet_clients.row_values, row_index)
            row_vals = _pad_row(row_vals, NUM_COLUMNS)

            # Обновляем поля (только если переданы)
            if user.username:
                row_vals[IDX_USERNAME - 1] = user.username
            if user.first_name:
                row_vals[IDX_FIRST_NAME - 1] = user.first_name
            if phone:
                row_vals[IDX_PHONE - 1] = phone
            if location:
                row_vals[IDX_LOCATION - 1] = location
            if mark:
                row_vals[IDX_MARK - 1] = mark
            if offer_no:
                row_vals[IDX_OFFER_NO - 1] = offer_no
            if offer:
                # добавляем оффер в H (№ оффера) и/или можно ставить галочки I..O
                # проще: если H пуст — пишем offer, иначе оставляем (или перезаписываем)
                if not row_vals[IDX_OFFER_NO - 1]:
                    row_vals[IDX_OFFER_NO - 1] = offer
                else:
                    # также можно дописать в H через ;
                    if offer not in row_vals[IDX_OFFER_NO - 1]:
                        row_vals[IDX_OFFER_NO - 1] = f"{row_vals[IDX_OFFER_NO - 1]};{offer}"

            if status:
                row_vals[IDX_STATUS - 1] = status

            row_vals[IDX_DATE - 1] = datetime.now(MSK).strftime("%Y-%m-%d %H:%M:%S")

            range_name = f"A{row_index}:Q{row_index}"
            await run_in_executor(sheet_clients.update, range_name, [row_vals], {'valueInputOption': 'USER_ENTERED'})
            logger.info(f"Обновлена строка {row_index} для user {user_id}")
        else:
            # новая запись
            all_values = await run_in_executor(sheet_clients.get_all_values)
            next_row = len(all_values) + 1  # next available row index

            client_no = next_row - 1  # первый data row will be 1 if header exists
            new_row = [""] * NUM_COLUMNS
            new_row[IDX_CLIENT_NO - 1] = str(client_no)
            new_row[IDX_USER_ID - 1] = user_id
            new_row[IDX_USERNAME - 1] = user.username or ""
            new_row[IDX_FIRST_NAME - 1] = user.first_name or ""
            new_row[IDX_PHONE - 1] = phone or ""
            new_row[IDX_LOCATION - 1] = location or ""
            new_row[IDX_MARK - 1] = mark or ""
            new_row[IDX_OFFER_NO - 1] = offer_no or offer or ""
            # I..O checkboxes left empty
            new_row[IDX_STATUS - 1] = status or "новый"
            new_row[IDX_DATE - 1] = datetime.now(MSK).strftime("%Y-%m-%d %H:%M:%S")

            range_name = f"A{next_row}:Q{next_row}"
            await run_in_executor(sheet_clients.update, range_name, [new_row], {'valueInputOption': 'USER_ENTERED'})
            logger.info(f"Добавлена новая строка {next_row} для user {user_id}")

        return True
    except Exception as e:
        logger.error(f"Ошибка при update_client: {e}")
        logger.error(traceback.format_exc())
        return False


async def log_event(user: types.User, event_type: str, content: str):
    """Запись в лист 'Логи от бота'"""
    if not sheet_logs:
        logger.error("❌ sheet_logs не инициализирован")
        return False
    try:
        now = datetime.now(MSK).strftime("%Y-%m-%d %H:%M:%S")
        row = [
            now,
            str(user.id),
            user.username or "",
            user.first_name or "",
            user.last_name or "",
            event_type,
            content[:300]
        ]
        await run_in_executor(lambda: sheet_logs.append_row(row, value_input_option="USER_ENTERED"))
        logger.info(f"✅ Лог добавлен: {event_type} для {user.id}")
        return True
    except Exception as e:
        logger.error(f"Ошибка log_event: {e}")
        logger.error(traceback.format_exc())
        return False


# === Handlers ===

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    ref = "без_метки"
    if len(message.text.split()) > 1:
        ref = message.text.split()[1]

    # Регистрируем/обновляем клиента и пишем лог
    await update_client(message.from_user, status="новый", mark=ref)
    await log_event(message.from_user, "START", ref)

    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Отправить телефон", request_contact=True)]],
        resize_keyboard=True
    )
    await message.answer(
        "🔥 Добро пожаловать!\n\nДля продолжения отправь свой номер телефона:",
        reply_markup=kb
    )


@dp.message(F.contact)
async def get_phone(message: types.Message):
    try:
        phone = message.contact.phone_number
    except Exception:
        phone = message.text.strip()
    await update_client(message.from_user, phone=phone)
    await log_event(message.from_user, "PHONE", phone)

    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📍 Отправить геолокацию", request_location=True)]],
        resize_keyboard=True
    )
    await message.answer("✅ Спасибо! Теперь отправь геолокацию:", reply_markup=kb)


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


@dp.callback_query(F.data == "why_free")
async def why_free(callback: types.CallbackQuery):
    await log_event(callback.from_user, "BTN", "why_free")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Забрать бонус", callback_data="bonus")],
        [InlineKeyboardButton(text="⏭ Эксперт", callback_data="step_expert")]
    ])
    await callback.message.edit_text(
        "💡 Секрет простой:\n"
        "– Ты играешь и выигрываешь по прогнозам.\n"
        "– Эксперт даёт прогноз и получает бонус.\n\n"
        "👉 Поэтому нам выгодно, чтобы ты выигрывал и оставался с нами 👍",
        reply_markup=kb
    )


@dp.callback_query(F.data.in_(["step_bk", "bonus"]))
async def step_bk(callback: types.CallbackQuery):
    await log_event(callback.from_user, "BTN", "step_bk/bonus")
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Fonbet - Бонус 1к", callback_data="bk_Fonbet")],
        [InlineKeyboardButton(text="🔗 1xBet - Бонус 2к", callback_data="bk_1xbet")],
        [InlineKeyboardButton(text="🔗 Pari - Бонус 5к", callback_data="bk_Pari")],
        [InlineKeyboardButton(text="⏭ Эксперты", callback_data="step_expert")]
    ])
    await callback.message.edit_text(
        "🚀 Переходи по ссылке, регистрируйся в БК и активируй бонус при пополнении.\n"
        "⚡ Вот несколько контор, которые ты можешь добавить.\n"
        "‼ Советуем распределить деньги по нескольким компаниям.\n\n"
        "⏭ Как закончишь, переходи к экспертам.",
        reply_markup=keyboard
    )


@dp.callback_query(F.data.startswith("bk_"))
async def on_bk_click(callback: types.CallbackQuery):
    bk_name = callback.data.split("_", 1)[1]
    await update_client(callback.from_user, offer=bk_name, status="взял оффер БК")
    await log_event(callback.from_user, "BK_CLICK", bk_name)

    # здесь вместо callback.message.edit_text - отправляем приватное сообщение с ссылкой
    # (так пользователь видит ссылку внизу, а не сообщение edit)
    await callback.message.answer(
        f"🔗 <b>{bk_name}</b> — вот твоя ссылка для получения бонуса:\nhttps://example.com/{bk_name}",
        parse_mode="HTML"
    )
    await callback.answer()


@dp.callback_query(F.data == "bonus")
async def show_categories(callback: types.CallbackQuery):
    await log_event(callback.from_user, "BTN", "bonus_menu")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Дебетовая карта", callback_data="offer_debit")],
        [InlineKeyboardButton(text="💳 Кредитная карта", callback_data="offer_credit")],
        [InlineKeyboardButton(text="🎲 Регистрация в БК", callback_data="offer_bk")]
    ])
    await callback.message.answer("Выбери категорию оффера:", reply_markup=kb)
    await callback.answer()


@dp.callback_query(F.data.startswith("offer_"))
async def choose_offer(callback: types.CallbackQuery):
    cat = callback.data.split("_", 1)[1]
    # В H мы запишем название категории/оффера
    await update_client(callback.from_user, offer=cat, status="оффер выбран")
    await log_event(callback.from_user, "OFFER", cat)
    await callback.message.answer(f"✅ Ты выбрал категорию: {cat}. Дальше я выдам список офферов этой категории.")
    await callback.answer()

@dp.message(Command("force_reset"))
async def force_reset(message: types.Message):
    """Принудительный сброс webhook и очистка апдейтов"""
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await bot.get_me()  # Проверяем соединение
        await message.answer("✅ Webhook сброшен, все апдейты очищены!")
        logger.info(f"Принудительный сброс выполнен пользователем {message.from_user.id}")
    except Exception as e:
        await message.answer(f"❌ Ошибка при сбросе: {e}")
        logger.error(f"Ошибка при принудительном сбросе: {e}")

@dp.message(Command("test_log"))
async def test_log(message: types.Message):
    ok = await log_event(message.from_user, "TEST", "Проверка записи в логи")
    if ok:
        await message.answer("✅ Лог записан")
    else:
        await message.answer("❌ Ошибка при записи лога")

# Healthcheck для Render
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

# Main
async def main():
    logger.info("Запуск бота...")
    ok = await init_google_sheets()
    if not ok:
        logger.error("Не удалось инициализировать Google Sheets. Бот будет работать, но без записи.")
    # старт веб-сервера для Render healthcheck
    asyncio.create_task(start_web_server())
    await dp.start_polling(bot, drop_pending_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
