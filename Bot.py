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
from aiohttp import web

load_dotenv()
API_TOKEN = os.getenv("API_TOKEN")

user_choices = {}  # { user_id: {"bk": "", "expert": ""} }

# ссылки на экспертов
EXPERTS = {
    "Football_Africa": {
        "name": "Эксперт",
        "link": "https://dzen.ru/?yredirect=true"
    },
    # "Cybersport_Gamesport": {
    #     "name": "Аналитика по киберспорту",
    #     "link": "https://t.me/GS_Helps"
    # },
    # Можно добавлять новых экспертов так:
    # "Tennis_Pro": {"name": "теннису", "link": "https://t.me/tennis_channel"}
}

# Партнёрские ссылки на БК
BK_LINKS = {
    "1": "https://dzen.ru/?yredirect=true",
    "2": "https://dzen.ru/?yredirect=true",
    "3": "https://dzen.ru/?yredirect=true"
}

# Добавляем новые типы событий
EVENT_TYPES = {
    "MSG": "Сообщение",
    "BTN": "Кнопка", 
    "BK_CLICK": "Клик по БК",
    "EXPERT_CLICK": "Клик по эксперту",
    "PHONE_REQUEST": "Запрос телефона",
    "LOCATION_REQUEST": "Запрос геолокации"
}

# Глобальный словарь для отслеживания кликов (в памяти)
click_tracking = {}

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

def log_to_sheets(row_data: list):
    """Записывает данные в таблицу"""
    try:
        # Находим первую пустую строку
        last_row = len(sheet.get_all_values()) + 1
        # Записываем данные
        sheet.update(
            range_name=f"A{last_row}",
            values=[row_data],
            value_input_option="USER_ENTERED"
        )
        logger.info(f"Лог записан в строку {last_row}: {row_data}")
    except Exception as e:
        logger.error(f"Ошибка при записи в таблицу: {e}")

async def log_to_google_async(user: types.User, event_type: str, content: str):
    """Асинхронная функция для записи логов в Google Таблицу"""
    try:
        if not sheet:
            logger.error("Google Sheets не инициализированы!")
            return False
            
        now = datetime.now(MSK).strftime("%Y-%m-%d %H:%M:%S")
        
        # Получаем следующую пустую строку
        loop = asyncio.get_event_loop()
        all_values = await loop.run_in_executor(None, sheet.get_all_values)
        next_row = len(all_values) + 1
        
        # Данные для записи
        row_data = [
            now,                   # A
            str(user.id),          # B  
            user.username or "",   # C
            user.first_name or "", # D
            user.last_name or "",  # E
            event_type,            # F
            content[:100],         # G
            "",                    # H phone
            ""                     # I location
        ]
        
        # Записываем точно в нужные ячейки
        range_name = f"A{next_row}:I{next_row}"
        
        # Правильный способ: создаем функцию-обертку
        def update_sheet():
            sheet.update(range_name, [row_data], value_input_option='USER_ENTERED')
        
        await loop.run_in_executor(None, update_sheet)
        
        logger.info(f"Лог записан в строку {next_row}: {user.id} - {event_type}")
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
        # Сначала обрабатываем событие
        result = await handler(event, data)
        
        # Потом логируем (чтобы не блокировать обработку)
        try:
            if isinstance(event, types.Message):
                logger.info(f"Получено сообщение от {event.from_user.id}: {event.text}")
                # Ждем завершения записи лога
                await log_to_google_async(event.from_user, "MSG", event.text or "")
                
            elif isinstance(event, types.CallbackQuery):
                logger.info(f"Получен callback от {event.from_user.id}: {event.data}")
                # Ждем завершения записи лога
                await log_to_google_async(event.from_user, "BTN", event.data or "")
                
        except Exception as e:
            logger.error(f"Ошибка в LoggingMiddleware: {e}")
            
        return result

dp.message.middleware(LoggingMiddleware())
dp.callback_query.middleware(LoggingMiddleware())

# Функция для безопасного редактирования сообщений
async def safe_edit_message(callback: types.CallbackQuery, text: str, reply_markup=None):
    """Безопасное редактирование сообщения с обработкой ошибки дублирования"""
    try:
        await callback.message.edit_text(text, reply_markup=reply_markup)
    except Exception as e:
        if "message is not modified" in str(e):
            # Просто отвечаем на callback, чтобы убрать "часики"
            await callback.answer()
        else:
            logger.error(f"Ошибка при редактировании сообщения: {e}")
            await callback.answer("Произошла ошибка")

# === Шаг 1. Приветствие ===
@dp.message(F.text.startswith("/start"))
async def send_welcome(message: types.Message):
    
    # Получаем метку, если она есть
    args = message.text.split(maxsplit=1)
    ref = args[1] if len(args) > 1 else "без_метки"

    now = datetime.now(MSK).strftime("%Y-%m-%d %H:%M:%S")
    user_id = message.from_user.id
    username = message.from_user.username or ""
    first_name = message.from_user.first_name or ""

    logger.info(f"Команда /start от пользователя {user_id} с меткой: {ref}")

    # 📌 Записываем в Google Sheets
    log_to_sheets([now, str(user_id), username, first_name, "", "START", "/start", "", "", ref])

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Сюрприз", callback_data="bonus")],
        [InlineKeyboardButton(text="ℹ️ Инфа", callback_data="why_free")]
    ])
    await message.answer(
        "🔥 Добро пожаловать!",
        reply_markup=keyboard
    )

# === Шаг 2. Почему бесплатно ===
@dp.callback_query(F.data == "why_free")
async def why_free(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Сюрприз", callback_data="bonus")],
        [InlineKeyboardButton(text="⏭ Точка", callback_data="step_expert")]
    ])
    await safe_edit_message(
        callback,
        "💡 Секрет простой:\n",
        keyboard
    )

# === Шаг 3. Регистрация в БК ===
@dp.callback_query(F.data.in_(["step_bk", "bonus"]))
async def step_bk(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 1", callback_data="bk_Fonbet")],
        [InlineKeyboardButton(text="🔗 2", callback_data="bk_1xbet")],
        [InlineKeyboardButton(text="🔗 3", callback_data="bk_Pari")],
        [InlineKeyboardButton(text="⏭ Точка", callback_data="step_expert")]
    ])
    await safe_edit_message(
        callback,
        "🚀 Ракета\n",
        keyboard
    )

@dp.callback_query(F.data.startswith("bk_"))
async def on_bk_click(callback: types.CallbackQuery):
    bk_name = callback.data.split("_", 1)[1]
    user_choices.setdefault(callback.from_user.id, {})["bk"] = bk_name

    # Логируем
    # await log_to_google_async(callback.from_user, "BK_CLICK", bk_name)

    # Отправляем ссылку на БК
    await callback.message.answer(
        f"🔗 <b>{bk_name}</b> — вот твоя ссылка\n{BK_LINKS[bk_name]}", parse_mode="HTML"
    )
    await callback.answer()

# === Шаг 4. Эксперт ===
@dp.callback_query(F.data == "step_expert")
async def step_expert(callback: types.CallbackQuery):
    # keyboard = InlineKeyboardMarkup(inline_keyboard=[
    #     [InlineKeyboardButton(text="📊 Эксперт по футболу", callback_data="exp_Football_Africa")],
    #     [InlineKeyboardButton(text="📊 Эксперт по киберспорту", callback_data="exp_Cybersport_Gamesport")],
    #     [InlineKeyboardButton(text="🎁 Забрать бонус", callback_data="bonus")],
    #     [InlineKeyboardButton(text="ℹ️ Почему мы это делаем?", callback_data="why_free")],
    #     [InlineKeyboardButton(text="📌 Советы", callback_data="step_tips")]
    # ])

    # Создаём кнопки экспертов динамически
    expert_buttons = [
        [InlineKeyboardButton(text=f"📊 {data['name']}", callback_data=f"exp_{key}")]
        for key, data in EXPERTS.items()
    ]

    # Добавляем дополнительные кнопки
    extra_buttons = [
        [InlineKeyboardButton(text="🎁 Сюрприз", callback_data="bonus")],
        [InlineKeyboardButton(text="ℹ️ Инфа", callback_data="why_free")],
        [InlineKeyboardButton(text="📌 Советы", callback_data="step_tips")]
    ]

    # Итоговая клавиатура
    keyboard = InlineKeyboardMarkup(inline_keyboard=expert_buttons + extra_buttons)

    await safe_edit_message(
        callback,
        "🎯 А вот и Точка📊\n",
        keyboard
    )

@dp.callback_query(F.data.startswith("exp_"))
async def on_expert_click(callback: types.CallbackQuery):
    # Пример: exp_Football_Africa → Football_Africa
    exp_key = callback.data.split("_", 1)[1]
    user_choices.setdefault(callback.from_user.id, {})["expert"] = exp_key
    # exp_name = callback.data.split("_", 1)[1]
    # user_choices.setdefault(callback.from_user.id, {})["expert"] = exp_name

    # Логируем
    # await log_to_google_async(callback.from_user, "EXPERT_CLICK", exp_name)

    expert = EXPERTS.get(exp_key)
    if not expert:
        await callback.message.answer("❌ Эксперт не найден")
        return

    await callback.message.answer(
        f"📊 <b>{EXPERTS[exp_key]['name']}</b>: {EXPERTS[exp_key]['link']}",
        parse_mode="HTML"
    )
    await callback.answer()

    # # Отправляем ссылку
    # await callback.message.answer(
    #     f"📊 Эксперт по <b>{exp_name}</b>: {Expert_LINKS[exp_name]}"
    # )
    # await callback.answer()

# === Шаг 5. Советы ===
@dp.callback_query(F.data == "step_tips")
async def step_tips(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Понял", callback_data="step_expert")]
    ])
    await safe_edit_message(
        callback,
        "⚠️ Советы:\n",
        keyboard
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

# Команда для принудительной остановки других экземпляров
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

# Healthcheck для Render.com
# async def health_check(request):
#     return web.Response(text="Bot is running!", status=200)

# async def start_webapp():
#     """Запуск веб-сервера для healthcheck"""
#     app = web.Application()
#     app.router.add_get('/', health_check)
#     app.router.add_get('/health', health_check)
#     runner = web.AppRunner(app)
#     await runner.setup()


async def handle(request):
    return web.Response(text="I'm alive!")

async def start_web_server():
    port = int(os.environ.get("PORT", 10000))  # Render передаёт PORT
    app = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logging.info(f"Веб-сервер запущен на порту {port}")


#     port = int(os.getenv('PORT', 10000))
#     site = web.TCPSite(runner, '0.0.0.0', port)
#     await site.start()
#     logger.info(f"Веб-сервер запущен на порту {port}")

async def main():
    logger.info("Запуск бота...")
    
    try:
        # Запускаем веб-сервер для healthcheck (ВАЖНО для Render.com!)
        # runner = await start_webapp()
        
        # Агрессивный сброс всех предыдущих экземпляров
        logger.info("Принудительное закрытие всех предыдущих сессий...")
        try:
            # Сначала пробуем получить информацию о боте
            me = await bot.get_me()
            logger.info(f"Бот найден: {me.username} (ID: {me.id})")
            
            # Удаляем webhook и все pending updates
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("Webhook удален, pending updates очищены")
            
            # Дополнительная пауза для гарантии
            await asyncio.sleep(2)
            
        except Exception as e:
            logger.error(f"Ошибка при сбросе webhook: {e}")
        
        # Инициализируем Google Sheets
        sheets_ok = await init_google_sheets()
        if not sheets_ok:
            logger.warning("Google Sheets не удалось инициализировать, логи не будут записываться!")
        
        # Запускаем фоновый веб-сервер для Render (healthcheck)
        asyncio.create_task(start_web_server())

        logger.info("Начинаем polling...")
        await dp.start_polling(
            bot, 
            drop_pending_updates=True,
            allowed_updates=dp.resolve_used_update_types()
        )
        
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске бота: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
    finally:
        logger.info("Бот завершает работу...")
        try:
            await bot.session.close()
        except:
            pass

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
