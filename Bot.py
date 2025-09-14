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
            now,                           # Время
            str(user.id),                  # ID пользователя
            user.username or "",           # Username
            user.first_name or "",         # Имя
            user.last_name or "",          # Фамилия
            user.language_code or "unknown", # Язык
            event_type,                    # Тип события
            content,                       # Содержание
            additional_data or "",         # Дополнительные данные
            str(user.is_premium or False), # Telegram Premium
            ""  # Место для подтверждения перехода
        ]
        
        # Записываем точно в нужные ячейки
        range_name = f"A{next_row}:G{next_row}"
        
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

def generate_tracking_link(bk_name, user_id):
    """Генерация ссылки с отслеживанием"""
    import urllib.parse
    base_url = BK_LINKS[bk_name]
    tracking_data = f"ref_{user_id}_{bk_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Кодируем данные для URL
    encoded_data = urllib.parse.quote(tracking_data)
    return f"{base_url}?ref={encoded_data}"

# Обновляем словарь BK_LINKS с отслеживанием
def get_tracking_bk_links(user_id):
    return {
        "Fonbet": generate_tracking_link("Fonbet", user_id),
        "1xbet": generate_tracking_link("1xbet", user_id), 
        "Pari": generate_tracking_link("Pari", user_id)
    }

# Клавиатура для запроса телефона
phone_keyboard = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="📞 Отправить номер телефона", request_contact=True)]],
    resize_keyboard=True,
    one_time_keyboard=True
)

# Клавиатура для запроса локации
location_keyboard = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="📍 Отправить геолокацию", request_location=True)]],
    resize_keyboard=True,
    one_time_keyboard=True
)

@dp.message(F.contact)
async def handle_contact(message: types.Message):
    """Обработка полученного номера телефона"""
    phone = message.contact.phone_number
    await log_to_google_async(
        message.from_user, 
        "PHONE_REQUEST", 
        f"Получен номер телефона",
        f"phone:{phone}"
    )
    
    await message.answer(
        "✅ Спасибо! Номер телефона получен.\n"
        "Это поможет нам персонализировать предложения.",
        reply_markup=ReplyKeyboardRemove()
    )

@dp.message(F.location)
async def handle_location(message: types.Message):
    """Обработка полученной геолокации"""
    lat = message.location.latitude
    lon = message.location.longitude
    await log_to_google_async(
        message.from_user,
        "LOCATION_REQUEST", 
        f"Получена геолокация",
        f"lat:{lat},lon:{lon}"
    )
    
    await message.answer(
        "✅ Спасибо! Геолокация получена.\n"
        "Мы учтем ваш регион для персонализации предложений.",
        reply_markup=ReplyKeyboardRemove()
    )

# Добавим простой веб-сервер для отслеживания переходов
async def handle_tracking(request):
    """Обработчик переходов по tracking ссылкам"""
    try:
        ref_data = request.query.get('ref', '')
        # Декодируем и разбираем данные
        # Здесь можно добавить запись в базу или Google Sheets
        logger.info(f"Переход по ссылке: {ref_data}")
        return web.Response(text="Redirecting...", status=302, headers={'Location': 'https://target-site.com'})
    except:
        return web.Response(status=400)

# Добавить в start_webapp
app.router.add_get('/track', handle_tracking)

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
    await safe_edit_message(
        callback,
        "💡 Секрет простой:\n"
        "– Ты играешь и выигрываешь по нашим прогнозам\n"
        "– Мы договариваемся о матчах, но не можем ставить много, поэтому делимся и получаем бонус\n\n"
        "👉 Поэтому нам выгодно, чтобы ты выигрывал и оставался с нами 👍",
        keyboard
    )

# === Шаг 3. Регистрация в БК ===
@dp.callback_query(F.data.in_(["step_bk", "bonus"]))
async def step_bk(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    tracking_links = get_tracking_bk_links(user_id)

     # Логируем клик по разделу БК
    await log_to_google_async(
        callback.from_user,
        "BK_CLICK",
        "Пользователь открыл раздел БК"
    )

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Fonbet - Бонус 1к", url=tracking_links["Fonbet"])],
        [InlineKeyboardButton(text="🔗 1xBet - Бонус 2к", url=tracking_links["1xbet"])],
        [InlineKeyboardButton(text="🔗 Pari - Бонус 5к", url=tracking_links["Pari"])],
        [InlineKeyboardButton(text="⏭ Эксперты", callback_data="step_expert")]
    ])

    await safe_edit_message(
        callback,
        "🚀 Переходи по ссылке, регистрируйся в БК и активируй бонус при пополнении.\n"
        "⚡ Вот несколько контор, которые ты можешь добавить.\n"
        "‼ Советуем распределить деньги по нескольким компаниям.\n\n"
        "⏭ Как закончишь, переходи к экспертам.",
        keyboard
    )

# === Шаг 4. Эксперт ===
@dp.callback_query(F.data == "step_expert")
async def step_expert(callback: types.CallbackQuery):
    # Логируем клик по разделу экспертов
    await log_to_google_async(
        callback.from_user,
        "EXPERT_CLICK", 
        "Пользователь открыл раздел экспертов"
    )

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Перейти к эксперту по футболу", url=Expert_LINKS["Football"])],
        [InlineKeyboardButton(text="📊 Перейти к эксперту по киберспорту", url=Expert_LINKS["Cybersport"])],
        [InlineKeyboardButton(text="🎁 Забрать бонус", callback_data="bonus")],
        [InlineKeyboardButton(text="ℹ️ Почему мы это делаем?", callback_data="why_free")],
        [InlineKeyboardButton(text="📌 Советы", callback_data="step_tips")],
        [InlineKeyboardButton(text="📞 Предоставить номер", callback_data="request_phone")],
        [InlineKeyboardButton(text="📍 Предоставить геолокацию", callback_data="request_location")]
    ])
    await safe_edit_message(
        callback,
        "🎯 А вот и ссылка на экспертов.📊\n"
        "Перейди в канал и попроси доступ к прогнозам, чтобы получить сегодняшние рекомендации и первую стратегию.\n\n"
        "Иногда мы добавляем новые бонусы от букмекеров, поэтому ждем тебя снова.\n"
        "И не забудь прочитать советы перед тем как ставить.\n",
        keyboard
    )

# === Шаг 5. Советы ===
@dp.callback_query(F.data == "step_tips")
async def step_tips(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Понял", callback_data="step_expert")]
    ])
    await safe_edit_message(
        callback,
        "⚠️ Перед тем как начать:\n"
        "1️. Ставь сначала маленькие суммы — важно привыкнуть к системе.\n"
        "2️. Для исключения блокировки аккаунта, иногда нужно специально проиграть ставку.\n"
        "3. Если вдруг что-то пойдёт не так — эксперт предложит компенсацию/совет, чтобы вернуть доверие\n\n"
        "👉 Твой плюс = наш плюс 💪",
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
async def health_check(request):
    return web.Response(text="Bot is running!", status=200)

async def start_webapp():
    """Запуск веб-сервера для healthcheck"""
    app = web.Application()
    app.router.add_get('/', health_check)
    app.router.add_get('/health', health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.getenv('PORT', 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"Веб-сервер запущен на порту {port}")

async def main():
    logger.info("Запуск бота...")
    
    try:
        # Запускаем веб-сервер для healthcheck (ВАЖНО для Render.com!)
        runner = await start_webapp()
        
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
    asyncio.run(main())
