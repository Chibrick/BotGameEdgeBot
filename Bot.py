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
from aiogram import BaseMiddleware
import traceback

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
sheet_offers = None    # "Офферы"

# меню пользователя: хранит (chat_id, message_id, category, page)
USER_MENU_MESSAGE: dict[int, dict] = {}

OFFERS = {}
OFFERS_BY_CATEGORY = {}   # { "Дебетовые карты": [offer_obj, ...], ... }
OFFERS_BY_ID = {}         # { "1": offer_obj, ... }
CLIENT_OFFER_COL_MAP = {} # { offer_id_int: column_index_in_clients_sheet }
PENDING_OFFER = {}        # { user_id: offer_id } временная память ожидающих ввода кода
PAGE_SIZE = 5

# Константы кол-во колонок и индексы (A..Q)
NUM_COLUMNS = 17
IDX_CLIENT_NO = 1   # A
IDX_USER_ID = 2     # B
IDX_USERNAME = 3    # C
IDX_FIRST_NAME = 4  # D
IDX_PHONE = 5       # E
IDX_DATE = 6        # F
IDX_MARK = 7        # G
IDX_OFFER_NO = 8    # H
# I..O = 9..15 (чекбоксы офферов)

# OFFERS = {
#     "debit": {
#         "tbank_black": {"name": "Дебетовая карта Black (Т-Банк)", "link": "https://..."},
#         "sber": {"name": "Сбербанк Дебетовая", "link": "https://..."},
#     },
#     "credit": {
#         "tbank_platinum": {"name": "Кредитная карта Platinum (Т-Банк)", "link": "https://..."},
#     },
#     "bk": {
#         "fonbet": {"name": "Fonbet Бонус", "link": "https://..."},
#     }
# }

SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1pGc9kdpdFggwZlc3wairUBunou1BW_fw-D-heBViHic/edit#gid=0"

class LoggingMiddleware(BaseMiddleware):
    """
    Логирует входящие Message и CallbackQuery.
    Не блокирует обработчик — логирование в Google выполняется в background (create_task).
    """
    async def __call__(self, handler, event, data):
        try:
            # MESSAGE
            if isinstance(event, types.Message):
                user = event.from_user
                text = (event.text or "")[:300]
                logger.info(f"[MSG] {user.id} @{user.username} : {text}")
                # лог в Google в фоне (не блокируем основной обработчик)
                try:
                    asyncio.create_task(log_event(user, "MSG", text))
                except Exception as e:
                    logger.error(f"Failed schedule log_event: {e}")

            # CALLBACK QUERY
            elif isinstance(event, types.CallbackQuery):
                user = event.from_user
                data_text = (event.data or "")[:200]
                logger.info(f"[CB] {user.id} @{user.username} : {data_text}")
                try:
                    asyncio.create_task(log_event(user, "CB", data_text))
                except Exception as e:
                    logger.error(f"Failed schedule log_event: {e}")

        except Exception as e:
            logger.error("Ошибка в LoggingMiddleware: " + str(e))
            logger.error(traceback.format_exc())

        # передаём событие дальше
        return await handler(event, data)

async def run_in_executor(fn, *args, **kwargs):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))

async def store_menu_message_for_user(user_id: int, msg: types.Message):
    USER_MENU_MESSAGE[user_id] = {
        "chat_id": msg.chat.id,
        "message_id": msg.message_id,
        "category": None,
        "page": 1
    }

async def edit_user_menu(user_id: int, text: str, keyboard: InlineKeyboardMarkup | None = None):
    """
    Попытка отредактировать существующее меню. Если не получилось — отправляем новое сообщение и сохраняем его.
    """
    info = USER_MENU_MESSAGE.get(user_id)
    if info:
        try:
            await bot.edit_message_text(
                text=text,
                chat_id=info["chat_id"],
                message_id=info["message_id"],
                reply_markup=keyboard,
                parse_mode="HTML"
            )
            return
        except Exception as e:
            logger.warning(f"Не удалось отредактировать меню (user {user_id}): {e}")

    # fallback: отправляем новое сообщение и сохраняем его как меню
    try:
        msg = await bot.send_message(chat_id=user_id, text=text, reply_markup=keyboard, parse_mode="HTML")
        await store_menu_message_for_user(user_id, msg)
    except Exception as e:
        logger.error(f"Ошибка при отправке fallback-меню пользователю {user_id}: {e}")

async def init_google_sheets():
    """Инициализация Google Sheets"""
    global client, sheet_clients, sheet_logs, sheet_offers
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
        sheet_offers = spreadsheet.worksheet("Офферы")

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
            if phone:
                row_vals[IDX_PHONE - 1] = phone
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

            range_name = f"A{row_index}:BK{row_index}"
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
            new_row[IDX_DATE - 1] = datetime.now(MSK).strftime("%Y-%m-%d %H:%M:%S")
            new_row[IDX_MARK - 1] = mark or ""
            new_row[IDX_OFFER_NO - 1] = offer_no or offer or ""
            # I..O checkboxes left empty            

            range_name = f"A{next_row}:AA{next_row}"
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

def _find_col_index_by_keywords(headers, keywords):
    """Возвращает индекс колонки (1-based) в headers, содержащий одно из keywords. None если не найдено."""
    for i, h in enumerate(headers, start=1):
        h_str = (h or "").strip().lower()
        for kw in keywords:
            if kw in h_str:
                return i
    return None

# async def load_offers_from_sheet():
#     """Загружает офферы из листа 'Офферы' в OFFERS_BY_CATEGORY и OFFERS_BY_ID.
#        Ожидает, что init_google_sheets уже выполнен и sheet_offers доступен.
#     """
#     global OFFERS, OFFERS_BY_CATEGORY, OFFERS_BY_ID, sheet_offers
#     if not sheet_offers:
#         logger.error("sheet_offers не инициализирован")
#         return False
#     try:
#         rows = await run_in_executor(sheet_offers.get_all_values)
#         if not rows or len(rows) < 2:
#             logger.warning("Лист 'Офферы' пуст или нет данных")
#             OFFERS_BY_CATEGORY = {}
#             OFFERS_BY_ID = {}
#             return True

#         header = rows[0]
#         # определяем колонки по ключевым словам (robust)
#         id_col = _find_col_index_by_keywords(header, ["№", "номер", "№ оффера", "№оффера", "id"])
#         cat_col = _find_col_index_by_keywords(header, ["категори", "category", "категория"])
#         name_col = _find_col_index_by_keywords(header, ["назван", "name", "название"])
#         partner_link_col = _find_col_index_by_keywords(header, ["партн", "партнёр", "partner", "партнёрская ссылка", "партнёрская"])
#         code_col = _find_col_index_by_keywords(header, ["код", "code"])
#         direct_link_col = _find_col_index_by_keywords(header, ["ссылка", "link"])

#         # fallback: если какие-то не определились — ставим дефолты (A..L)
#         id_col = id_col or 1
#         cat_col = cat_col or 2
#         direct_link_col = direct_link_col or 3
#         name_col = name_col or 4
#         partner_link_col = partner_link_col or direct_link_col
#         code_col = code_col or len(header)  # если нет, ставим последнюю колонку

#         OFFERS_BY_CATEGORY = {}
#         OFFERS_BY_ID = {}

#         for idx, row in enumerate(rows[1:], start=2):
#             # безопасно получить значение по колонке
#             def get(r, col_idx):
#                 try:
#                     return r[col_idx-1].strip()
#                 except Exception:
#                     return ""

#             offer_id = get(row, id_col)
#             if not offer_id:
#                 continue
#             # normalize id as string
#             offer_id = str(offer_id)
#             category = get(row, cat_col) or "Без категории"
#             name = get(row, name_col) or f"Оффер {offer_id}"
#             partner_link = get(row, partner_link_col) or get(row, direct_link_col)
#             code = get(row, code_col) or ""
#             offer_obj = {
#                 "id": offer_id,
#                 "category": category,
#                 "name": name,
#                 "partner_link": partner_link,
#                 "code": code,
#                 "row": idx  # реальная строка в листе "Офферы"
#             }
#             OFFERS_BY_ID[offer_id] = offer_obj
#             OFFERS_BY_CATEGORY.setdefault(category, []).append(offer_obj)

#         # сортируем офферы в каждой категории по числовому id (если можно)
#         for k, lst in OFFERS_BY_CATEGORY.items():
#             try:
#                 lst.sort(key=lambda x: int(x["id"]))
#             except:
#                 lst.sort(key=lambda x: x["id"])

#         logger.info(f"Загружено офферов: {len(OFFERS_BY_ID)} категорий: {len(OFFERS_BY_CATEGORY)}")
#         return True
#     except Exception as e:
#         logger.error(f"Ошибка load_offers_from_sheet: {e}")
#         logger.error(traceback.format_exc())
#         return False

async def load_offers_from_sheet():
    global OFFERS, OFFERS_BY_CATEGORY, OFFERS_BY_ID, sheet_offers
    if not sheet_offers:
        logger.error("sheet_offers не инициализирован")
        return False

    try:
        values = sheet_offers.get_all_values()
        if not values or len(values) < 2:
            logger.warning("Лист 'Офферы' пуст или нет данных")
            OFFERS, OFFERS_BY_CATEGORY, OFFERS_BY_ID = {}, {}, {}
            return False

        header = values[0]
        rows = values[1:]

        OFFERS = {}
        OFFERS_BY_CATEGORY = {}
        OFFERS_BY_ID = {}

        for idx, row in enumerate(rows, start=2):
            if not row or not row[0].strip():
                continue

            offer_id = row[0].strip()             # № оффера (A)
            category = row[1].strip()             # Категория (B)
            link = row[2].strip()                 # Ссылка (C)
            name = row[3].strip()                 # Название (D)
            code = row[11].strip() if len(row) > 11 else ""  # Код (L)

            if not category:
                category = "Без категории"

            # сохраняем в OFFERS
            if category not in OFFERS:
                OFFERS[category] = {}
            OFFERS[category][offer_id] = {
                "name": name,
                "link": link,
                "code": code
            }

            # создаём универсальный объект
            offer_obj = {
                "id": offer_id,
                "category": category,
                "name": name,
                "partner_link": link,
                "code": code,
                "row": idx
            }

            OFFERS_BY_ID[offer_id] = offer_obj
            OFFERS_BY_CATEGORY.setdefault(category, []).append(offer_obj)

        # сортируем офферы в каждой категории
        for k, lst in OFFERS_BY_CATEGORY.items():
            try:
                lst.sort(key=lambda x: int(x["id"]))
            except:
                lst.sort(key=lambda x: x["id"])

        logger.info(f"Офферы загружены: {len(OFFERS_BY_ID)} шт. в {len(OFFERS_BY_CATEGORY)} категориях")
        return True

    except Exception as e:
        logger.error(f"Ошибка при загрузке офферов: {e}")
        logger.error(traceback.format_exc())
        return False

async def build_client_offer_col_map():
    """Считает маппинг: offer_id (int) -> column_index (в листе 'Клиенты - Партнерки'),
       если в шапке есть колонки с номерами офферов.
    """
    global CLIENT_OFFER_COL_MAP, sheet_clients
    CLIENT_OFFER_COL_MAP = {}
    if not sheet_clients:
        logger.error("sheet_clients не инициализирован")
        return {}
    try:
        header = await run_in_executor(sheet_clients.row_values, 1)
        # header is list of header values; ищем ячейки, которые являются числом (1,2,3...)
        for i, h in enumerate(header, start=1):
            if not h:
                continue
            hs = h.strip()
            # если значение точно число (например "1" или "10"), мапим
            if hs.isdigit():
                CLIENT_OFFER_COL_MAP[int(hs)] = i
        logger.info(f"Client offer col map built: {CLIENT_OFFER_COL_MAP}")
        return CLIENT_OFFER_COL_MAP
    except Exception as e:
        logger.error(f"Ошибка build_client_offer_col_map: {e}")
        logger.error(traceback.format_exc())
        return {}

async def _get_client_row_index(user_id: str):
    """Возвращает номер строки в sheet_clients (1-based) где в колонке B (IDX_USER_ID) содержится user_id"""
    if not sheet_clients:
        return None
    try:
        col_vals = await run_in_executor(sheet_clients.col_values, IDX_USER_ID)
        for i, v in enumerate(col_vals, start=1):
            if v == user_id:
                return i
        return None
    except Exception as e:
        logger.error(f"_get_client_row_index error: {e}")
        return None

async def get_user_taken_offers_by_row(row_index):
    """Возвращает set() числовых id офферов, которые отмечены для пользователя в строке row_index.
       Смотрим: 1) поле H (IDX_OFFER_NO) — если там список '1;3;10', 2) чекбоксы по CLIENT_OFFER_COL_MAP.
    """
    taken = set()
    if not sheet_clients:
        return taken
    try:
        row_vals = await run_in_executor(sheet_clients.row_values, row_index)
        row_vals = _pad_row(row_vals, NUM_COLUMNS)
        # поле H (IDX_OFFER_NO) может содержать "1;3;10"
        raw = row_vals[IDX_OFFER_NO - 1] or ""
        for x in raw.replace(" ", "").split(";"):
            if x.strip().isdigit():
                taken.add(x.strip())
        # чекбоксы
        if CLIENT_OFFER_COL_MAP:
            for offer_id, col_idx in CLIENT_OFFER_COL_MAP.items():
                try:
                    v = row_vals[col_idx-1]
                except:
                    v = ""
                if str(v).strip().lower() in ("true", "1", "да", "x", "x "):
                    taken.add(str(offer_id))
        return taken
    except Exception as e:
        logger.error(f"get_user_taken_offers_by_row error: {e}")
        logger.error(traceback.format_exc())
        return taken

async def mark_offer_taken_for_user(row_index, offer_id):
    """Помечает оффер за пользователем: 
       - дописывает offer_id в H (если не было)
       - ставит чекбокс TRUE в соответствующей колонке, если она присутствует
    """
    if not sheet_clients:
        return False
    try:
        # получаем текущую строку
        row_vals = await run_in_executor(sheet_clients.row_values, row_index)
        row_vals = _pad_row(row_vals, NUM_COLUMNS)

        # обновим поле H (IDX_OFFER_NO)
        already = row_vals[IDX_OFFER_NO - 1] or ""
        parts = [p for p in [s.strip() for s in already.split(";")] if p]
        if str(offer_id) not in parts:
            parts.append(str(offer_id))
        new_h = ";".join(parts)
        row_vals[IDX_OFFER_NO - 1] = new_h

        # если есть колонка чекбокса для этого оффера — отметим
        if CLIENT_OFFER_COL_MAP:
            try:
                offer_int = int(offer_id)
                col_idx = CLIENT_OFFER_COL_MAP.get(offer_int)
            except:
                col_idx = None
            if col_idx:
                # пометим TRUE в колонке col_idx
                row_vals[col_idx - 1] = "SELECTED"

        # обновляем всю строку A..Q
        range_name = f"A{row_index}:BK{row_index}"
        await run_in_executor(sheet_clients.update, range_name, [row_vals], {'valueInputOption': 'USER_ENTERED'})
        logger.info(f"Offer {offer_id} marked for row {row_index}")
        return True
    except Exception as e:
        logger.error(f"mark_offer_taken_for_user error: {e}")
        logger.error(traceback.format_exc())
        return False

# def _build_offers_keyboard(offers_page, category, page, total_pages):
#     """Создаёт клавиатуру для списка офферов (offers_page — список offer_obj)."""
#     kb = InlineKeyboardMarkup()
#     for off in offers_page:
#         text = f"{off['id']}. {off['name']}"
#         kb.add(InlineKeyboardButton(text=text, callback_data=f"offer_select:{off['id']}"))
#     # навигация
#     nav_row = []
#     if page > 1:
#         nav_row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"offers_page:{category}:{page-1}"))
#     if page < total_pages:
#         nav_row.append(InlineKeyboardButton(text="Вперёд ➡️", callback_data=f"offers_page:{category}:{page+1}"))
#     if nav_row:
#         kb.row(*nav_row)
#     kb.add(InlineKeyboardButton(text="◀️ Вернуться", callback_data="back_to_categories"))
#     return kb

def _build_offers_keyboard(offers_page, category, page, total_pages):
    """Создаёт клавиатуру для списка офферов (offers_page — список offer_obj)."""
    buttons: list[list[InlineKeyboardButton]] = []

    # кнопки офферов
    for off in offers_page:
        text = f"{off['id']}. {off['name']}"
        buttons.append([InlineKeyboardButton(text=text, callback_data=f"offer_select:{off['id']}")])

    # навигация
    nav_row: list[InlineKeyboardButton] = []
    if page > 1:
        nav_row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"offers_page:{category}:{page-1}"))
    if page < total_pages:
        nav_row.append(InlineKeyboardButton(text="Вперёд ➡️", callback_data=f"offers_page:{category}:{page+1}"))
    if nav_row:
        buttons.append(nav_row)

    # кнопка возврата
    buttons.append([InlineKeyboardButton(text="◀️ Вернуться", callback_data="back_to_categories")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def show_offers_page_for_user(user_id: int, category: str, page: int = 1):
    """Редактирует пользовательское меню, показывая страницу офферов."""
    lst = OFFERS_BY_CATEGORY.get(category, [])
    if not lst:
        await edit_user_menu(user_id, "В этой категории пока нет офферов.", None)
        return

    # получаем ряд пользователя (если есть) и какие офферы уже брал
    row_index = await _get_client_row_index(str(user_id))
    taken = set()
    if row_index:
        taken = await get_user_taken_offers_by_row(row_index)

    # фильтруем доступные офферы
    available = [o for o in lst if o['id'] not in taken]
    if not available:
        await edit_user_menu(user_id, "❗ Все офферы в этой категории вы уже брали.", None)
        return

    total = len(available)
    total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    page = max(1, min(page, total_pages))
    start = (page - 1) * PAGE_SIZE
    page_slice = available[start:start + PAGE_SIZE]

    # текст и клавиатура
    text = f"Категория: {category}\nСтраница {page}/{total_pages}\nВыберите оффер:"
    kb = _build_offers_keyboard(page_slice, category, page, total_pages)

    # редактируем или отправляем меню
    await edit_user_menu(user_id, text, kb)

    # обновляем сохранённые метаданные меню
    info = USER_MENU_MESSAGE.get(user_id)
    if info:
        info["category"] = category
        info["page"] = page






# === Handlers ===

@dp.message(Command(commands=["reload_offers"]))
async def cmd_reload_offers(message: types.Message):
    # ручной лог события команды
    ok = await load_offers_from_sheet()
    await build_client_offer_col_map()
    if ok:
        await message.answer("Офферы перезагружены.")
    else:
        await message.answer("Ошибка при перезагрузке офферов.")

@dp.message(Command(commands=["force_reset"]))
async def force_reset(message: types.Message):
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await bot.get_me()
        await message.answer("✅ Webhook сброшен, все апдейты очищены!")
        logger.info(f"Принудительный сброс выполнен пользователем {message.from_user.id}")
    except Exception as e:
        await message.answer(f"❌ Ошибка при сбросе: {e}")
        logger.error(f"Ошибка при принудительном сбросе: {e}")

@dp.message(Command(commands=["test_log"]))
async def test_log(message: types.Message):
    ok = await log_event(message.from_user, "CMD", "/test_log")
    if ok:
        await message.answer("✅ Лог записан")
    else:
        await message.answer("❌ Ошибка при записи лога")

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

    # Формируем кнопки категорий динамически из OFFERS_BY_CATEGORY (или OFFERS)
    categories = list(OFFERS_BY_CATEGORY.keys()) if OFFERS_BY_CATEGORY else list(OFFERS.keys())
    if not categories:
        # если ещё нет офферов — просто уведомим
        msg = await message.answer("✅ Отлично! Данные сохранены.\n\nНа данный момент офферы не загружены. Попробуй позже.")
        return

    category_buttons = [
        [InlineKeyboardButton(text=f"📂 {cat}", callback_data=f"category:{cat}")]
        for cat in categories
    ]

    category_buttons.append([InlineKeyboardButton(text="📋 Мои офферы", callback_data="my_offers")])

    keyboard = InlineKeyboardMarkup(inline_keyboard=category_buttons)
    msg = await message.answer(
        "✅ Отлично! Данные сохранены.\n\nТеперь ты в системе! Выбери категорию оффера: 👇",
        reply_markup=keyboard
    )

    # Сохраняем это сообщение как "меню" пользователя
    await store_menu_message_for_user(message.from_user.id, msg)







@dp.callback_query(F.data == "my_offers")
async def show_my_offers_menu(callback: types.CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟡 Офферы в работе", callback_data="my_offers_in_progress")],
        [InlineKeyboardButton(text="✅ Выполненные офферы", callback_data="my_offers_done")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_categories")]
    ])
    await edit_user_menu(
        callback.from_user.id,
        "📋 <b>Мои офферы</b>\n\nЭто твои офферы, можешь посмотреть подробную информацию:",
        reply_markup=kb
    )
    await callback.answer()

@dp.callback_query(F.data == "my_offers_in_progress")
async def show_my_offers_in_progress(callback: types.CallbackQuery):
    user_id = str(callback.from_user.id)
    row_index = await _get_client_row_index(user_id)
    if not row_index:
        await callback.answer("❌ Ты не зарегистрирован")
        return

    # забираем всю строку
    row_vals = await run_in_executor(sheet_clients.row_values, row_index)
    row_vals = _pad_row(row_vals, NUM_COLUMNS)

    selected_offers = []
    for offer in OFFERS_BY_ID.values():
        try:
            col_idx = CLIENT_OFFER_COL_MAP.get(int(offer["id"]))
            if col_idx and col_idx <= len(row_vals):
                val = str(row_vals[col_idx - 1]).strip().upper()
                if val == "SELECTED":
                    selected_offers.append(offer)
        except:
            continue

    if not selected_offers:
        await edit_user_menu(callback.from_user.id, "🟡 У тебя нет офферов в работе.", None)
        await callback.answer()
        return

    # пагинация по 5
    total = len(selected_offers)
    total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    page_slice = selected_offers[:PAGE_SIZE]

    kb = _build_offers_keyboard(page_slice, "my_offers_in_progress", 1, total_pages)
    await edit_user_menu(
        callback.from_user.id,
        f"🟡 <b>Офферы в работе</b>\nСтраница 1/{total_pages}",
        reply_markup=kb
    )
    await callback.answer()

@dp.callback_query(F.data == "my_offers_done")
async def show_my_offers_done(callback: types.CallbackQuery):
    user_id = str(callback.from_user.id)
    row_index = await _get_client_row_index(user_id)
    if not row_index:
        await callback.answer("❌ Ты не зарегистрирован")
        return

    row_vals = await run_in_executor(sheet_clients.row_values, row_index)
    row_vals = _pad_row(row_vals, NUM_COLUMNS)

    done_offers = []
    for offer in OFFERS_BY_ID.values():
        try:
            col_idx = CLIENT_OFFER_COL_MAP.get(int(offer["id"]))
            if col_idx and col_idx <= len(row_vals):
                val = str(row_vals[col_idx - 1]).strip().upper()
                if val == "DONE":
                    done_offers.append(offer)
        except:
            continue

    if not done_offers:
        await edit_user_menu(callback.from_user.id, "✅ У тебя нет выполненных офферов.", None)
        await callback.answer()
        return

    total = len(done_offers)
    total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    page_slice = done_offers[:PAGE_SIZE]

    kb = _build_offers_keyboard(page_slice, "my_offers_done", 1, total_pages)
    await edit_user_menu(
        callback.from_user.id,
        f"✅ <b>Выполненные офферы</b>\nСтраница 1/{total_pages}",
        reply_markup=kb
    )
    await callback.answer()











# обработчик выбора категории
@dp.callback_query(F.data.startswith("category:"))
async def category_handler(callback: types.CallbackQuery):
    # category:Дебетовые карты
    _, category = callback.data.split(":", 1)
    await edit_user_menu(callback.from_user.id, f"✅ Ты выбрал категорию: {category}. Подождите...", None)
    await show_offers_page_for_user(callback.from_user.id, category, page=1)
    await callback.answer()

# обработчик навигации страниц
@dp.callback_query(F.data.startswith("offers_page:"))
async def offers_page_handler(callback: types.CallbackQuery):
    try:
        _, cat, page_str = callback.data.split(":", 2)
        page = int(page_str)
    except:
        await callback.answer("Ошибка навигации")
        return
    await show_offers_page_for_user(callback.from_user.id, cat, page)
    await callback.answer()

@dp.callback_query(F.data == "back_to_categories")
async def back_to_categories_handler(callback: types.CallbackQuery):
    # формируем список категорий
    categories = list(OFFERS_BY_CATEGORY.keys()) if OFFERS_BY_CATEGORY else list(OFFERS.keys())
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=f"📂 {cat}", callback_data=f"category:{cat}")] for cat in categories]
    )
    await edit_user_menu(callback.from_user.id, "Выберите категорию оффера:", keyboard)
    await callback.answer()

# обработчик выбора конкретного оффера
@dp.callback_query(F.data.startswith("offer_select:"))
async def offer_select_handler(callback: types.CallbackQuery):
    offer_id = callback.data.split(":", 1)[1]
    offer = OFFERS_BY_ID.get(offer_id)
    if not offer:
        await callback.answer("Оффер не найден")
        return

    # проверим, не брал ли пользователь уже этот оффер
    row_index = await _get_client_row_index(str(callback.from_user.id))
    if row_index:
        taken = await get_user_taken_offers_by_row(row_index)
        if offer_id in taken:
            await callback.answer("Вы уже брали этот оффер.")
            return

    # Помечаем ожидание кода
    PENDING_OFFER[callback.from_user.id] = offer_id

    # редактируем меню и показываем запрос ввода кода + кнопку "Отмена"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_pending")]
    ])
    prompt = f"Вы выбрали оффер {offer_id} — {offer['name']}\n\nВведи код, который указан рядом с оффером (в таблице).\n\nЕсли хочешь выйти — нажми ❌ Отмена или напиши 'отмена'."
    await edit_user_menu(callback.from_user.id, prompt, kb)
    await callback.answer()

@dp.callback_query(F.data == "cancel_pending")
async def cancel_pending_cb(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    PENDING_OFFER.pop(user_id, None)
    # если пользователь ранее был в категории — возвращаем страницу
    info = USER_MENU_MESSAGE.get(user_id)
    if info and info.get("category"):
        await show_offers_page_for_user(user_id, info["category"], info.get("page", 1))
    else:
        # возвращаем категории
        categories = list(OFFERS_BY_CATEGORY.keys()) if OFFERS_BY_CATEGORY else list(OFFERS.keys())
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"📂 {cat}", callback_data=f"category:{cat}")] for cat in categories])
        await edit_user_menu(user_id, "Выберите категорию оффера:", kb)
    await callback.answer("Отменено")

@dp.message()
async def handle_messages_for_code(message: types.Message):
    user_id = message.from_user.id
    text = (message.text or "").strip()

    # логируем все входящие
    await log_event(message.from_user, "FALLBACK_MSG", text)

    # если мы не ждём код — игнорируем (или можно обрабатывать другие фразы)
    if user_id not in PENDING_OFFER:
        return

    # поддержка слов отмены
    if text.lower() in ("отмена", "отменить", "cancel", "exit"):
        PENDING_OFFER.pop(user_id, None)
        info = USER_MENU_MESSAGE.get(user_id)
        if info and info.get("category"):
            await show_offers_page_for_user(user_id, info["category"], info.get("page", 1))
        else:
            # показать категории
            categories = list(OFFERS_BY_CATEGORY.keys()) if OFFERS_BY_CATEGORY else list(OFFERS.keys())
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"📂 {cat}", callback_data=f"category:{cat}")] for cat in categories])
            await edit_user_menu(user_id, "Отменено. Выберите категорию оффера:", kb)
        return

    offer_id = PENDING_OFFER.get(user_id)
    offer = OFFERS_BY_ID.get(offer_id)
    if not offer:
        await message.answer("Ошибка: оффер не найден. Попробуй выбрать снова.")
        PENDING_OFFER.pop(user_id, None)
        return

    entered = text.strip().lower()
    correct = (offer.get("code","").strip().lower() == entered)
    if not correct:
        # редактируем то же меню с пометкой "Код неверный"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_pending")]
        ])
        await edit_user_menu(user_id, f"Код неверный. Попробуй ещё раз или нажми ❌ Отмена.\n\nОффер: {offer_id} — {offer['name']}", kb)
        await log_event(message.from_user, "OFFER_CODE_INCORRECT", f"{offer_id} / {entered}")
        return

    # код верный -> отмечаем и даём ссылку (и редактируем меню на подтверждение)
    # помечаем в таблице
    row_index = await _get_client_row_index(str(user_id))
    if not row_index:
        await update_client(message.from_user, status="взял оффер", offer=offer_id)
        row_index = await _get_client_row_index(str(user_id))

    ok = await mark_offer_taken_for_user(row_index, offer_id)
    if not ok:
        logger.warning("Не удалось пометить оффер, но всё равно отправлю ссылку.")

    await log_event(message.from_user, "OFFER_TAKEN", offer_id)

    # редактируем меню: показываем ссылку + кнопку "Вернуться к офферам"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Вернуться к офферам", callback_data=f"offers_page:{offer['category']}:1")],
        [InlineKeyboardButton(text="⬅️ К категориям", callback_data="back_to_categories")]
    ])
    text_ok = f"✅ Код верный! Вот ссылка на оффер:\n{offer.get('partner_link') or offer.get('link')}\n\nПосле выполнения нажми «◀️ Вернуться к офферам» или «⬅️ К категориям»."
    # очистим pending
    PENDING_OFFER.pop(user_id, None)
    await edit_user_menu(user_id, text_ok, kb)

# @dp.callback_query(F.data.startswith("offer_"))
# async def on_bk_click(callback: types.CallbackQuery):
#     bk_name = callback.data.split("_", 1)[1]
#     offer_category = callback.data.split("_", 1)[1]
#     # await update_client(callback.from_user, offer=offer_category, status="Выбрал категорию")
#     await log_event(callback.from_user, "Сategory_CLICK", offer_category)

#     # здесь вместо callback.message.edit_text - отправляем приватное сообщение с ссылкой
#     # (так пользователь видит ссылку внизу, а не сообщение edit)
#     await callback.message.answer(
#         f"🔗 <b>{bk_name}</b> — вот твоя ссылка для получения бонуса:\nhttps://example.com/{bk_name}",
#         parse_mode="HTML"
#     )
#     await callback.answer()

# @dp.callback_query(F.data.startswith("exp_"))
# async def on_expert_click(callback: types.CallbackQuery):
#     # Пример: exp_Football_Africa → Football_Africa
#     exp_key = callback.data.split("_", 1)[1]
#     user_choices.setdefault(callback.from_user.id, {})["expert"] = exp_key

#     expert = EXPERTS.get(exp_key)
#     if not expert:
#         await callback.message.answer("❌ Эксперт не найден")
#         return

#     await callback.message.answer(
#         f"📊 <b>{EXPERTS[exp_key]['name']}</b>: {EXPERTS[exp_key]['link']}",
#         parse_mode="HTML"
#     )
#     await callback.answer()

@dp.message()
async def fallback_message_handler(message: types.Message):
    # если пользователь ожидает код, у тебя уже есть логика PENDING_OFFER — она сработает,
    # потому что этот handler будет вызван и для обычных текстов.
    await log_event(message.from_user, "FALLBACK_MSG", message.text or "")
    # не обязательно отвечать автоматически — но можно отправить подсказку:
    # await message.reply("Я получил сообщение. Если ты вводишь код — пиши его, иначе используй меню.")

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

# Регистрируем middleware (важно: до старта polling)
dp.message.middleware(LoggingMiddleware())
dp.callback_query.middleware(LoggingMiddleware())

# Main
async def main():
    logger.info("Запуск бота...")
    ok = await init_google_sheets()
    if not ok:
        logger.error("Не удалось инициализировать Google Sheets. Бот будет работать, но без записи.")
    # старт веб-сервера для Render healthcheck
    asyncio.create_task(start_web_server())

    logger.info("Начинаем polling...")
    await dp.start_polling(
        bot,
        drop_pending_updates=True,
        allowed_updates=["message", "callback_query"]
    )

if __name__ == "__main__":
    asyncio.run(main())
