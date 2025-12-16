import asyncio
import logging
from decimal import Decimal
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
import sqlite3
from datetime import datetime
import uuid
from aiocryptopay import AioCryptoPay, Networks
import os
from dotenv import load_dotenv
from aiohttp import web
import socket

# Загружаем переменные из .env файла
load_dotenv()

# ========== КОНФИГУРАЦИЯ ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
router = Router()

# Загрузка конфигурации
TOKEN = os.getenv('BOT_TOKEN')
CRYPTO_PAY_TOKEN = os.getenv('CRYPTO_PAY_TOKEN')
USE_TESTNET = os.getenv('USE_TESTNET')
PORT = int(os.getenv('PORT', 8080))  # Порт для HTTP-сервера

# Проверка обязательных переменных
if not TOKEN or not CRYPTO_PAY_TOKEN:
    logger.error("❌ Ошибка: BOT_TOKEN или CRYPTO_PAY_TOKEN не установлены в .env файле")
    raise ValueError("Необходимо установить BOT_TOKEN и CRYPTO_PAY_TOKEN в .env файле")

# Лимиты
USDT_MAX_LIMIT = Decimal('0.5')
COMMISSION = Decimal('0.05')

# Валюты (теперь только исходные, целевая всегда USDT)
CRYPTO_ASSETS = {
    'BTC': {'name': 'Bitcoin', 'decimals': 6},
    'ETH': {'name': 'Ethereum', 'decimals': 5},
    'SOL': {'name': 'Solana', 'decimals': 3},
    'TON': {'name': 'Toncoin', 'decimals': 3},
    'NOT': {'name': 'Notcoin', 'decimals': 0},
    # USDT убрали, так как обмен только на USDT
}

# Инициализация Crypto Pay API
crypto_pay = AioCryptoPay(
    token=CRYPTO_PAY_TOKEN,
    network=Networks.MAIN_NET
)

# ========== HTTP СЕРВЕР ДЛЯ CRON/PING ==========
async def handle_health(request):
    """Обработчик для health check"""
    return web.Response(text="OK")

async def handle_root(request):
    """Обработчик для корневого пути"""
    return web.json_response({
        "status": "online",
        "service": "Crypto Exchange Bot",
        "timestamp": datetime.now().isoformat(),
        "endpoints": {
            "health": "/health",
            "status": "/status"
        }
    })

async def handle_status(request):
    """Обработчик для проверки статуса бота"""
    try:
        conn = sqlite3.connect("crypto_exchange.db")
        cursor = conn.cursor()
        
        # Получаем статистику
        cursor.execute('SELECT COUNT(*) FROM users')
        users_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM exchanges')
        exchanges_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM exchanges WHERE status = 'completed'")
        completed_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM exchanges WHERE status = 'pending'")
        pending_count = cursor.fetchone()[0]
        
        conn.close()
        
        return web.json_response({
            "status": "running",
            "bot": "online",
            "database": "connected",
            "users": users_count,
            "total_exchanges": exchanges_count,
            "completed_exchanges": completed_count,
            "pending_exchanges": pending_count,
            "uptime": get_uptime(),
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return web.json_response({
            "status": "error",
            "message": str(e)
        }, status=500)

def get_uptime():
    """Возвращает время работы сервера"""
    if not hasattr(get_uptime, 'start_time'):
        get_uptime.start_time = datetime.now()
    uptime = datetime.now() - get_uptime.start_time
    days = uptime.days
    hours, remainder = divmod(uptime.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    if days > 0:
        return f"{days}d {hours}h {minutes}m"
    elif hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    else:
        return f"{minutes}m {seconds}s"

async def start_http_server():
    """Запуск HTTP сервера"""
    app = web.Application()
    
    # Добавляем маршруты
    app.router.add_get('/', handle_root)
    app.router.add_get('/health', handle_health)
    app.router.add_get('/status', handle_status)
    
    # Получаем IP-адрес для привязку
    host = '0.0.0.0'  # Слушаем все интерфейсы
    
    # Пытаемся запустить на указанном порту
    runner = web.AppRunner(app)
    await runner.setup()
    
    try:
        site = web.TCPSite(runner, host, PORT)
        await site.start()
        logger.info(f"✅ HTTP сервер запущен на http://{host}:{PORT}")
        
        # Выводим информацию о доступных эндпоинтах
        logger.info(f"📡 Доступные эндпоинты:")
        logger.info(f"   • http://{host}:{PORT}/ - информация о сервисе")
        logger.info(f"   • http://{host}:{PORT}/health - health check")
        logger.info(f"   • http://{host}:{PORT}/status - статус бота")
        
    except OSError as e:
        logger.error(f"❌ Не удалось запустить HTTP сервер на порту {PORT}: {e}")
        logger.info("Пробуем использовать случайный порт...")
        
        # Пробуем найти свободный порт
        for port in range(8080, 8100):
            try:
                site = web.TCPSite(runner, host, port)
                await site.start()
                logger.info(f"✅ HTTP сервер запущен на http://{host}:{port}")
                return port
            except OSError:
                continue
        
        logger.error("❌ Не удалось найти свободный порт для HTTP сервера")
        return None
    
    return PORT

# ========== КЭШИРОВАНИЕ КУРСОВ ==========
exchange_rates_cache = {}
cache_expiry = None
CACHE_DURATION = 300  # 5 минут в секундах

async def get_exchange_rate_with_cache(from_currency: str, to_currency: str = 'USDT') -> Decimal:
    """Получает курс обмена с кэшированием (всегда на USDT)"""
    global exchange_rates_cache, cache_expiry
    
    current_time = datetime.now().timestamp()
    
    # Если кэш устарел или пустой, обновляем
    if not cache_expiry or current_time > cache_expiry or not exchange_rates_cache:
        try:
            rates = await crypto_pay.get_exchange_rates()
            exchange_rates_cache = {}
            
            for rate in rates:
                key = f"{rate.source}_{rate.target}"
                exchange_rates_cache[key] = Decimal(str(rate.rate))
            
            cache_expiry = current_time + CACHE_DURATION
            logger.info(f"Кэш курсов обновлен. Сохранено {len(exchange_rates_cache)} курсов")
            
        except Exception as e:
            logger.error(f"Ошибка обновления кэша курсов: {e}")
            # Если не удалось обновить, используем старый кэш или возвращаем ошибку
    
    # Пытаемся найти прямой курс на USDT
    direct_key = f"{from_currency}_{to_currency}"
    if direct_key in exchange_rates_cache:
        return exchange_rates_cache[direct_key]
    
    # Пытаемся найти через BTC
    if (f"{from_currency}_BTC" in exchange_rates_cache and 
        f"BTC_{to_currency}" in exchange_rates_cache):
        return (exchange_rates_cache[f"{from_currency}_BTC"] * 
                exchange_rates_cache[f"BTC_{to_currency}"])
    
    # Пытаемся найти через TON
    if (f"{from_currency}_TON" in exchange_rates_cache and 
        f"TON_{to_currency}" in exchange_rates_cache):
        return (exchange_rates_cache[f"{from_currency}_TON"] * 
                exchange_rates_cache[f"TON_{to_currency}"])
    
    return None

# ========== СОСТОЯНИЯ FSM ==========
class ExchangeStates(StatesGroup):
    choosing_from_currency = State()  # Шаг 1: Выбор валюты для обмена
    entering_amount = State()         # Шаг 2: Ввод суммы
    confirming_exchange = State()     # Шаг 3: Подтверждение

# ========== БАЗА ДАННЫХ SQLite ==========
class Database:
    def __init__(self, db_file="crypto_exchange.db"):
        self.db_file = db_file
        self.create_tables()
    
    def get_connection(self):
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        return conn
    
    def create_tables(self):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER UNIQUE NOT NULL,
                    username TEXT,
                    full_name TEXT,
                    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS exchanges (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    exchange_id TEXT UNIQUE NOT NULL,
                    from_currency TEXT NOT NULL,
                    to_currency TEXT DEFAULT 'USDT',
                    amount TEXT NOT NULL,
                    commission TEXT NOT NULL,
                    commission_usdt TEXT NOT NULL,
                    final_amount TEXT NOT NULL,
                    amount_usdt TEXT,
                    invoice_id INTEGER,
                    invoice_url TEXT,
                    check_id INTEGER,
                    check_url TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    paid_at TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (id)
                )
            ''')
            
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_exchanges_user_id ON exchanges(user_id)')
            
            conn.commit()
            conn.close()
            logger.info("База данных инициализирована")
        except Exception as e:
            logger.error(f"Ошибка создания таблиц: {e}")

db = Database()

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
def get_currency_keyboard():
    """Создает клавиатуру для выбора исходной валюты"""
    keyboard = []
    row = []
    for currency_code, currency_info in CRYPTO_ASSETS.items():
        button_text = f"{currency_info['name']} ({currency_code}) → USDT"
        callback_data = f"from_currency:{currency_code}"
        row.append(InlineKeyboardButton(text=button_text, callback_data=callback_data))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def format_amount(amount: Decimal, currency: str) -> str:
    decimals = CRYPTO_ASSETS[currency]['decimals']
    return f"{amount:.{decimals}f}"

def get_or_create_user(telegram_id: int, username: str, full_name: str) -> int:
    conn = db.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('SELECT id FROM users WHERE telegram_id = ?', (telegram_id,))
    user = cursor.fetchone()
    
    if user:
        user_id = user['id']
    else:
        cursor.execute('''
            INSERT INTO users (telegram_id, username, full_name)
            VALUES (?, ?, ?)
        ''', (telegram_id, username, full_name))
        user_id = cursor.lastrowid
    
    conn.commit()
    conn.close()
    return user_id

async def validate_usdt_limit(amount: Decimal, currency: str) -> tuple:
    """Проверяет лимит 0.5 USDT и возвращает курс"""
    try:
        # Получаем курс через кэш
        rate_to_usdt = await get_exchange_rate_with_cache(currency)
        
        if not rate_to_usdt:
            # В тестовом режиме используем фиксированные курсы
            if USE_TESTNET:
                logger.warning(f"Курс {currency}/USDT не найден в кэше, используем тестовые значения")
                
                test_rates = {
                    'BTC': Decimal('30000'),
                    'ETH': Decimal('2000'),
                    'TON': Decimal('2'),
                    'SOL': Decimal('100'),
                    'NOT': Decimal('0.006'),
                }
                
                rate_to_usdt = test_rates.get(currency)
            
            if not rate_to_usdt:
                return False, f"Не удалось получить курс {currency}/USDT", Decimal('0'), Decimal('0')
        
        # Конвертируем сумму в USDT
        amount_usdt = amount * rate_to_usdt
        
        # Рассчитываем максимальную сумму в выбранной валюте
        max_amount_in_currency = USDT_MAX_LIMIT / rate_to_usdt
        
        # Проверяем минимум $0.01
        if amount_usdt < Decimal('0.01'):
            return False, f"Минимальная сумма: {format_amount(Decimal('0.01') / rate_to_usdt, currency)} {currency} ($0.01 USDT)", amount_usdt, rate_to_usdt, max_amount_in_currency
        
        # Проверяем лимит 0.5 USDT
        if amount_usdt > USDT_MAX_LIMIT:
            return False, f"Максимальная сумма: {format_amount(max_amount_in_currency, currency)} {currency} ({USDT_MAX_LIMIT} USDT)", amount_usdt, rate_to_usdt, max_amount_in_currency
        
        # Логируем для отладки
        logger.info(f"Курс {currency}/USDT: {rate_to_usdt}, сумма {amount} {currency} = {amount_usdt:.4f} USDT")
        
        return True, "", amount_usdt, rate_to_usdt, max_amount_in_currency
        
    except Exception as e:
        logger.error(f"Ошибка проверки лимита USDT: {e}")
        return False, "Ошибка проверки суммы", Decimal('0'), Decimal('0'), Decimal('0')

# ========== КОМАНДА ДЛЯ ПРОВЕРКИ КУРСОВ ==========
@router.message(Command("rates"))
async def cmd_rates(message: Message):
    """Показывает курсы всех валют к USDT"""
    try:
        # Получаем курсы из кэша или API
        rates_text = "📈 <b>Курсы валют к USDT:</b>\n\n"
        
        for currency_code in CRYPTO_ASSETS.keys():
            rate = await get_exchange_rate_with_cache(currency_code)
            if rate:
                # Рассчитываем максимальную сумму в валюте
                max_in_currency = USDT_MAX_LIMIT / rate
                rates_text += f"<b>{currency_code}</b> → USDT: {rate}\n"
                rates_text += f"   Макс. сумма: {format_amount(max_in_currency, currency_code)} {currency_code}\n\n"
            else:
                rates_text += f"<b>{currency_code}</b> → USDT: не доступен\n\n"
        
        rates_text += f"💡 <b>Общий лимит:</b> {USDT_MAX_LIMIT} USDT\n"
        rates_text += f"💸 <b>Комиссия:</b> {COMMISSION * 100}%"
        
        await message.answer(rates_text, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Ошибка получения курсов: {e}")
        await message.answer("❌ Ошибка получения курсов обмена")

# ========== ОБРАБОТЧИКИ КОМАНД ==========
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    
    get_or_create_user(
        message.from_user.id,
        message.from_user.username,
        message.from_user.full_name
    )
    
    welcome_text = f"""
👋 Добро пожаловать в FlipExchange Bot!

💰 <b>Теперь бот работает только на обмен в USDT!</b>
• Вы можете обменять BTC, ETH, SOL, TON, NOT на USDT
• Максимальная сумма обмена: <b>{USDT_MAX_LIMIT} USDT</b>
• Комиссия за обмен: <b>{COMMISSION * 100}%</b>

💱 <b>Доступные валюты для обмена на USDT:</b>
• Bitcoin (BTC)
• Ethereum (ETH)  
• Solana (SOL)
• Toncoin (TON)
• Notcoin (NOT)

📊 <b>Максимальные суммы в валютах:</b>
• BTC: ~0.000016 BTC
• TON: ~0.25 TON
• NOT: ~83 NOT

📈 <b>Как это работает:</b>
1. Выбираете валюту для обмена на USDT
2. Вводите сумму (конвертируется в USDT, лимит {USDT_MAX_LIMIT} USDT)
3. Оплачиваете счет
4. Получаете чек в USDT

Чтобы начать, нажмите /exchange
Для проверки статуса: /status
Для проверки курсов: /rates
Для отмены: /cancel
    """
    
    await message.answer(welcome_text, parse_mode="HTML")

@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("✅ Текущая операция отменена. Нажмите /exchange для нового обмена.")

@router.message(Command("exchange"))
async def cmd_exchange(message: Message, state: FSMContext):
    await message.answer(
        "🔄 Выберите валюту, которую хотите обменять на USDT:",
        reply_markup=get_currency_keyboard()
    )
    await state.set_state(ExchangeStates.choosing_from_currency)

@router.message(Command("status"))
async def cmd_status(message: Message):
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT id FROM users WHERE telegram_id = ?', (message.from_user.id,))
        user = cursor.fetchone()
        
        if user:
            user_id = user['id']
            cursor.execute('''
                SELECT * FROM exchanges 
                WHERE user_id = ? 
                ORDER BY created_at DESC 
                LIMIT 1
            ''', (user_id,))
            exchange = cursor.fetchone()
            
            if exchange:
                status_text = f"""
📋 <b>Статус вашего обмена:</b>
ID: {exchange['exchange_id']}
Создан: {exchange['created_at']}
Статус: {exchange['status']}
Отдаете: {format_amount(Decimal(exchange['amount']), exchange['from_currency'])} {exchange['from_currency']}
Получаете: {format_amount(Decimal(exchange['final_amount']), 'USDT')} USDT
Комиссия: {format_amount(Decimal(exchange['commission']), exchange['from_currency'])} {exchange['from_currency']}
                """
                if exchange['check_url']:
                    status_text += f"\n📄 Чек: {exchange['check_url']}"
                
                keyboard = None
                if exchange['status'] == 'pending' and exchange['invoice_url']:
                    keyboard = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="💳 Оплатить счет", url=exchange['invoice_url'])],
                        [InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"check_payment:{exchange['id']}")]
                    ])
                
                await message.answer(status_text, parse_mode="HTML", reply_markup=keyboard)
            else:
                await message.answer("У вас нет активных обменов.")
        else:
            await message.answer("Вы не зарегистрированы. Нажмите /start")
        
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка проверки статуса: {e}")
        await message.answer("Ошибка при проверке статуса.")

# ========== ОБРАБОТЧИКИ FSM ==========
@router.callback_query(F.data.startswith("from_currency:"))
async def process_from_currency(callback: CallbackQuery, state: FSMContext):
    from_currency = callback.data.split(":")[1]
    
    # Получаем курс для расчета максимальной суммы
    rate_to_usdt = await get_exchange_rate_with_cache(from_currency)
    
    if not rate_to_usdt:
        # В тестовом режиме используем фиксированные курсы
        if USE_TESTNET:
            test_rates = {
                'BTC': Decimal('30000'),
                'ETH': Decimal('2000'),
                'TON': Decimal('2'),
                'SOL': Decimal('100'),
                'NOT': Decimal('0.006'),
            }
            rate_to_usdt = test_rates.get(from_currency, Decimal('1'))
        else:
            rate_to_usdt = Decimal('1')
    
    # Рассчитываем максимальную сумму в выбранной валюте
    max_amount_in_currency = USDT_MAX_LIMIT / rate_to_usdt
    
    await state.update_data(
        from_currency=from_currency, 
        to_currency='USDT',
        rate_to_usdt=str(rate_to_usdt),
        max_amount_in_currency=str(max_amount_in_currency)
    )
    
    await callback.message.edit_text(
        f"Вы выбрали: {CRYPTO_ASSETS[from_currency]['name']}\n\n"
        f"Введите сумму {CRYPTO_ASSETS[from_currency]['name']}, которую хотите обменять на USDT.\n"
        f"<b>Максимальная сумма: {format_amount(max_amount_in_currency, from_currency)} {from_currency} ({USDT_MAX_LIMIT} USDT)</b>\n\n"
        f"<i>Пример: {format_amount(max_amount_in_currency / Decimal('10'), from_currency)}</i>",
        parse_mode="HTML"
    )
    await state.set_state(ExchangeStates.entering_amount)
    await callback.answer()

@router.message(ExchangeStates.entering_amount, F.text)
async def process_amount(message: Message, state: FSMContext):
    try:
        # Получаем данные из состояния
        data = await state.get_data()
        from_currency = data.get('from_currency')
        max_amount_in_currency = Decimal(data.get('max_amount_in_currency', '0'))
        
        if not from_currency:
            await message.answer("❌ Ошибка: не выбрана валюта. Начните заново: /exchange")
            await state.clear()
            return
        
        # Извлекаем первое числовое значение из текста
        text = message.text.strip()
        
        # Ищем первое число в тексте (включая десятичные числа)
        import re
        # Регулярное выражение для поиска чисел с плавающей точкой
        pattern = r'[-+]?\d*\.\d+|\d+'
        matches = re.findall(pattern, text)
        
        if not matches:
            await message.answer("❌ Не найдено числовое значение. Введите сумму (например: 0.025)")
            return
        
        # Берем первое найденное число
        number_str = matches[0].replace(',', '.')
        
        try:
            amount = Decimal(number_str)
        except (ValueError, Exception):
            await message.answer("❌ Пожалуйста, введите корректное число (например: 0.025)")
            return
        
        # Проверка на положительное число
        if amount <= 0:
            await message.answer("❌ Сумма должна быть больше 0")
            return
        
        # Проверка лимита в USDT и получение курса
        is_valid, error_msg, amount_usdt, rate_to_usdt, calculated_max = await validate_usdt_limit(amount, from_currency)
        
        if not is_valid:
            await message.answer(f"❌ {error_msg}")
            return
        
        # Расчет комиссии и итоговой суммы
        # 1. Комиссия в исходной валюте
        commission_original = amount * COMMISSION
        
        # 2. Комиссия в USDT (по курсу)
        commission_usdt = commission_original * rate_to_usdt
        
        # 3. Итоговая сумма в USDT после комиссии
        final_amount_usdt = amount_usdt - commission_usdt
        
        # Сохраняем ВСЕ данные в состоянии
        await state.update_data({
            'amount': str(amount),
            'final_amount': str(final_amount_usdt),  # в USDT
            'commission_amount': str(commission_original),  # в исходной валюте
            'commission_usdt': str(commission_usdt),  # в USDT
            'amount_usdt': str(amount_usdt),
            'rate_to_usdt': str(rate_to_usdt),
            'from_currency': from_currency,
            'to_currency': 'USDT',
            'max_amount_in_currency': str(max_amount_in_currency)
        })
        
        # Формируем сообщение с подтверждением
        confirmation_text = f"""
✅ <b>Подтвердите обмен:</b>

📤 Отправляете: {format_amount(amount, from_currency)} {from_currency}
   (максимум: {format_amount(max_amount_in_currency, from_currency)} {from_currency})
   
📥 Получаете: {format_amount(final_amount_usdt, 'USDT')} USDT
💸 Комиссия ({COMMISSION * 100}%): {format_amount(commission_original, from_currency)} {from_currency}

<b>Лимит обмена: {USDT_MAX_LIMIT} USDT</b>

Всё верно?
        """
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да, продолжить", callback_data="confirm_exchange"),
                InlineKeyboardButton(text="❌ Нет, отменить", callback_data="cancel_exchange")
            ]
        ])
        
        await message.answer(confirmation_text, parse_mode="HTML", reply_markup=keyboard)
        await state.set_state(ExchangeStates.confirming_exchange)
        
    except Exception as e:
        logger.error(f"Ошибка обработки суммы: {e}")
        await message.answer("❌ Произошла ошибка. Пожалуйста, попробуйте еще раз.")

@router.callback_query(F.data == "confirm_exchange")
async def confirm_exchange(callback: CallbackQuery, state: FSMContext):
    try:
        # Получаем ВСЕ данные из состояния
        data = await state.get_data()
        
        # Проверяем наличие обязательных полей
        required_fields = ['from_currency', 'amount', 'final_amount', 'commission_amount', 'commission_usdt', 'amount_usdt']
        missing_fields = [field for field in required_fields if field not in data]
        
        if missing_fields:
            logger.error(f"Отсутствуют поля в состоянии: {missing_fields}")
            await callback.message.answer("❌ Произошла ошибка. Пожалуйста, начните обмен заново: /exchange")
            await state.clear()
            return
        
        # Получаем или создаем пользователя
        user_id = get_or_create_user(
            callback.from_user.id,
            callback.from_user.username,
            callback.from_user.full_name
        )
        
        exchange_id = str(uuid.uuid4())[:8]
        
        # Создание инвойса через Crypto Pay API (в исходной валюте)
        logger.info(f"Создание инвойса: {data['amount']} {data['from_currency']} -> {data['final_amount']} USDT")
        
        invoice = await crypto_pay.create_invoice(
            asset=data['from_currency'],
            amount=float(data['amount']),
            description=f"Обмен {data['from_currency']} на USDT",
            hidden_message=f"User {user_id} | Exchange: {exchange_id}",
            expires_in=900  # 15 минут
        )
        
        # Сохранение в базу данных
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO exchanges (
                user_id, exchange_id, from_currency, to_currency,
                amount, commission, commission_usdt, final_amount, amount_usdt,
                invoice_id, invoice_url, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            user_id, exchange_id, data['from_currency'], 'USDT',
            data['amount'], data['commission_amount'], data['commission_usdt'], 
            data['final_amount'], data['amount_usdt'],
            invoice.invoice_id, invoice.bot_invoice_url, 'pending'
        ))
        
        exchange_db_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        await state.update_data(
            exchange_db_id=exchange_db_id, 
            invoice_id=invoice.invoice_id
        )
        
        # Отправка инвойса пользователю
        invoice_text = f"""
💰 <b>Счет для оплаты</b>

ID обмена: {exchange_id}
Сумма к оплате: {format_amount(Decimal(data['amount']), data['from_currency'])} {data['from_currency']}
Получите: {format_amount(Decimal(data['final_amount']), 'USDT')} USDT
Комиссия: {format_amount(Decimal(data['commission_amount']), data['from_currency'])} {data['from_currency']}

Счет действителен 15 минут
"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Оплатить счет", url=invoice.bot_invoice_url)],
            [InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"check_payment:{exchange_db_id}")],
            [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_exchange")]
        ])
        
        await callback.message.edit_text(invoice_text, parse_mode="HTML", reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Ошибка создания инвойса: {e}", exc_info=True)
        await callback.message.answer("❌ Ошибка при создании счета. Попробуйте позже или начните заново: /exchange")
        await state.clear()
    
    await callback.answer()

@router.callback_query(F.data.startswith("check_payment:"))
async def check_payment(callback: CallbackQuery, state: FSMContext):
    exchange_db_id = int(callback.data.split(":")[1])
    
    try:
        # Получаем информацию об обмене
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM exchanges WHERE id = ?', (exchange_db_id,))
        exchange = cursor.fetchone()
        
        if not exchange:
            await callback.answer("Обмен не найден", show_alert=True)
            return
        
        # Проверяем владельца
        cursor.execute('SELECT telegram_id FROM users WHERE id = ?', (exchange['user_id'],))
        user = cursor.fetchone()
        
        if not user or user['telegram_id'] != callback.from_user.id:
            await callback.answer("Этот обмен принадлежит другому пользователю", show_alert=True)
            return
        
        # Проверяем статус инвойса
        invoices = await crypto_pay.get_invoices(invoice_ids=exchange['invoice_id'])
        if not invoices or invoices[0].status != 'paid':
            await callback.answer("Счёт ещё не оплачен или обрабатывается. Попробуйте позже.", show_alert=True)
            return
        
        # Если уже создан чек
        if exchange['check_id']:
            await callback.answer("Чек уже создан", show_alert=True)
            return
        
        # Создаем чек в USDT
        check = await crypto_pay.create_check(
            asset='USDT',
            amount=float(exchange['final_amount']),
            pin_to_user_id=callback.from_user.id
        )
        
        # Обновляем базу данных
        cursor.execute('''
            UPDATE exchanges 
            SET status = 'completed', paid_at = CURRENT_TIMESTAMP,
                check_id = ?, check_url = ?
            WHERE id = ?
        ''', (check.check_id, check.bot_check_url, exchange_db_id))
        conn.commit()
        conn.close()
        
        # Отправляем чек пользователю
        receipt_text = f"""
🎉 <b>Обмен успешно завершен!</b>

ID операции: {exchange['exchange_id']}
Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

📤 Вы отправили: {format_amount(Decimal(exchange['amount']), exchange['from_currency'])} {exchange['from_currency']}
📥 Вы получили: {format_amount(Decimal(exchange['final_amount']), 'USDT')} USDT
💸 Комиссия: {format_amount(Decimal(exchange['commission']), exchange['from_currency'])} {exchange['from_currency']}

💎 <b>Ваш чек:</b> {check.bot_check_url}
Активируйте его в @{'CryptoTestnetBot' if USE_TESTNET else 'CryptoBot'}
"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💎 Активировать чек", url=check.bot_check_url)],
            [InlineKeyboardButton(text="🔄 Новый обмен", callback_data="new_exchange")]
        ])
        
        await callback.message.edit_text(receipt_text, parse_mode="HTML", reply_markup=keyboard)
        await state.clear()
        
    except Exception as e:
        logger.error(f"Ошибка проверки оплаты: {e}", exc_info=True)
        await callback.answer("Ошибка при обработке оплаты. Попробуйте позже.", show_alert=True)
    
    await callback.answer()

@router.callback_query(F.data == "new_exchange")
async def new_exchange_callback(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await cmd_exchange(callback.message, state)
    await callback.answer()

@router.callback_query(F.data == "cancel_exchange")
async def cancel_exchange(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("❌ Обмен отменен.")
    await state.clear()
    await callback.answer()

# ========== ЗАПУСК БОТА ==========
async def main():
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    ADMIN_ID = 7511053219  # Ваш Telegram ID
    
    try:
        # Запускаем HTTP сервер в фоновом режиме
        http_port = await start_http_server()
        
        if http_port:
            logger.info(f"✅ HTTP сервер успешно запущен на порту {http_port}")
        else:
            logger.warning("⚠️ HTTP сервер не запущен, но бот продолжит работу")
        
        # Отправляем уведомление администратору о запуске
        try:
            await bot.send_message(
                ADMIN_ID,
                f"🤖 *Бот успешно запущен!*\n\n"
                f"• Режим: Только обмен на USDT\n"
                f"• HTTP сервер: {'запущен' if http_port else 'не запущен'}\n"
                f"• Порт: {http_port if http_port else 'N/A'}\n"
                f"• Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"• Режим сети: {'Testnet' if USE_TESTNET else 'Mainnet'}",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Не удалось отправить уведомление администратору: {e}")
        
        # Запускаем бота
        logger.info("🤖 Запуск Telegram бота...")
        await dp.start_polling(bot, skip_updates=True)
        
    except Exception as e:
        logger.error(f"❌ Ошибка запуска бота: {e}")
    finally:
        # Закрываем соединения при завершении
        await bot.session.close()
        logger.info("👋 Бот остановлен")

if __name__ == "__main__":
    # Создаем event loop и запускаем main
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Получен сигнал прерывания, завершаем работу...")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")



