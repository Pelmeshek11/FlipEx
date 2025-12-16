import asyncio
import logging
import sqlite3
import uuid
import os
import re
from datetime import datetime
from typing import Optional, Dict, Tuple, Any
from contextlib import closing

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiocryptopay import AioCryptoPay, Networks
from dotenv import load_dotenv
from aiohttp import web

# Загружаем переменные из .env файла
load_dotenv()

# ========== КОНФИГУРАЦИЯ ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)
router = Router()

# Загрузка конфигурации
TOKEN = os.getenv('BOT_TOKEN')
CRYPTO_PAY_TOKEN = os.getenv('CRYPTO_PAY_TOKEN')
USE_TESTNET = os.getenv('USE_TESTNET')
ADMIN_ID = int(os.getenv('ADMIN_ID', '7511053219'))
PORT = int(os.getenv('PORT', '8080'))

# Проверка обязательных переменных
if not TOKEN:
    logger.error("❌ Ошибка: BOT_TOKEN не установлен в .env файле")
    raise ValueError("Необходимо установить BOT_TOKEN в .env файле")
if not CRYPTO_PAY_TOKEN:
    logger.error("❌ Ошибка: CRYPTO_PAY_TOKEN не установлен в .env файле")
    raise ValueError("Необходимо установить CRYPTO_PAY_TOKEN в .env файле")

# Константы
USDT_MAX_LIMIT = 0.5
COMMISSION_RATE = 0.05  # 5%
MIN_USDT_AMOUNT = 0.01

# Валюты для обмена на USDT
CRYPTO_ASSETS = {
    'BTC': {'name': 'Bitcoin', 'decimals': 8},
    'ETH': {'name': 'Ethereum', 'decimals': 6},
    'SOL': {'name': 'Solana', 'decimals': 3},
    'TON': {'name': 'Toncoin', 'decimals': 3},
    'NOT': {'name': 'Notcoin', 'decimals': 0},
}

# Инициализация Crypto Pay API
crypto_pay = AioCryptoPay(
    token=CRYPTO_PAY_TOKEN,
    network=Networks.MAIN_NET
)

# ========== HTTP СЕРВЕР ДЛЯ CRON/PING ==========
class HTTPServer:
    """HTTP сервер для health checks"""
    _start_time: Optional[datetime] = None
    
    @classmethod
    def get_uptime(cls) -> str:
        """Возвращает время работы сервера"""
        if cls._start_time is None:
            cls._start_time = datetime.now()
        uptime = datetime.now() - cls._start_time
        
        days = uptime.days
        hours, remainder = divmod(uptime.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        if days > 0:
            return f"{days}d {hours}h {minutes}m"
        elif hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        else:
            return f"{minutes}m {seconds}s"

    @staticmethod
    async def handle_health(request):
        """Обработчик для health check"""
        return web.Response(text="OK")

    @staticmethod
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

    @classmethod
    async def handle_status(cls, request):
        """Обработчик для проверки статуса бота"""
        try:
            db = Database()
            stats = db.get_statistics()
            
            return web.json_response({
                "status": "running",
                "bot": "online",
                "database": "connected",
                "users": stats.get('users_count', 0),
                "total_exchanges": stats.get('exchanges_count', 0),
                "completed_exchanges": stats.get('completed_count', 0),
                "pending_exchanges": stats.get('pending_count', 0),
                "uptime": cls.get_uptime(),
                "timestamp": datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return web.json_response({
                "status": "error",
                "message": str(e)
            }, status=500)

    @classmethod
    async def start(cls):
        """Запуск HTTP сервера"""
        app = web.Application()
        
        # Добавляем маршруты
        app.router.add_get('/', cls.handle_root)
        app.router.add_get('/health', cls.handle_health)
        app.router.add_get('/status', cls.handle_status)
        
        runner = web.AppRunner(app)
        await runner.setup()
        
        host = '0.0.0.0'
        
        for port in range(PORT, PORT + 20):
            try:
                site = web.TCPSite(runner, host, port)
                await site.start()
                logger.info(f"✅ HTTP сервер запущен на http://{host}:{port}")
                logger.info(f"📡 Доступные эндпоинты:")
                logger.info(f"   • http://{host}:{port}/ - информация о сервисе")
                logger.info(f"   • http://{host}:{port}/health - health check")
                logger.info(f"   • http://{host}:{port}/status - статус бота")
                return port
            except OSError:
                continue
        
        logger.error("❌ Не удалось найти свободный порт для HTTP сервера")
        return None

# ========== КЭШИРОВАНИЕ КУРСОВ ==========
class ExchangeRateCache:
    """Кэш курсов валют"""
    def __init__(self, duration: int = 300):
        self.cache: Dict[str, float] = {}
        self.cache_expiry: Optional[float] = None
        self.duration = duration
    
    async def get_rate(self, from_currency: str, to_currency: str = 'USDT') -> Optional[float]:
        """Получает курс обмена с кэшированием"""
        current_time = datetime.now().timestamp()
        
        if not self.cache_expiry or current_time > self.cache_expiry or not self.cache:
            await self._update_cache()
        
        # Пытаемся найти прямой курс
        direct_key = f"{from_currency}_{to_currency}"
        if direct_key in self.cache:
            return self.cache[direct_key]
        
        # Пытаемся найти через BTC
        if (f"{from_currency}_BTC" in self.cache and 
            f"BTC_{to_currency}" in self.cache):
            return self.cache[f"{from_currency}_BTC"] * self.cache[f"BTC_{to_currency}"]
        
        # Пытаемся найти через TON
        if (f"{from_currency}_TON" in self.cache and 
            f"TON_{to_currency}" in self.cache):
            return self.cache[f"{from_currency}_TON"] * self.cache[f"TON_{to_currency}"]
        
        return None
    
    async def _update_cache(self):
        """Обновляет кэш курсов"""
        try:
            rates = await crypto_pay.get_exchange_rates()
            self.cache.clear()
            
            for rate in rates:
                key = f"{rate.source}_{rate.target}"
                try:
                    self.cache[key] = float(rate.rate)
                except (ValueError, TypeError):
                    continue
            
            self.cache_expiry = datetime.now().timestamp() + self.duration
            logger.info(f"Кэш курсов обновлен. Сохранено {len(self.cache)} курсов")
            
        except Exception as e:
            logger.error(f"Ошибка обновления кэша курсов: {e}")

# ========== СОСТОЯНИЯ FSM ==========
class ExchangeStates(StatesGroup):
    choosing_from_currency = State()
    entering_amount = State()
    confirming_exchange = State()

# ========== БАЗА ДАННЫХ SQLite ==========
class Database:
    """Класс для работы с базой данных"""
    def __init__(self, db_file: str = "crypto_exchange.db"):
        self.db_file = db_file
        self._create_tables()
    
    def _create_tables(self):
        """Создает таблицы в базе данных"""
        try:
            with closing(sqlite3.connect(self.db_file)) as conn:
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
                        amount REAL NOT NULL,
                        commission REAL NOT NULL,
                        commission_usdt REAL NOT NULL,
                        final_amount REAL NOT NULL,
                        amount_usdt REAL,
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
                
                # Создаем индексы
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_users_telegram_id 
                    ON users(telegram_id)
                ''')
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_exchanges_user_id 
                    ON exchanges(user_id)
                ''')
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_exchanges_status 
                    ON exchanges(status)
                ''')
                
                conn.commit()
                logger.info("База данных инициализирована")
        except Exception as e:
            logger.error(f"Ошибка создания таблиц: {e}")
            raise
    
    def get_connection(self):
        """Возвращает соединение с базой данных"""
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        return conn
    
    def get_or_create_user(self, telegram_id: int, username: str, full_name: str) -> int:
        """Создает или получает пользователя"""
        with closing(self.get_connection()) as conn:
            cursor = conn.cursor()
            
            cursor.execute(
                'SELECT id FROM users WHERE telegram_id = ?',
                (telegram_id,)
            )
            user = cursor.fetchone()
            
            if user:
                return user['id']
            else:
                cursor.execute('''
                    INSERT INTO users (telegram_id, username, full_name)
                    VALUES (?, ?, ?)
                ''', (telegram_id, username or '', full_name or ''))
                conn.commit()
                return cursor.lastrowid
    
    def save_exchange(self, exchange_data: dict) -> int:
        """Сохраняет обмен в базу данных"""
        with closing(self.get_connection()) as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO exchanges (
                    user_id, exchange_id, from_currency, to_currency,
                    amount, commission, commission_usdt, final_amount, amount_usdt,
                    invoice_id, invoice_url, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                exchange_data['user_id'],
                exchange_data['exchange_id'],
                exchange_data['from_currency'],
                exchange_data['to_currency'],
                exchange_data['amount'],
                exchange_data['commission'],
                exchange_data['commission_usdt'],
                exchange_data['final_amount'],
                exchange_data['amount_usdt'],
                exchange_data['invoice_id'],
                exchange_data['invoice_url'],
                exchange_data['status']
            ))
            
            conn.commit()
            return cursor.lastrowid
    
    def update_exchange_with_check(self, exchange_id: int, check_data: dict):
        """Обновляет обмен данными чека"""
        with closing(self.get_connection()) as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE exchanges 
                SET status = 'completed', paid_at = CURRENT_TIMESTAMP,
                    check_id = ?, check_url = ?
                WHERE id = ?
            ''', (
                check_data['check_id'],
                check_data['check_url'],
                exchange_id
            ))
            
            conn.commit()
    
    def get_user_exchanges(self, user_id: int, limit: int = 10):
        """Получает обмены пользователя"""
        with closing(self.get_connection()) as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM exchanges 
                WHERE user_id = ? 
                ORDER BY created_at DESC 
                LIMIT ?
            ''', (user_id, limit))
            
            return cursor.fetchall()
    
    def get_statistics(self) -> dict:
        """Возвращает статистику бота"""
        with closing(self.get_connection()) as conn:
            cursor = conn.cursor()
            
            stats = {}
            
            # Количество пользователей
            cursor.execute('SELECT COUNT(*) FROM users')
            stats['users_count'] = cursor.fetchone()[0]
            
            # Общее количество обменов
            cursor.execute('SELECT COUNT(*) FROM exchanges')
            stats['exchanges_count'] = cursor.fetchone()[0]
            
            # Завершенные обмены
            cursor.execute("SELECT COUNT(*) FROM exchanges WHERE status = 'completed'")
            stats['completed_count'] = cursor.fetchone()[0]
            
            # Ожидающие обмены
            cursor.execute("SELECT COUNT(*) FROM exchanges WHERE status = 'pending'")
            stats['pending_count'] = cursor.fetchone()[0]
            
            return stats

# Инициализируем кэш и базу данных
rate_cache = ExchangeRateCache()
db = Database()

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
def format_amount(amount: float, currency: str) -> str:
    """Форматирует сумму с учетом точности валюты"""
    if currency not in CRYPTO_ASSETS:
        return f"{amount:.8f}"
    
    decimals = CRYPTO_ASSETS[currency]['decimals']
    
    # Для 0 знаков после запятой
    if decimals == 0:
        return f"{int(amount)}"
    
    # Для положительного количества знаков
    format_str = f"{{:.{decimals}f}}"
    return format_str.format(amount).rstrip('0').rstrip('.')

def extract_number(text: str) -> Optional[float]:
    """Извлекает число из текста"""
    # Убираем пробелы и заменяем запятые на точки
    text = text.strip().replace(',', '.')
    
    # Регулярное выражение для поиска чисел
    pattern = r'[-+]?\d*\.?\d+'
    matches = re.findall(pattern, text)
    
    if not matches:
        return None
    
    try:
        return float(matches[0])
    except (ValueError, TypeError):
        return None

async def validate_exchange_amount(
    amount: float, 
    currency: str
) -> Tuple[bool, str, Optional[float], Optional[float], Optional[float]]:
    """
    Проверяет сумму обмена
    
    Возвращает:
    (is_valid, error_message, amount_usdt, rate, max_amount_in_currency)
    """
    try:
        if amount <= 0:
            return False, "Сумма должна быть больше 0", None, None, None
        
        # Получаем курс
        rate = await rate_cache.get_rate(currency)
        
        if not rate:
            # В тестовом режиме используем фиксированные курсы
            if USE_TESTNET:
                test_rates = {
                    'BTC': 30000.0,
                    'ETH': 2000.0,
                    'TON': 2.0,
                    'SOL': 100.0,
                    'NOT': 0.006,
                }
                rate = test_rates.get(currency)
            
            if not rate or rate <= 0:
                return False, f"Не удалось получить курс {currency}/USDT", None, None, None
        
        # Конвертируем в USDT
        amount_usdt = amount * rate
        
        # Рассчитываем максимальную сумму в выбранной валюте
        max_amount_in_currency = USDT_MAX_LIMIT / rate if rate > 0 else 0
        
        # Проверяем минимум
        if amount_usdt < MIN_USDT_AMOUNT:
            min_amount = MIN_USDT_AMOUNT / rate
            return False, (
                f"Минимальная сумма: {format_amount(min_amount, currency)} {currency} "
                f"(${MIN_USDT_AMOUNT:.2f} USDT)"
            ), amount_usdt, rate, max_amount_in_currency
        
        # Проверяем лимит
        if amount_usdt > USDT_MAX_LIMIT:
            return False, (
                f"Максимальная сумма: {format_amount(max_amount_in_currency, currency)} {currency} "
                f"(${USDT_MAX_LIMIT:.2f} USDT)"
            ), amount_usdt, rate, max_amount_in_currency
        
        logger.info(
            f"Курс {currency}/USDT: {rate}, "
            f"сумма {amount} {currency} = {amount_usdt:.4f} USDT"
        )
        
        return True, "", amount_usdt, rate, max_amount_in_currency
        
    except ZeroDivisionError:
        return False, "Ошибка: курс равен нулю", None, None, None
    except Exception as e:
        logger.error(f"Ошибка проверки суммы: {e}")
        return False, "Ошибка проверки суммы", None, None, None

def get_currency_keyboard() -> InlineKeyboardMarkup:
    """Создает клавиатуру для выбора валюты"""
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

async def send_admin_notification(bot: Bot, message: str):
    """Отправляет уведомление администратору"""
    try:
        await bot.send_message(ADMIN_ID, message, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Не удалось отправить уведомление администратору: {e}")

# ========== ОБРАБОТЧИКИ КОМАНД ==========
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    """Обработчик команды /start"""
    await state.clear()
    
    db.get_or_create_user(
        message.from_user.id,
        message.from_user.username,
        message.from_user.full_name
    )
    
    welcome_text = f"""
👋 Добро пожаловать в FlipExchange Bot!

💰 <b>Теперь бот работает только на обмен в USDT!</b>
• Вы можете обменять BTC, ETH, SOL, TON, NOT на USDT
• Максимальная сумма обмена: <b>{USDT_MAX_LIMIT} USDT</b>
• Комиссия за обмен: <b>{COMMISSION_RATE * 100:.1f}%</b>

💱 <b>Доступные валюты для обмена на USDT:</b>
• Bitcoin (BTC)
• Ethereum (ETH)  
• Solana (SOL)
• Toncoin (TON)
• Notcoin (NOT)

📊 <b>Минимальная сумма:</b> ${MIN_USDT_AMOUNT:.2f} USDT

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
    """Обработчик команды /cancel"""
    await state.clear()
    await message.answer("✅ Текущая операция отменена. Нажмите /exchange для нового обмена.")

@router.message(Command("exchange"))
async def cmd_exchange(message: Message, state: FSMContext):
    """Обработчик команды /exchange"""
    await message.answer(
        "🔄 Выберите валюту, которую хотите обменять на USDT:",
        reply_markup=get_currency_keyboard()
    )
    await state.set_state(ExchangeStates.choosing_from_currency)

@router.message(Command("status"))
async def cmd_status(message: Message):
    """Обработчик команды /status"""
    try:
        user_id = db.get_or_create_user(
            message.from_user.id,
            message.from_user.username,
            message.from_user.full_name
        )
        
        exchanges = db.get_user_exchanges(user_id, limit=1)
        
        if not exchanges:
            await message.answer("У вас нет активных обменов.")
            return
        
        exchange = exchanges[0]
        
        status_text = f"""
📋 <b>Статус вашего обмена:</b>
ID: {exchange['exchange_id']}
Создан: {exchange['created_at']}
Статус: {exchange['status']}
Отдаете: {format_amount(exchange['amount'], exchange['from_currency'])} {exchange['from_currency']}
Получаете: {format_amount(exchange['final_amount'], 'USDT')} USDT
Комиссия: {format_amount(exchange['commission'], exchange['from_currency'])} {exchange['from_currency']}
        """
        
        keyboard = None
        if exchange['status'] == 'pending' and exchange['invoice_url']:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💳 Оплатить счет", url=exchange['invoice_url'])],
                [InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"check_payment:{exchange['id']}")]
            ])
        
        await message.answer(status_text, parse_mode="HTML", reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Ошибка проверки статуса: {e}")
        await message.answer("❌ Ошибка при проверке статуса.")

@router.message(Command("rates"))
async def cmd_rates(message: Message):
    """Показывает курсы всех валют к USDT"""
    try:
        rates_text = "📈 <b>Курсы валют к USDT:</b>\n\n"
        
        for currency_code in CRYPTO_ASSETS.keys():
            rate = await rate_cache.get_rate(currency_code)
            if rate and rate > 0:
                max_in_currency = USDT_MAX_LIMIT / rate
                rates_text += f"<b>{currency_code}</b> → USDT: {rate:.8f}\n"
                rates_text += f"   Макс. сумма: {format_amount(max_in_currency, currency_code)} {currency_code}\n\n"
            else:
                rates_text += f"<b>{currency_code}</b> → USDT: не доступен\n\n"
        
        rates_text += f"💡 <b>Общий лимит:</b> {USDT_MAX_LIMIT:.2f} USDT\n"
        rates_text += f"💸 <b>Комиссия:</b> {COMMISSION_RATE * 100:.1f}%\n"
        rates_text += f"📊 <b>Минимальная сумма:</b> ${MIN_USDT_AMOUNT:.2f} USDT"
        
        await message.answer(rates_text, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Ошибка получения курсов: {e}")
        await message.answer("❌ Ошибка получения курсов обмена")

# ========== ОБРАБОТЧИКИ FSM ==========
@router.callback_query(F.data.startswith("from_currency:"))
async def process_from_currency(callback: CallbackQuery, state: FSMContext):
    """Обработчик выбора валюты"""
    from_currency = callback.data.split(":")[1]
    
    if from_currency not in CRYPTO_ASSETS:
        await callback.answer("❌ Неизвестная валюта")
        return
    
    # Получаем курс для расчета максимальной суммы
    rate = await rate_cache.get_rate(from_currency)
    
    if not rate or rate <= 0:
        if USE_TESTNET:
            test_rates = {
                'BTC': 30000.0,
                'ETH': 2000.0,
                'TON': 2.0,
                'SOL': 100.0,
                'NOT': 0.006,
            }
            rate = test_rates.get(from_currency, 1.0)
        else:
            rate = 1.0
    
    # Рассчитываем максимальную сумму в выбранной валюте
    max_amount_in_currency = USDT_MAX_LIMIT / rate if rate > 0 else 0
    
    await state.update_data(
        from_currency=from_currency,
        to_currency='USDT',
        rate=rate,
        max_amount_in_currency=max_amount_in_currency
    )
    
    currency_info = CRYPTO_ASSETS[from_currency]
    
    await callback.message.edit_text(
        f"Вы выбрали: {currency_info['name']}\n\n"
        f"Введите сумму {currency_info['name']}, которую хотите обменять на USDT.\n"
        f"<b>Максимальная сумма: {format_amount(max_amount_in_currency, from_currency)} {from_currency} "
        f"({USDT_MAX_LIMIT:.2f} USDT)</b>\n\n"
        f"<i>Пример: {format_amount(max_amount_in_currency / 10, from_currency)}</i>",
        parse_mode="HTML"
    )
    await state.set_state(ExchangeStates.entering_amount)
    await callback.answer()

@router.message(ExchangeStates.entering_amount, F.text)
async def process_amount(message: Message, state: FSMContext):
    """Обработчик ввода суммы"""
    try:
        # Получаем данные из состояния
        data = await state.get_data()
        from_currency = data.get('from_currency')
        
        if not from_currency:
            await message.answer("❌ Ошибка: не выбрана валюта. Начните заново: /exchange")
            await state.clear()
            return
        
        # Извлекаем число из текста
        amount = extract_number(message.text)
        
        if amount is None:
            await message.answer("❌ Пожалуйста, введите корректное число (например: 0.025)")
            return
        
        # Проверка на положительное число
        if amount <= 0:
            await message.answer("❌ Сумма должна быть больше 0")
            return
        
        # Проверка лимита
        is_valid, error_msg, amount_usdt, rate, max_amount = await validate_exchange_amount(
            amount, from_currency
        )
        
        if not is_valid:
            await message.answer(f"❌ {error_msg}")
            return
        
        # Расчет комиссии и итоговой суммы
        commission_original = amount * COMMISSION_RATE
        commission_usdt = commission_original * rate if rate else 0
        final_amount_usdt = amount_usdt - commission_usdt if amount_usdt else 0
        
        # Сохраняем данные в состоянии
        await state.update_data({
            'amount': amount,
            'final_amount': final_amount_usdt,
            'commission_amount': commission_original,
            'commission_usdt': commission_usdt,
            'amount_usdt': amount_usdt,
            'rate': rate,
            'max_amount_in_currency': max_amount
        })
        
        # Формируем сообщение с подтверждением
        confirmation_text = f"""
✅ <b>Подтвердите обмен:</b>

📤 Отправляете: {format_amount(amount, from_currency)} {from_currency}
   (максимум: {format_amount(max_amount, from_currency)} {from_currency})
   
📥 Получаете: {format_amount(final_amount_usdt, 'USDT')} USDT
💸 Комиссия ({COMMISSION_RATE * 100:.1f}%): {format_amount(commission_original, from_currency)} {from_currency}

<b>Лимит обмена: {USDT_MAX_LIMIT:.2f} USDT</b>

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
    """Обработчик подтверждения обмена"""
    try:
        data = await state.get_data()
        
        # Проверяем наличие обязательных полей
        required_fields = [
            'from_currency', 'amount', 'final_amount', 
            'commission_amount', 'commission_usdt', 'amount_usdt'
        ]
        
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            logger.error(f"Отсутствуют поля в состоянии: {missing_fields}")
            await callback.message.answer("❌ Произошла ошибка. Пожалуйста, начните обмен заново: /exchange")
            await state.clear()
            return
        
        # Создаем или получаем пользователя
        user_id = db.get_or_create_user(
            callback.from_user.id,
            callback.from_user.username,
            callback.from_user.full_name
        )
        
        exchange_id = str(uuid.uuid4())[:8]
        
        # Создание инвойса через Crypto Pay API
        logger.info(f"Создание инвойса: {data['amount']} {data['from_currency']} -> {data['final_amount']} USDT")
        
        try:
            invoice = await crypto_pay.create_invoice(
                asset=data['from_currency'],
                amount=float(data['amount']),
                description=f"Обмен {data['from_currency']} на USDT",
                hidden_message=f"User {user_id} | Exchange: {exchange_id}",
                expires_in=900  # 15 минут
            )
        except Exception as e:
            logger.error(f"Ошибка создания инвойса: {e}")
            await callback.message.answer("❌ Ошибка при создании счета. Попробуйте позже.")
            return
        
        # Сохранение в базу данных
        exchange_data = {
            'user_id': user_id,
            'exchange_id': exchange_id,
            'from_currency': data['from_currency'],
            'to_currency': 'USDT',
            'amount': float(data['amount']),
            'commission': float(data['commission_amount']),
            'commission_usdt': float(data['commission_usdt']),
            'final_amount': float(data['final_amount']),
            'amount_usdt': float(data['amount_usdt']),
            'invoice_id': invoice.invoice_id,
            'invoice_url': invoice.bot_invoice_url,
            'status': 'pending'
        }
        
        exchange_db_id = db.save_exchange(exchange_data)
        
        await state.update_data(
            exchange_db_id=exchange_db_id,
            invoice_id=invoice.invoice_id
        )
        
        # Отправка инвойса пользователю
        invoice_text = f"""
💰 <b>Счет для оплаты</b>

ID обмена: {exchange_id}
Сумма к оплате: {format_amount(data['amount'], data['from_currency'])} {data['from_currency']}
Получите: {format_amount(data['final_amount'], 'USDT')} USDT
Комиссия: {format_amount(data['commission_amount'], data['from_currency'])} {data['from_currency']}

Счет действителен 15 минут
"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Оплатить счет", url=invoice.bot_invoice_url)],
            [InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"check_payment:{exchange_db_id}")],
            [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_exchange")]
        ])
        
        await callback.message.edit_text(invoice_text, parse_mode="HTML", reply_markup=keyboard)
        
        # Уведомляем администратора
        await send_admin_notification(
            callback.bot,
            f"💎 *Новый обмен создан!*\n\n"
            f"• Пользователь: @{callback.from_user.username or 'N/A'}\n"
            f"• ID обмена: {exchange_id}\n"
            f"• Сумма: {format_amount(data['amount'], data['from_currency'])} {data['from_currency']}\n"
            f"• В USDT: {data['final_amount']:.4f}\n"
            f"• Время: {datetime.now().strftime('%H:%M:%S')}"
        )
        
    except Exception as e:
        logger.error(f"Ошибка подтверждения обмена: {e}", exc_info=True)
        await callback.message.answer("❌ Ошибка при создании счета. Попробуйте позже или начните заново: /exchange")
        await state.clear()
    
    await callback.answer()

@router.callback_query(F.data.startswith("check_payment:"))
async def check_payment(callback: CallbackQuery, state: FSMContext):
    """Проверка оплаты инвойса"""
    try:
        exchange_db_id = int(callback.data.split(":")[1])
        
        # Проверяем статус инвойса
        data = await state.get_data()
        invoice_id = data.get('invoice_id')
        
        if not invoice_id:
            await callback.answer("❌ Информация об обмене не найдена", show_alert=True)
            return
        
        # Получаем информацию об инвойсе
        try:
            invoices = await crypto_pay.get_invoices(invoice_ids=invoice_id)
            if not invoices:
                await callback.answer("❌ Счет не найден", show_alert=True)
                return
            
            invoice = invoices[0]
            
            if invoice.status != 'paid':
                await callback.answer("⚠️ Счёт ещё не оплачен или обрабатывается. Попробуйте позже.", show_alert=True)
                return
        except Exception as e:
            logger.error(f"Ошибка проверки инвойса: {e}")
            await callback.answer("❌ Ошибка при проверке оплаты", show_alert=True)
            return
        
        # Создаем чек в USDT
        try:
            data = await state.get_data()
            check_amount = data.get('final_amount', 0)
            
            check = await crypto_pay.create_check(
                asset='USDT',
                amount=float(check_amount),
                pin_to_user_id=callback.from_user.id
            )
        except Exception as e:
            logger.error(f"Ошибка создания чека: {e}")
            await callback.answer("❌ Ошибка при создании чека", show_alert=True)
            return
        
        # Обновляем базу данных
        db.update_exchange_with_check(exchange_db_id, {
            'check_id': check.check_id,
            'check_url': check.bot_check_url
        })
        
        # Отправляем чек пользователю
        receipt_text = f"""
🎉 <b>Обмен успешно завершен!</b>

ID операции: {data.get('exchange_id', 'N/A')}
Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

📤 Вы отправили: {format_amount(data.get('amount', 0), data.get('from_currency', ''))} {data.get('from_currency', '')}
📥 Вы получили: {format_amount(data.get('final_amount', 0), 'USDT')} USDT
💸 Комиссия: {format_amount(data.get('commission_amount', 0), data.get('from_currency', ''))} {data.get('from_currency', '')}

💎 <b>Ваш чек:</b> {check.bot_check_url}
Активируйте его в @{'CryptoTestnetBot' if USE_TESTNET else 'CryptoBot'}
"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💎 Активировать чек", url=check.bot_check_url)],
            [InlineKeyboardButton(text="🔄 Новый обмен", callback_data="new_exchange")]
        ])
        
        await callback.message.edit_text(receipt_text, parse_mode="HTML", reply_markup=keyboard)
        await state.clear()
        
        # Уведомляем администратора
        await send_admin_notification(
            callback.bot,
            f"✅ *Обмен успешно завершен!*\n\n"
            f"• ID обмена: {data.get('exchange_id', 'N/A')}\n"
            f"• Пользователь: @{callback.from_user.username or 'N/A'}\n"
            f"• Сумма: {format_amount(data.get('amount', 0), data.get('from_currency', ''))} {data.get('from_currency', '')}\n"
            f"• Выдано: {data.get('final_amount', 0):.4f} USDT\n"
            f"• Чек: {check.check_id}"
        )
        
    except ValueError:
        await callback.answer("❌ Неверный ID обмена", show_alert=True)
    except Exception as e:
        logger.error(f"Ошибка проверки оплаты: {e}", exc_info=True)
        await callback.answer("❌ Ошибка при обработке оплаты", show_alert=True)
    
    await callback.answer()

@router.callback_query(F.data == "new_exchange")
async def new_exchange_callback(callback: CallbackQuery, state: FSMContext):
    """Обработчик нового обмена"""
    await state.clear()
    await cmd_exchange(callback.message, state)
    await callback.answer()

@router.callback_query(F.data == "cancel_exchange")
async def cancel_exchange(callback: CallbackQuery, state: FSMContext):
    """Обработчик отмены обмена"""
    await callback.message.edit_text("❌ Обмен отменен.")
    await state.clear()
    await callback.answer()

# ========== ЗАПУСК БОТА ==========
async def main():
    """Основная функция запуска бота"""
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    try:
        # Запускаем HTTP сервер
        http_port = await HTTPServer.start()
        
        # Отправляем уведомление администратору
        network_mode = "Testnet" if USE_TESTNET else "Mainnet"
        startup_msg = (
            f"🤖 *Бот успешно запущен!*\n\n"
            f"• Режим: Только обмен на USDT\n"
            f"• HTTP сервер: {'запущен' if http_port else 'не запущен'}\n"
            f"• Порт: {http_port if http_port else 'N/A'}\n"
            f"• Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"• Режим сети: {network_mode}\n"
            f"• Лимит: {USDT_MAX_LIMIT:.2f} USDT"
        )
        
        await send_admin_notification(bot, startup_msg)
        
        # Запускаем бота
        logger.info("🤖 Запуск Telegram бота...")
        await dp.start_polling(bot, skip_updates=True)
        
    except KeyboardInterrupt:
        logger.info("Получен сигнал прерывания, завершаем работу...")
    except Exception as e:
        logger.error(f"❌ Ошибка запуска бота: {e}", exc_info=True)
        
        # Отправляем уведомление об ошибке
        error_msg = (
            f"❌ *Бот остановлен с ошибкой!*\n\n"
            f"• Ошибка: {str(e)[:100]}\n"
            f"• Время: {datetime.now().strftime('%H:%M:%S')}"
        )
        
        try:
            await send_admin_notification(bot, error_msg)
        except:
            pass
            
        raise
    finally:
        # Закрываем соединения
        await bot.session.close()
        if hasattr(crypto_pay, 'session'):
            await crypto_pay.session.close()
        logger.info("👋 Бот остановлен")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Завершение работы по команде пользователя...")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
