import asyncio
import logging
import sqlite3
import uuid
import os
import re
from datetime import datetime, timedelta
from typing import Optional, Dict, Tuple, Any, List
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
import aiohttp
import json

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
ADMIN_ID = int(os.getenv('ADMIN_ID', '7511053219'))
PORT = int(os.getenv('PORT', '8080'))

# Проверка обязательных переменных
if not all([TOKEN, CRYPTO_PAY_TOKEN]):
    logger.error("❌ Ошибка: Не установлены обязательные переменные в .env файле")
    raise ValueError("Установите BOT_TOKEN и CRYPTO_PAY_TOKEN в .env файле")

# Константы
COMMISSION_RATE = 0.05  # 5%
MIN_USDT_AMOUNT = 0.01
CACHE_DURATION = 300  # 5 минут

# Валюты для обмена на USDT с их лимитами
CRYPTO_ASSETS = {
    'BTC': {'name': 'Bitcoin', 'decimals': 8, 'max_limit': 0.00003},
    'ETH': {'name': 'Ethereum', 'decimals': 6, 'max_limit': 0.001},
    'SOL': {'name': 'Solana', 'decimals': 3, 'max_limit': 0.01},
    'TON': {'name': 'Toncoin', 'decimals': 3, 'max_limit': 0.5},
    'NOT': {'name': 'Notcoin', 'decimals': 0, 'max_limit': 500},
}

# Инициализация Crypto Pay API
crypto_pay = AioCryptoPay(token=CRYPTO_PAY_TOKEN, network=Networks.MAIN_NET)

# ========== КЭШ КУРСОВ BINANCE ==========
class BinanceRateCache:
    """Кэш курсов валют с Binance"""
    def __init__(self, duration: int = CACHE_DURATION):
        self.cache: Dict[str, Dict] = {}
        self.duration = duration
    
    async def get_rate(self, currency: str) -> Optional[float]:
        """Получает курс валюты к USDT с кэшированием"""
        currency = currency.upper()
        
        # Проверяем кэш
        if currency in self.cache:
            cache_item = self.cache[currency]
            if datetime.now().timestamp() - cache_item['timestamp'] < self.duration:
                return cache_item['rate']
        
        # Получаем новый курс
        rate = await self._fetch_rate_from_binance(currency)
        if rate:
            self.cache[currency] = {
                'rate': rate,
                'timestamp': datetime.now().timestamp()
            }
        return rate
    
    async def _fetch_rate_from_binance(self, currency: str) -> Optional[float]:
        """Получает курс с Binance API"""
        if currency == 'USDT':
            return 1.0
            
        symbol = f"{currency}USDT"
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        return float(data['price'])
                    else:
                        logger.error(f"Ошибка Binance API: {response.status}")
                        return await self._get_fallback_rate(currency)
        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError, KeyError, ValueError) as e:
            logger.error(f"Ошибка получения курса {currency}: {e}")
            return await self._get_fallback_rate(currency)
    
    async def _get_fallback_rate(self, currency: str) -> Optional[float]:
        """Резервные курсы на случай ошибки API"""
        fallback_rates = {
            'BTC': 87626.45,
            'ETH': 2937.02,
            'SOL': 127.67,
            'TON': 1.55,
            'NOT': 0.0005329,
        }
        return fallback_rates.get(currency)

# ========== HTTP СЕРВЕР ==========
class HTTPServer:
    """Простой HTTP сервер для health checks"""
    
    @staticmethod
    async def handle_health(request):
        return web.Response(text="OK")
    
    @staticmethod
    async def handle_status(request):
        stats = db.get_statistics()
        return web.json_response({
            "status": "running",
            "timestamp": datetime.now().isoformat(),
            "statistics": stats
        })
    
    @classmethod
    async def start(cls):
        app = web.Application()
        app.router.add_get('/health', cls.handle_health)
        app.router.add_get('/status', cls.handle_status)
        
        runner = web.AppRunner(app)
        await runner.setup()
        
        for port in range(PORT, PORT + 10):
            try:
                site = web.TCPSite(runner, '0.0.0.0', port)
                await site.start()
                logger.info(f"✅ HTTP сервер запущен на порту {port}")
                return port
            except OSError:
                continue
        
        logger.warning("⚠️ Не удалось запустить HTTP сервер")
        return None

# ========== СОСТОЯНИЯ FSM ==========
class ExchangeStates(StatesGroup):
    choosing_currency = State()
    entering_amount = State()
    confirming = State()

# ========== БАЗА ДАННЫХ ==========
class Database:
    """Упрощенный класс для работы с SQLite"""
    
    def __init__(self, db_file: str = "crypto_exchange.db"):
        self.db_file = db_file
        self._init_db()
    
    def _init_db(self):
        """Инициализация базы данных"""
        with closing(sqlite3.connect(self.db_file)) as conn:
            conn.executescript('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER UNIQUE NOT NULL,
                    username TEXT,
                    full_name TEXT,
                    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS exchanges (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    exchange_id TEXT UNIQUE NOT NULL,
                    from_currency TEXT NOT NULL,
                    amount REAL NOT NULL,
                    amount_usdt REAL NOT NULL,
                    commission_usdt REAL NOT NULL,
                    final_amount REAL NOT NULL,
                    invoice_id INTEGER,
                    invoice_url TEXT,
                    check_id INTEGER,
                    check_url TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    paid_at TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(id)
                );
                
                CREATE INDEX IF NOT EXISTS idx_exchange_status ON exchanges(status);
                CREATE INDEX IF NOT EXISTS idx_user_exchanges ON exchanges(user_id);
            ''')
            conn.commit()
    
    def get_connection(self):
        """Возвращает соединение с базой данных"""
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        return conn
    
    def get_or_create_user(self, telegram_id: int, username: str, full_name: str) -> int:
        """Создает или получает пользователя"""
        with closing(self.get_connection()) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT id FROM users WHERE telegram_id = ?', (telegram_id,))
            user = cursor.fetchone()
            
            if user:
                return user['id']
            else:
                cursor.execute(
                    'INSERT INTO users (telegram_id, username, full_name) VALUES (?, ?, ?)',
                    (telegram_id, username or '', full_name or '')
                )
                conn.commit()
                return cursor.lastrowid
    
    def save_exchange(self, exchange_data: dict) -> int:
        """Сохраняет обмен в базу данных"""
        with closing(self.get_connection()) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO exchanges (
                    user_id, exchange_id, from_currency, amount,
                    amount_usdt, commission_usdt, final_amount,
                    invoice_id, invoice_url, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                exchange_data['user_id'],
                exchange_data['exchange_id'],
                exchange_data['from_currency'],
                exchange_data['amount'],
                exchange_data['amount_usdt'],
                exchange_data['commission_usdt'],
                exchange_data['final_amount'],
                exchange_data['invoice_id'],
                exchange_data['invoice_url'],
                exchange_data['status']
            ))
            conn.commit()
            return cursor.lastrowid
    
    def update_exchange_status(self, exchange_id: int, check_data: dict = None, status: str = 'completed'):
        """Обновляет статус обмена"""
        with closing(self.get_connection()) as conn:
            cursor = conn.cursor()
            if check_data:
                cursor.execute('''
                    UPDATE exchanges 
                    SET status = ?, paid_at = CURRENT_TIMESTAMP,
                        check_id = ?, check_url = ?
                    WHERE id = ?
                ''', (status, check_data.get('check_id'), check_data.get('check_url'), exchange_id))
            else:
                cursor.execute('''
                    UPDATE exchanges SET status = ? WHERE id = ?
                ''', (status, exchange_id))
            conn.commit()
    
    def get_exchange(self, exchange_id: int):
        """Получает информацию об обмене"""
        with closing(self.get_connection()) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM exchanges WHERE id = ?', (exchange_id,))
            return cursor.fetchone()
    
    def get_statistics(self) -> dict:
        """Возвращает статистику"""
        with closing(self.get_connection()) as conn:
            cursor = conn.cursor()
            
            cursor.execute('SELECT COUNT(*) FROM users')
            users = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM exchanges')
            total = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM exchanges WHERE status = 'completed'")
            completed = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM exchanges WHERE status = 'pending'")
            pending = cursor.fetchone()[0]
            
            return {
                'users': users,
                'total_exchanges': total,
                'completed': completed,
                'pending': pending
            }

# Инициализация
rate_cache = BinanceRateCache()
db = Database()

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
def format_amount(amount: float, currency: str) -> str:
    """Форматирует сумму с учетом точности валюты"""
    if currency not in CRYPTO_ASSETS:
        return f"{amount:.8f}"
    
    decimals = CRYPTO_ASSETS[currency]['decimals']
    if decimals == 0:
        return f"{int(amount)}"
    
    format_str = f"{{:.{decimals}f}}"
    formatted = format_str.format(amount)
    return formatted.rstrip('0').rstrip('.')

def extract_number(text: str) -> Optional[float]:
    """Извлекает число из текста"""
    text = text.strip().replace(',', '.')
    match = re.search(r'[-+]?\d*\.?\d+', text)
    return float(match.group()) if match else None

async def validate_amount(amount: float, currency: str) -> Tuple[bool, str, Optional[float], Optional[float]]:
    """
    Проверяет сумму обмена
    
    Возвращает: (is_valid, error_message, amount_usdt, max_limit)
    """
    if amount <= 0:
        return False, "Сумма должна быть больше 0", None, None
    
    rate = await rate_cache.get_rate(currency)
    if not rate or rate <= 0:
        return False, f"Не удалось получить курс {currency}/USDT", None, None
    
    amount_usdt = amount * rate
    max_limit = CRYPTO_ASSETS[currency]['max_limit']
    
    if amount_usdt < MIN_USDT_AMOUNT:
        min_in_currency = MIN_USDT_AMOUNT / rate
        return False, (
            f"Минимальная сумма: {format_amount(min_in_currency, currency)} {currency} "
            f"(${MIN_USDT_AMOUNT:.2f} USDT)"
        ), amount_usdt, max_limit
    
    if amount > max_limit:
        return False, (
            f"Максимальная сумма: {format_amount(max_limit, currency)} {currency} "
            f"(${(max_limit * rate):.2f} USDT)"
        ), amount_usdt, max_limit
    
    return True, "", amount_usdt, max_limit

def get_currency_keyboard() -> InlineKeyboardMarkup:
    """Создает клавиатуру для выбора валюты"""
    buttons = []
    for code, info in CRYPTO_ASSETS.items():
        buttons.append([
            InlineKeyboardButton(
                text=f"{info['name']} ({code}) → USDT",
                callback_data=f"currency:{code}"
            )
        ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def send_admin_notification(bot: Bot, message: str):
    """Отправляет уведомление администратору"""
    try:
        await bot.send_message(ADMIN_ID, message, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Ошибка отправки уведомления: {e}")

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
    
    # Формируем текст с лимитами
    limits_text = ""
    for code, info in CRYPTO_ASSETS.items():
        limits_text += f"• {info['name']} ({code}): макс. {format_amount(info['max_limit'], code)}\n"
    
    welcome_text = f"""
👋 Добро пожаловать в FlipExchange!

💰 <b>Обмен криптовалюты на USDT</b>

Доступные валюты:
{limits_text}

📋 <b>Доступные команды:</b>
/exchange - начать обмен
/status - статус последнего обмена
/rates - текущие курсы
/cancel - отмена операции

Для начала нажмите /exchange
"""
    
    await message.answer(welcome_text, parse_mode="HTML")

@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("✅ Операция отменена. Используйте /exchange для нового обмена.")

@router.message(Command("exchange"))
async def cmd_exchange(message: Message, state: FSMContext):
    await message.answer(
        "🔄 Выберите валюту для обмена на USDT:",
        reply_markup=get_currency_keyboard()
    )
    await state.set_state(ExchangeStates.choosing_currency)

@router.message(Command("status"))
async def cmd_status(message: Message):
    """Показывает статус последнего обмена"""
    try:
        user_id = db.get_or_create_user(
            message.from_user.id,
            message.from_user.username,
            message.from_user.full_name
        )
        
        with closing(db.get_connection()) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM exchanges 
                WHERE user_id = ? 
                ORDER BY created_at DESC 
                LIMIT 1
            ''', (user_id,))
            exchange = cursor.fetchone()
        
        if not exchange:
            await message.answer("У вас нет обменов.")
            return
        
        status_text = f"""
📋 <b>Статус обмена:</b>

ID: {exchange['exchange_id']}
Валюта: {exchange['from_currency']} → USDT
Сумма: {format_amount(exchange['amount'], exchange['from_currency'])} {exchange['from_currency']}
К получению: {format_amount(exchange['final_amount'], 'USDT')} USDT
Статус: {exchange['status']}
Дата: {exchange['created_at']}
"""
        
        keyboard = None
        if exchange['status'] == 'pending' and exchange['invoice_url']:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💳 Оплатить счет", url=exchange['invoice_url'])],
                [InlineKeyboardButton(text="✅ Проверить оплату", callback_data=f"check:{exchange['id']}")]
            ])
        
        await message.answer(status_text, parse_mode="HTML", reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Ошибка проверки статуса: {e}")
        await message.answer("❌ Ошибка при проверке статуса.")

@router.message(Command("rates"))
async def cmd_rates(message: Message):
    """Показывает текущие курсы"""
    try:
        rates_text = "📈 <b>Текущие курсы к USDT:</b>\n\n"
        
        for currency, info in CRYPTO_ASSETS.items():
            rate = await rate_cache.get_rate(currency)
            if rate and rate > 0:
                max_limit = info['max_limit']
                max_usdt = max_limit * rate
                rates_text += f"<b>{currency}</b>: 1 = {rate:.8f} USDT\n"
                rates_text += f"Макс: {format_amount(max_limit, currency)} {currency} (${max_usdt:.2f} USDT)\n\n"
        
        rates_text += f"💸 Комиссия: {COMMISSION_RATE * 100:.1f}%"
        
        await message.answer(rates_text, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Ошибка получения курсов: {e}")
        await message.answer("❌ Ошибка получения курсов")

# ========== ОБРАБОТЧИКИ FSM ==========
@router.callback_query(F.data.startswith("currency:"))
async def process_currency(callback: CallbackQuery, state: FSMContext):
    """Обработчик выбора валюты"""
    currency = callback.data.split(":")[1]
    
    if currency not in CRYPTO_ASSETS:
        await callback.answer("❌ Неизвестная валюта")
        return
    
    # Получаем курс для расчета максимальной суммы в USDT
    rate = await rate_cache.get_rate(currency)
    if not rate or rate <= 0:
        await callback.answer("❌ Ошибка получения курса")
        return
    
    currency_info = CRYPTO_ASSETS[currency]
    max_limit = currency_info['max_limit']
    max_usdt = max_limit * rate
    
    await state.update_data({
        'currency': currency,
        'rate': rate,
        'max_limit': max_limit
    })
    
    await callback.message.edit_text(
        f"Вы выбрали: {currency_info['name']} ({currency})\n\n"
        f"Введите сумму {currency}, которую хотите обменять на USDT.\n"
        f"<b>Максимум: {format_amount(max_limit, currency)} {currency} "
        f"(${max_usdt:.2f} USDT)</b>\n\n"
        f"<i>Пример: {format_amount(max_limit / 10, currency)}</i>",
        parse_mode="HTML"
    )
    await state.set_state(ExchangeStates.entering_amount)
    await callback.answer()

@router.message(ExchangeStates.entering_amount)
async def process_amount(message: Message, state: FSMContext):
    """Обработчик ввода суммы"""
    try:
        data = await state.get_data()
        currency = data.get('currency')
        
        if not currency:
            await message.answer("❌ Ошибка: валюта не выбрана. /exchange")
            await state.clear()
            return
        
        amount = extract_number(message.text)
        if amount is None:
            await message.answer("❌ Введите корректное число (например: 0.025)")
            return
        
        is_valid, error_msg, amount_usdt, max_limit = await validate_amount(amount, currency)
        
        if not is_valid:
            await message.answer(f"❌ {error_msg}")
            return
        
        # Расчет комиссии и итоговой суммы
        commission_usdt = amount_usdt * COMMISSION_RATE
        final_amount_usdt = amount_usdt - commission_usdt
        
        await state.update_data({
            'amount': amount,
            'amount_usdt': amount_usdt,
            'commission_usdt': commission_usdt,
            'final_amount_usdt': final_amount_usdt
        })
        
        confirmation_text = f"""
✅ <b>Подтвердите обмен:</b>

📤 Отправляете: {format_amount(amount, currency)} {currency}
📥 Получаете: {format_amount(final_amount_usdt, 'USDT')} USDT
💸 Комиссия ({COMMISSION_RATE * 100:.1f}%): {format_amount(commission_usdt, 'USDT')} USDT

Курс: 1 {currency} = {data.get('rate', 0):.8f} USDT
        """
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Подтвердить", callback_data="confirm"),
                InlineKeyboardButton(text="❌ Отменить", callback_data="cancel")
            ]
        ])
        
        await message.answer(confirmation_text, parse_mode="HTML", reply_markup=keyboard)
        await state.set_state(ExchangeStates.confirming)
        
    except Exception as e:
        logger.error(f"Ошибка обработки суммы: {e}")
        await message.answer("❌ Произошла ошибка. Попробуйте еще раз.")

@router.callback_query(F.data == "confirm")
async def confirm_exchange(callback: CallbackQuery, state: FSMContext):
    """Создание инвойса для обмена"""
    try:
        data = await state.get_data()
        
        if not all(k in data for k in ['currency', 'amount', 'final_amount_usdt']):
            await callback.answer("❌ Ошибка данных")
            return
        
        user_id = db.get_or_create_user(
            callback.from_user.id,
            callback.from_user.username,
            callback.from_user.full_name
        )
        
        exchange_id = str(uuid.uuid4())[:8]
        
        # Создание инвойса в Crypto Pay
        try:
            invoice = await crypto_pay.create_invoice(
                asset=data['currency'],
                amount=float(data['amount']),
                description=f"Обмен {data['currency']} на USDT",
                hidden_message=f"User: {user_id} | Exchange: {exchange_id}",
                expires_in=900
            )
        except Exception as e:
            logger.error(f"Ошибка создания инвойса: {e}")
            await callback.message.answer("❌ Ошибка при создании счета. Попробуйте позже.")
            return
        
        # Сохранение в базу данных
        exchange_data = {
            'user_id': user_id,
            'exchange_id': exchange_id,
            'from_currency': data['currency'],
            'amount': float(data['amount']),
            'amount_usdt': float(data['amount_usdt']),
            'commission_usdt': float(data['commission_usdt']),
            'final_amount': float(data['final_amount_usdt']),
            'invoice_id': invoice.invoice_id,
            'invoice_url': invoice.bot_invoice_url,
            'status': 'pending'
        }
        
        exchange_db_id = db.save_exchange(exchange_data)
        
        await state.update_data({'exchange_db_id': exchange_db_id, 'invoice_id': invoice.invoice_id})
        
        # Отправка счета пользователю
        invoice_text = f"""
💰 <b>Счет для оплаты</b>

ID обмена: {exchange_id}
Сумма: {format_amount(data['amount'], data['currency'])} {data['currency']}
К получению: {format_amount(data['final_amount_usdt'], 'USDT')} USDT
Комиссия: {format_amount(data['commission_usdt'], 'USDT')} USDT

⏰ Счет действителен 15 минут
"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Оплатить счет", url=invoice.bot_invoice_url)],
            [InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"check:{exchange_db_id}")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
        ])
        
        await callback.message.edit_text(invoice_text, parse_mode="HTML", reply_markup=keyboard)
        
        # Уведомление администратора
        await send_admin_notification(
            callback.bot,
            f"💎 *Новый обмен создан!*\n\n"
            f"• Пользователь: @{callback.from_user.username or 'N/A'}\n"
            f"• ID: {exchange_id}\n"
            f"• Сумма: {format_amount(data['amount'], data['currency'])} {data['currency']}\n"
            f"• В USDT: {data['final_amount_usdt']:.4f}"
        )
        
    except Exception as e:
        logger.error(f"Ошибка подтверждения: {e}", exc_info=True)
        await callback.message.answer("❌ Ошибка при создании счета.")
    finally:
        await callback.answer()

@router.callback_query(F.data.startswith("check:"))
async def check_payment(callback: CallbackQuery, state: FSMContext):
    """Проверка оплаты инвойса"""
    try:
        exchange_db_id = int(callback.data.split(":")[1])
        
        # Проверяем статус инвойса
        data = await state.get_data()
        invoice_id = data.get('invoice_id')
        
        if not invoice_id:
            await callback.answer("❌ Информация не найдена")
            return
        
        try:
            invoices = await crypto_pay.get_invoices(invoice_ids=invoice_id)
            if not invoices or invoices[0].status != 'paid':
                await callback.answer("⚠️ Счёт ещё не оплачен", show_alert=True)
                return
        except Exception as e:
            logger.error(f"Ошибка проверки инвойса: {e}")
            await callback.answer("❌ Ошибка проверки оплаты")
            return
        
        # Создаем чек в USDT
        try:
            check_amount = data.get('final_amount_usdt', 0)
            check = await crypto_pay.create_check(
                asset='USDT',
                amount=float(check_amount),
                pin_to_user_id=callback.from_user.id
            )
        except Exception as e:
            logger.error(f"Ошибка создания чека: {e}")
            await callback.answer("❌ Ошибка создания чека")
            return
        
        # Обновляем базу данных
        db.update_exchange_status(exchange_db_id, {
            'check_id': check.check_id,
            'check_url': check.bot_check_url
        })
        
        # Отправляем чек пользователю
        receipt_text = f"""
🎉 <b>Обмен успешно завершен!</b>

ID операции: {data.get('exchange_id', 'N/A')}
Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

📤 Вы отправили: {format_amount(data.get('amount', 0), data.get('currency', ''))} {data.get('currency', '')}
📥 Вы получили: {format_amount(data.get('final_amount_usdt', 0), 'USDT')} USDT

💎 <b>Ваш чек:</b> {check.bot_check_url}
Активируйте его в @CryptoBot
"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💎 Активировать чек", url=check.bot_check_url)],
            [InlineKeyboardButton(text="🔄 Новый обмен", callback_data="new_exchange")]
        ])
        
        await callback.message.edit_text(receipt_text, parse_mode="HTML", reply_markup=keyboard)
        await state.clear()
        
        # Уведомление администратора
        await send_admin_notification(
            callback.bot,
            f"✅ *Обмен завершен!*\n\n"
            f"• ID: {data.get('exchange_id', 'N/A')}\n"
            f"• Пользователь: @{callback.from_user.username or 'N/A'}\n"
            f"• Выдано: {data.get('final_amount_usdt', 0):.4f} USDT\n"
            f"• Чек ID: {check.check_id}"
        )
        
    except Exception as e:
        logger.error(f"Ошибка проверки оплаты: {e}")
        await callback.answer("❌ Ошибка при обработке оплаты")
    finally:
        await callback.answer()

@router.callback_query(F.data == "new_exchange")
async def new_exchange(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await cmd_exchange(callback.message, state)
    await callback.answer()

@router.callback_query(F.data == "cancel")
async def cancel_operation(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("❌ Операция отменена.")
    await state.clear()
    await callback.answer()

# ========== ЗАПУСК БОТА ==========
async def main():
    """Основная функция запуска"""
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    
    try:
        # Запускаем HTTP сервер
        await HTTPServer.start()
        
        # Формируем текст с лимитами для администратора
        limits_text = ""
        for code, info in CRYPTO_ASSETS.items():
            limits_text += f"• {code}: макс. {format_amount(info['max_limit'], code)}\n"
        
        # Уведомление администратора
        await bot.send_message(
            ADMIN_ID,
            f"🤖 *Бот запущен!*\n\n"
            f"• Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"<b>Лимиты валют:</b>\n{limits_text}\n"
            f"• Комиссия: {COMMISSION_RATE * 100:.1f}%",
            parse_mode="HTML"
        )
        
        logger.info("🤖 Бот запущен")
        await dp.start_polling(bot, skip_updates=True)
        
    except KeyboardInterrupt:
        logger.info("Завершение работы...")
    except Exception as e:
        logger.error(f"Ошибка запуска: {e}", exc_info=True)
        await send_admin_notification(bot, f"❌ *Бот упал!*\nОшибка: {str(e)[:200]}")
    finally:
        await bot.session.close()
        logger.info("Бот остановлен")

if __name__ == "__main__":
    asyncio.run(main())

