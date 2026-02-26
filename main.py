import logging
import asyncpg
import os
import asyncio
import re
import time
import hashlib
import sys
from collections import defaultdict
from typing import Optional, Tuple, Dict, Any, List, Set
from datetime import datetime, timedelta
from contextlib import suppress

from aiohttp import web
from aiogram import Bot, Dispatcher, types, F, html
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject, StateFilter
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv
from aiogram.client.default import DefaultBotProperties
from aiogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup,
    KeyboardButton, ReplyKeyboardRemove, ForceReply
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.dispatcher.middlewares.base import BaseMiddleware

# --- Constants ---
CATEGORIES = [
    "Crush", "Love", "Relationship", "Breakup", "Dating",
    "Mental Health", "Trauma", "Addiction", "Health",
    "Family", "Friendship", "School",
    "Harassment", "Sexual",
    "Religion", "Mental", "Funny",
    "Question", "Advice Needed",
    "Other"
]
POINTS_PER_CONFESSION = 1
POINTS_PER_LIKE_RECEIVED = 1
POINTS_PER_DISLIKE_RECEIVED = -1
MAX_CATEGORIES = 3
MAX_PHOTO_SIZE_MB = 5
RATE_LIMIT_SECONDS = 30

# Load environment variables
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKENS")
ADMIN_IDS_STR = os.getenv("ADMIN_ID")  # Keep backward compatibility
CHANNEL_ID = os.getenv("CHANNEL_ID")
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "15"))
DATABASE_URL = os.getenv("DATABASE_URL")
HTTP_PORT_STR = os.getenv("PORT")

# Validate essential environment variables
if not BOT_TOKEN:
    raise ValueError("FATAL: BOT_TOKEN environment variable not set!")
if not ADMIN_IDS_STR:
    raise ValueError("FATAL: ADMIN_ID environment variable not set!")
if not CHANNEL_ID:
    raise ValueError("FATAL: CHANNEL_ID environment variable not set!")
if not DATABASE_URL:
    raise ValueError("FATAL: DATABASE_URL environment variable not set!")

# Parse admin IDs (support both single and multiple)
ADMIN_IDS: Set[int] = set()
try:
    # Try to parse as comma-separated first
    if ',' in ADMIN_IDS_STR:
        for admin_id_str in ADMIN_IDS_STR.split(','):
            ADMIN_IDS.add(int(admin_id_str.strip()))
    else:
        # Single admin ID
        ADMIN_IDS.add(int(ADMIN_IDS_STR))
except ValueError:
    raise ValueError("FATAL: ADMIN_ID environment variable must contain valid integers!")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Bot and Dispatcher
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher(storage=MemoryStorage())

# Bot info
bot_info = None

# Database connection
db = None
db_connection_retries = 0
MAX_DB_RETRIES = 5

# Rate limiting
user_last_action = defaultdict(float)

# --- FSM States ---
class ConfessionForm(StatesGroup):
    selecting_categories = State()
    waiting_for_text = State()

class CommentForm(StatesGroup):
    waiting_for_comment = State()
    waiting_for_reply = State()

class ContactAdminForm(StatesGroup):
    waiting_for_message = State()

class AdminActions(StatesGroup):
    waiting_for_rejection_reason = State()

class UserProfileForm(StatesGroup):
    waiting_for_profile_name = State()

class ChatForm(StatesGroup):
    chatting = State()

class ContactRequestForm(StatesGroup):
    waiting_for_contact_message = State()

class BlockForm(StatesGroup):
    waiting_for_block_duration = State()
    waiting_for_block_reason = State()

# --- Database Helper Functions ---
async def ensure_db_connection():
    """Ensure database connection is active, reconnect if needed"""
    global db, db_connection_retries
    
    if db is None:
        try:
            await create_db_pool()
            return True
        except Exception as e:
            logger.error(f"Failed to create database pool: {e}")
            return False
    
    try:
        async with db.acquire() as conn:
            await conn.fetchval('SELECT 1')
        db_connection_retries = 0
        return True
    except Exception as e:
        db_connection_retries += 1
        logger.error(f"Database connection lost (attempt {db_connection_retries}/{MAX_DB_RETRIES}): {e}")
        
        if db_connection_retries <= MAX_DB_RETRIES:
            try:
                await db.close()
            except:
                pass
            try:
                await create_db_pool()
                logger.info("Database connection reestablished")
                return True
            except Exception as reconnect_error:
                logger.error(f"Failed to reconnect to database: {reconnect_error}")
        return False


# --- Database Helper Functions ---
async def execute_query(query: str, *params):
    """Execute query and return results"""
    # Try using db_manager first
    if db_manager and db_manager.pool:
        async with db_manager.pool.acquire() as conn:
            return await conn.fetch(query, *params)
    # Fallback to old db
    if db:
        async with db.acquire() as conn:
            return await conn.fetch(query, *params)
    raise Exception("No database connection available")

async def fetch_one(query: str, *params):
    """Fetch single row"""
    if db_manager and db_manager.pool:
        async with db_manager.pool.acquire() as conn:
            return await conn.fetchrow(query, *params)
    if db:
        async with db.acquire() as conn:
            return await conn.fetchrow(query, *params)
    raise Exception("No database connection available")

async def execute_update(query: str, *params):
    """Execute INSERT/UPDATE/DELETE query"""
    if db_manager and db_manager.pool:
        async with db_manager.pool.acquire() as conn:
            return await conn.execute(query, *params)
    if db:
        async with db.acquire() as conn:
            return await conn.execute(query, *params)
    raise Exception("No database connection available")

async def execute_insert_return_id(query: str, *params):
    """Execute INSERT and return inserted ID"""
    if db_manager and db_manager.pool:
        async with db_manager.pool.acquire() as conn:
            result = await conn.fetchval(query, *params)
            if result is None:
                result = await conn.fetchval("SELECT LASTVAL();")
            return result
    if db:
        async with db.acquire() as conn:
            result = await conn.fetchval(query, *params)
            if result is None:
                result = await conn.fetchval("SELECT LASTVAL();")
            return result
    raise Exception("No database connection available")

async def is_admin(user_id: int) -> bool:
    """Check if user is admin"""
    return user_id in ADMIN_IDS

async def get_profile_name(user_id: int) -> str:
    """Get user's profile name with caching"""
    return await profile_cache.get_profile_name(user_id)

def encode_user_id(user_id: int) -> str:
    """Encode user ID to a short, non-reversible string"""
    salt = "profile_salt_v1"
    data = f"{user_id}{salt}".encode()
    return hashlib.sha256(data).hexdigest()[:12]

async def get_encoded_profile_link(user_id: int) -> str:
    """Get encoded profile link for a user"""
    encoded = encode_user_id(user_id)
    return f"https://t.me/{bot_info.username}?start=profile_{encoded}"

async def get_user_id_from_encoded(encoded_id: str) -> Optional[int]:
    """Get user ID from encoded string by checking database"""
    try:
        users = await execute_query("SELECT user_id FROM user_status")
        for user_row in users:
            user_id = user_row['user_id']
            if encode_user_id(user_id) == encoded_id:
                return user_id
    except Exception as e:
        logger.error(f"Error decoding user ID: {e}")
    return None



# --- Database Setup ---

async def create_db_pool():
    """Create PostgreSQL connection pool for Supabase - OPTIMIZED"""
    global db, db_manager
    db_manager = DatabaseManager()  # Initialize first
    try:
        connection_string = DATABASE_URL
        
        # Handle different connection string formats
        if connection_string.startswith('postgresql://'):
            connection_string = connection_string.replace('postgresql://', 'postgres://')
        
        # Add sslmode=require if not present
        if 'sslmode' not in connection_string:
            if '?' in connection_string:
                connection_string += '&sslmode=require'
            else:
                connection_string += '?sslmode=require'
        
        logger.info("Connecting to database with optimized settings...")
        
        # Create pool with optimized settings
        db = await asyncpg.create_pool(
            dsn=connection_string,
            min_size=5,              # Increased from 1
            max_size=20,              # Increased from 10
            command_timeout=60,
            max_queries=50000,
            max_inactive_connection_lifetime=300,
            timeout=30,
            statement_cache_size=0    # Keep this for PgBouncer!
        )
        
        # Set the pool in db_manager
        db_manager.pool = db
        
        logger.info("✅ Database connection pool created (optimized)")
        
        # Start background tasks
        asyncio.create_task(rate_limiter.cleanup_task())
        
        return db
    except Exception as e:
        logger.error(f"Failed to create database pool: {e}")
        raise

async def setup():
    """Initialize bot and database"""
    global db, bot_info
    
    try:
        bot_info = await bot.get_me()
        logger.info(f"Bot: @{bot_info.username}")
        
        db = await create_db_pool()
        
        async with db.acquire() as conn:
            # --- Existing Tables (kept exactly as they are) ---
                        # --- Contact Requests Table (FIXED to allow NULL values) ---
                        # --- Contact Requests Table (FIXED to allow NULL values) ---
                        # --- Contact Requests Table (FIXED to allow NULL values) ---
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS contact_requests (
                    id SERIAL PRIMARY KEY,
                    confession_id INTEGER REFERENCES confessions(id) ON DELETE CASCADE,
                    comment_id INTEGER REFERENCES comments(id) ON DELETE CASCADE,
                    requester_user_id BIGINT NOT NULL,
                    requested_user_id BIGINT NOT NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'pending',
                    message TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (comment_id, requester_user_id)
                );
            """)
            logger.info("✅ Contact requests table ready (with NULL support)")
            
            # ADD THIS LINE - Add message column if it doesn't exist
                        # ADD THIS - Remove NOT NULL constraint from confession_id and comment_id
            await conn.execute("""
                DO $$ 
                BEGIN
                    -- Check if confession_id has NOT NULL constraint
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='contact_requests' 
                        AND column_name='confession_id' 
                        AND is_nullable = 'NO'
                    ) THEN
                        ALTER TABLE contact_requests ALTER COLUMN confession_id DROP NOT NULL;
                    END IF;
                    
                    -- Check if comment_id has NOT NULL constraint
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='contact_requests' 
                        AND column_name='comment_id' 
                        AND is_nullable = 'NO'
                    ) THEN
                        ALTER TABLE contact_requests ALTER COLUMN comment_id DROP NOT NULL;
                    END IF;
                END $$;
            """)
            logger.info("✅ Removed NOT NULL constraints from confession_id and comment_id")
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS comments (
                    id SERIAL PRIMARY KEY,
                    confession_id INTEGER NOT NULL REFERENCES confessions(id) ON DELETE CASCADE,
                    user_id BIGINT NOT NULL,
                    text TEXT NULL,
                    sticker_file_id TEXT NULL,
                    animation_file_id TEXT NULL,
                    parent_comment_id INTEGER NULL REFERENCES comments(id) ON DELETE CASCADE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_comments_confession_id ON comments(confession_id);
                CREATE INDEX IF NOT EXISTS idx_comments_user_id ON comments(user_id);
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS reactions (
                    id SERIAL PRIMARY KEY,
                    comment_id INTEGER REFERENCES comments(id) ON DELETE CASCADE,
                    user_id BIGINT NOT NULL,
                    reaction_type VARCHAR(10) NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(comment_id, user_id)
                );
            """)
            
            
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS user_points (
                    user_id BIGINT PRIMARY KEY,
                    points INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_user_points_user_id ON user_points(user_id);
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS reports (
                    id SERIAL PRIMARY KEY,
                    comment_id INTEGER NOT NULL REFERENCES comments(id) ON DELETE CASCADE,
                    reporter_user_id BIGINT NOT NULL,
                    reported_user_id BIGINT NOT NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (comment_id, reporter_user_id)
                );
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS deletion_requests (
                    id SERIAL PRIMARY KEY,
                    confession_id INTEGER NOT NULL REFERENCES confessions(id) ON DELETE CASCADE,
                    user_id BIGINT NOT NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMP WITH TIME ZONE,
                    reviewed_at TIMESTAMP WITH TIME ZONE,
                    UNIQUE (confession_id, user_id)
                );
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS user_status (
                    user_id BIGINT PRIMARY KEY,
                    has_accepted_rules BOOLEAN NOT NULL DEFAULT FALSE,
                    is_blocked BOOLEAN NOT NULL DEFAULT FALSE,
                    blocked_until TIMESTAMP WITH TIME ZONE NULL,
                    block_reason TEXT NULL,
                    profile_name TEXT NULL DEFAULT 'Anonymous'
                );
            """)
            
            # Ensure profile_name column exists
            await conn.execute("""
                DO $$ 
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                  WHERE table_name='user_status' AND column_name='profile_name') THEN
                        ALTER TABLE user_status ADD COLUMN profile_name TEXT NULL DEFAULT 'Anonymous';
                    END IF;
                END $$;
            """)
            
            # --- NEW TABLES (Added safely) ---
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS active_chats (
                    id SERIAL PRIMARY KEY,
                    user1_id BIGINT NOT NULL,
                    user2_id BIGINT NOT NULL,
                    started_by BIGINT NOT NULL,
                    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    last_message_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    is_active INTEGER DEFAULT 1
                );
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS chat_messages (
                    id SERIAL PRIMARY KEY,
                    chat_id INTEGER NOT NULL REFERENCES active_chats(id) ON DELETE CASCADE,
                    sender_id BIGINT NOT NULL,
                    message_text TEXT,
                    sticker_file_id TEXT,
                    animation_file_id TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Create indexes for new tables
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_active_chats_user1 ON active_chats(user1_id);
                CREATE INDEX IF NOT EXISTS idx_active_chats_user2 ON active_chats(user2_id);
                CREATE INDEX IF NOT EXISTS idx_active_chats_is_active ON active_chats(is_active);
                CREATE INDEX IF NOT EXISTS idx_chat_messages_chat_id ON chat_messages(chat_id);
                CREATE INDEX IF NOT EXISTS idx_chat_messages_created_at ON chat_messages(created_at);
            """)
            
            logger.info("✅ All database tables ready")
        
    except Exception as e:
        logger.critical(f"Failed to setup: {e}")
        raise



# --- Database Manager for Optimized Queries ---
class DatabaseManager:
    def __init__(self):
        self.pool = None
        self._last_check = 0
        self._check_interval = 60
    
    async def init_pool(self, dsn):
        self.pool = await asyncpg.create_pool(
            dsn=dsn,
            min_size=5,           # Keep 5 connections ready
            max_size=20,           # Allow up to 20 concurrent connections
            command_timeout=60,
            max_queries=50000,
            max_inactive_connection_lifetime=300,
            timeout=30,
            statement_cache_size=0
        )
        return self.pool
    
    async def fetch(self, query, *args):
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, *args)
    
    async def fetchrow(self, query, *args):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    
    async def execute(self, query, *args):
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args)
    
    async def fetch_batch_profiles(self, user_ids):
        """Get multiple user profiles in one query"""
        if not user_ids:
            return {}
        
        rows = await self.fetch("""
            SELECT 
                u.user_id,
                COALESCE(us.profile_name, 'Anonymous') as profile_name,
                COALESCE(up.points, 0) as points
            FROM unnest($1::bigint[]) u(user_id)
            LEFT JOIN user_status us ON u.user_id = us.user_id
            LEFT JOIN user_points up ON u.user_id = up.user_id
        """, user_ids)
        
        return {row['user_id']: (row['profile_name'], row['points']) for row in rows}

# Initialize database manager
db_manager = None
# --- Profile Cache for Faster Access ---
class ProfileCache:
    def __init__(self, ttl_seconds=300):
        self.cache = {}
        self.ttl = ttl_seconds
    
    async def get_profile_name(self, user_id: int) -> str:
        """Get profile name with caching"""
        cached = self.cache.get(user_id)
        if cached and time.time() - cached['timestamp'] < self.ttl:
            return cached['name']
        
        # Cache miss - fetch from database
        row = await fetch_one("SELECT profile_name FROM user_status WHERE user_id = $1", user_id)
        name = row['profile_name'] if row and row['profile_name'] else "Anonymous"
        
        self.cache[user_id] = {
            'name': name,
            'timestamp': time.time()
        }
        return name
    
    def invalidate(self, user_id: int):
        """Remove user from cache when profile changes"""
        self.cache.pop(user_id, None)



# --- Optimized Rate Limiter ---
class RateLimiter:
    def __init__(self, rate_limit_seconds=30):
        self.rate_limit = rate_limit_seconds
        self.user_actions = {}
    
    def check_and_update(self, user_id: int) -> bool:
        """Check rate limit and update timestamp"""
        current_time = time.time()
        last_time = self.user_actions.get(user_id, 0)
        
        if current_time - last_time < self.rate_limit:
            return False
        
        self.user_actions[user_id] = current_time
        return True
    
    async def cleanup_task(self):
        """Background task to clean up old entries"""
        while True:
            await asyncio.sleep(300)  # Clean every 5 minutes
            current_time = time.time()
            cutoff = current_time - self.rate_limit * 2
            self.user_actions = {
                uid: ts for uid, ts in self.user_actions.items() 
                if ts > cutoff
            }

# Initialize rate limiter
rate_limiter = RateLimiter()

# Initialize profile cache
profile_cache = ProfileCache()
# --- Middleware ---
class BlockUserMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: types.TelegramObject, data: Dict[str, Any]) -> Any:
        user = data.get('event_from_user')
        if not user:
            return await handler(event, data)

        user_id = user.id
        
        # STEP 1: LOG EVERYTHING (for debugging)
        if isinstance(event, types.CallbackQuery):
            logger.info(f"🛡️ MIDDLEWARE - Callback: {event.data} from user {user_id}")
        
        # STEP 4: ALLOW CONTACT-RELATED CALLBACKS (the fix)
        if isinstance(event, types.CallbackQuery):
            # Always allow these callbacks regardless of block/rules status
            if event.data.startswith(('accept_rules', 'req_contact_', 'approve_contact_', 'reject_contact_')):
                logger.info(f"🛡️ MIDDLEWARE - ✅ ALLOWING contact callback: {event.data}")
                logger.info(f"🛡️ MIDDLEWARE - Bypassing all checks for user {user_id}")
                return await handler(event, data)
            else:
                logger.info(f"🛡️ MIDDLEWARE - ❌ NOT a contact callback: {event.data}")
        
        # Allow /start and /help commands
        if isinstance(event, types.Message) and event.text:
            if event.text.startswith('/start') or event.text.startswith('/help'):
                logger.info(f"🛡️ MIDDLEWARE - Allowing start/help command from user {user_id}")
                return await handler(event, data)
        
        # Check if user is admin
        if await is_admin(user_id):
            logger.info(f"🛡️ MIDDLEWARE - User {user_id} is admin, allowing")
            return await handler(event, data)

        # Check block status for everything else
        try:
            logger.info(f"🛡️ MIDDLEWARE - Checking status for user {user_id}")
            row = await fetch_one("SELECT is_blocked, blocked_until, block_reason, has_accepted_rules FROM user_status WHERE user_id = $1", user_id)
            logger.info(f"🛡️ MIDDLEWARE - User status: {row}")
            
            # Check if user has accepted rules
            if not row or not row['has_accepted_rules']:
                logger.warning(f"🛡️ MIDDLEWARE - User {user_id} hasn't accepted rules")
                if isinstance(event, types.CallbackQuery):
                    await event.answer("Please accept the rules first using /start", show_alert=True)
                return
            
            # Check if blocked
            if row and row['is_blocked']:
                logger.warning(f"🛡️ MIDDLEWARE - User {user_id} is blocked")
                now = datetime.now(datetime.utcnow().astimezone().tzinfo)
                blocked_until = row['blocked_until']
                
                if blocked_until and blocked_until < now:
                    logger.info(f"🛡️ MIDDLEWARE - Block expired for user {user_id}, unblocking")
                    await execute_update("UPDATE user_status SET is_blocked = FALSE, blocked_until = NULL, block_reason = NULL WHERE user_id = $1", user_id)
                    return await handler(event, data)
                else:
                    expiry_info = f"until {blocked_until.strftime('%Y-%m-%d %H:%M %Z')}" if blocked_until else "permanently"
                    if isinstance(event, types.CallbackQuery):
                        logger.info(f"🛡️ MIDDLEWARE - Sending block message to user {user_id}")
                        await event.answer(f"You are blocked {expiry_info}.", show_alert=True)
                    return
            
            logger.info(f"🛡️ MIDDLEWARE - User {user_id} passed all checks, allowing")
        except Exception as e:
            logger.error(f"🛡️ MIDDLEWARE - Error checking status for user {user_id}: {e}")
        
        return await handler(event, data)

# --- Helper Functions (existing ones kept, new ones added) ---
def create_category_keyboard(selected_categories: List[str] = None):
    if selected_categories is None:
        selected_categories = []
    builder = InlineKeyboardBuilder()
    for category in CATEGORIES:
        prefix = "✅ " if category in selected_categories else ""
        builder.button(text=f"{prefix}{category}", callback_data=f"category_{category}")
    builder.adjust(2)
    if 1 <= len(selected_categories) <= MAX_CATEGORIES:
         builder.row(InlineKeyboardButton(text=f"➡️ Done Selecting ({len(selected_categories)}/{MAX_CATEGORIES})", callback_data="category_done"))
    elif len(selected_categories) > MAX_CATEGORIES:
         builder.row(InlineKeyboardButton(text=f"⚠️ Too Many ({len(selected_categories)}/{MAX_CATEGORIES}) - Click to Confirm", callback_data="category_done"))
    builder.row(InlineKeyboardButton(text="❌ Cancel Selection", callback_data="category_cancel"))
    return builder.as_markup()

async def get_comment_reactions(comment_id: int) -> Tuple[int, int]:
    row = await fetch_one(
        "SELECT COALESCE(SUM(CASE WHEN reaction_type = 'like' THEN 1 ELSE 0 END), 0) AS likes, "
        "COALESCE(SUM(CASE WHEN reaction_type = 'dislike' THEN 1 ELSE 0 END), 0) AS dislikes "
        "FROM reactions WHERE comment_id = $1", comment_id
    )
    return (row['likes'], row['dislikes']) if row else (0, 0)

async def get_user_points(user_id: int) -> int:
    row = await fetch_one("SELECT points FROM user_points WHERE user_id = $1", user_id)
    return row['points'] if row else 0

async def update_user_points(user_id: int, delta: int):
    if delta == 0:
        return
    await execute_update(
        "INSERT INTO user_points (user_id, points) VALUES ($1, $2) "
        "ON CONFLICT (user_id) DO UPDATE SET points = user_points.points + $2",
        user_id, delta
    )
async def build_comment_keyboard(comment_id: int, commenter_user_id: int, viewer_user_id: int, confession_owner_id: int, is_admin: bool = False):
    likes, dislikes = await get_comment_reactions(comment_id)
    builder = InlineKeyboardBuilder()
    
    # Only reactions and reply - NO profile, NO report, NO contact request
    if commenter_user_id != viewer_user_id:
        builder.button(text=f"👍 {likes}", callback_data=f"react_like_{comment_id}")
        builder.button(text=f"👎 {dislikes}", callback_data=f"react_dislike_{comment_id}")
    else:
        builder.button(text=f"👍 {likes}", callback_data="noop")
        builder.button(text=f"👎 {dislikes}", callback_data="noop")
    
    builder.button(text="↪️ Reply", callback_data=f"reply_{comment_id}")
    
    # REMOVED: Report button and Contact Request button
    # Now just 3 buttons in one row
    builder.adjust(3)
    
    return builder.as_markup()

async def safe_send_message(user_id: int, text: str, **kwargs) -> Optional[types.Message]:
    try:
        return await bot.send_message(user_id, text, **kwargs)
    except (TelegramForbiddenError, TelegramBadRequest) as e:
        if "bot was blocked" in str(e) or "user is deactivated" in str(e) or "chat not found" in str(e):
            logger.warning(f"Could not send message to user {user_id}: Blocked/deactivated. {e}")
        else:
            logger.warning(f"Telegram API error sending to {user_id}: {e}")
    except TelegramRetryAfter as e:
        logger.warning(f"Flood control for {user_id}. Retrying after {e.retry_after}s")
        await asyncio.sleep(e.retry_after)
        return await safe_send_message(user_id, text, **kwargs)
    except Exception as e:
        logger.error(f"Unexpected error sending message to {user_id}: {e}", exc_info=True)
    return None



# --- Batch User Info Helper ---
async def get_batch_user_profiles(user_ids: List[int]) -> Dict[int, Tuple[str, int]]:
    """Get multiple users' profile info in one query"""
    if not user_ids:
        return {}
    
    # Remove duplicates
    unique_ids = list(set(user_ids))
    
    rows = await execute_query("""
        SELECT 
            u.user_id,
            COALESCE(us.profile_name, 'Anonymous') as profile_name,
            COALESCE(up.points, 0) as points
        FROM unnest($1::bigint[]) u(user_id)
        LEFT JOIN user_status us ON u.user_id = us.user_id
        LEFT JOIN user_points up ON u.user_id = up.user_id
    """, unique_ids)
    
    return {row['user_id']: (row['profile_name'], row['points']) for row in rows}

async def update_channel_post_button(confession_id: int):
    if not bot_info or not CHANNEL_ID:
        return
    conf_data = await fetch_one("SELECT message_id FROM confessions WHERE id = $1 AND status = 'approved'", confession_id)
    count_row = await fetch_one("SELECT COUNT(*) as count FROM comments WHERE confession_id = $1", confession_id)
    count = count_row['count'] if count_row else 0
    
    if not conf_data or not conf_data['message_id']:
        return
    
    ch_msg_id = conf_data['message_id']
    link = f"https://t.me/{bot_info.username}?start=view_{confession_id}"
    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"💬 View / Add Comments ({count})", url=link)]])
    
    try:
        await bot.edit_message_reply_markup(chat_id=CHANNEL_ID, message_id=ch_msg_id, reply_markup=markup)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            logger.error(f"Failed edit channel post {ch_msg_id} for conf {confession_id}: {e}")
    except Exception as e:
        logger.error(f"Unexpected err updating btn for conf {confession_id}: {e}", exc_info=True)

async def get_comment_sequence_number(confession_id: int, comment_id: int) -> Optional[int]:
    row = await fetch_one("""
        WITH ranked_comments AS (
            SELECT id, ROW_NUMBER() OVER (ORDER BY created_at ASC) as rn
            FROM comments
            WHERE confession_id = $1
        )
        SELECT rn FROM ranked_comments WHERE id = $2;
    """, confession_id, comment_id)
    return row['rn'] if row else None

async def show_comments_for_confession(user_id: int, confession_id: int, message_to_edit: Optional[types.Message] = None, page: int = 1):
    logger.info(f"📝 SHOW COMMENTS - User: {user_id}, Confession: {confession_id}, Page: {page}")
    
    conf_data = await fetch_one("SELECT status, user_id FROM confessions WHERE id = $1", confession_id)
    
    if not conf_data or conf_data['status'] != 'approved':
        err_txt = f"Confession #{confession_id} not found or not approved."
        if message_to_edit:
            await message_to_edit.edit_text(err_txt, reply_markup=None)
        else:
            await safe_send_message(user_id, err_txt)
        return
    
    confession_owner_id = conf_data['user_id']
    total_row = await fetch_one("SELECT COUNT(*) as count FROM comments WHERE confession_id = $1", confession_id)
    total_count = total_row['count'] if total_row else 0
    
    if total_count == 0:
        msg_text = "<i>No comments yet. Be the first!</i>"
        if message_to_edit:
            await message_to_edit.edit_text(msg_text, reply_markup=None)
        else:
            await safe_send_message(user_id, msg_text)
        nav = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Add Comment", callback_data=f"add_{confession_id}")]])
        await safe_send_message(user_id, "You can add your own comment below:", reply_markup=nav)
        return
    
    total_pages = (total_count + PAGE_SIZE - 1) // PAGE_SIZE
    page = max(1, min(page, total_pages))
    offset = (page - 1) * PAGE_SIZE
    
    comments_raw = await execute_query("""
        SELECT c.id, c.user_id, c.text, c.sticker_file_id, c.animation_file_id, c.parent_comment_id, c.created_at
        FROM comments c 
        WHERE c.confession_id = $1 
        ORDER BY c.created_at ASC LIMIT $2 OFFSET $3
    """, confession_id, PAGE_SIZE, offset)
    
    # OPTIMIZATION: Batch load all user profiles in ONE query
    commenter_ids = [c['user_id'] for c in comments_raw]
    commenter_ids.append(confession_owner_id)
    users_info = await get_batch_user_profiles(commenter_ids)
    
    is_admin_user = await is_admin(user_id)
    
    # Get all comment IDs to build parent mapping
    comment_ids = [c['id'] for c in comments_raw]
    
    # FIXED: Pre-fetch all reactions for these comments in one query
    reactions_rows = []
    if comment_ids:
        reactions_rows = await execute_query("""
            SELECT comment_id, 
                   COALESCE(SUM(CASE WHEN reaction_type = 'like' THEN 1 ELSE 0 END), 0) as likes,
                   COALESCE(SUM(CASE WHEN reaction_type = 'dislike' THEN 1 ELSE 0 END), 0) as dislikes
            FROM reactions
            WHERE comment_id = ANY($1::int[])
            GROUP BY comment_id
        """, comment_ids)
    
    # Build a map of comment_id -> (likes, dislikes)
    reaction_map = {}
    for row in reactions_rows:
        reaction_map[row['comment_id']] = (row['likes'], row['dislikes'])
    
    if not comments_raw:
        await safe_send_message(user_id, f"<i>No comments on page {page}.</i>")
    else:
        # Send comments IN ORDER (not parallel) to maintain sequence
        message_id_map = {}  # Map comment_id to the sent message ID for replies
        
        for i, c_data in enumerate(comments_raw):
            seq_num = offset + i + 1
            
            # Send comment and store the message ID
            sent_message = await send_single_comment_ordered(
                user_id, i, c_data, confession_id, confession_owner_id,
                users_info, offset, is_admin_user, page, total_pages,
                reaction_map.get(c_data['id'], (0, 0)),
                message_id_map  # Pass the map of parent message IDs
            )
            
            if sent_message:
                message_id_map[c_data['id']] = sent_message.message_id
            
            # REMOVE this delay or reduce it significantly
            # await asyncio.sleep(0.1)  # COMMENT THIS OUT FOR NOW
    
    # Navigation after all comments are sent
    nav_row = []
    if page > 1:
        nav_row.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"comments_page_{confession_id}_{page-1}"))
    if total_pages > 1:
        nav_row.append(InlineKeyboardButton(text=f"Page {page}/{total_pages}", callback_data="noop"))
    if page < total_pages:
        nav_row.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"comments_page_{confession_id}_{page+1}"))
    
    nav_keyboard = InlineKeyboardMarkup(inline_keyboard=[nav_row, [InlineKeyboardButton(text="➕ Add Comment", callback_data=f"add_{confession_id}")]])
    end_txt = f"--- Showing comments {offset+1} to {min(offset+PAGE_SIZE, total_count)} of {total_count} for Confession #{confession_id} ---"
    await safe_send_message(user_id, end_txt, reply_markup=nav_keyboard)

# NEW helper function for sending single comment
async def send_single_comment_ordered(user_id: int, index: int, c_data: dict, confession_id: int, 
                                      confession_owner_id: int, users_info: dict, offset: int, 
                                      is_admin_user: bool, page: int, total_pages: int,
                                      reaction_counts: Tuple[int, int],
                                      message_id_map: Dict[int, int]) -> Optional[types.Message]:
    """Send a single comment in order with proper reply threading"""
    seq_num = offset + index + 1
    db_id = c_data['id']
    commenter_uid = c_data['user_id']
    parent_comment_id = c_data['parent_comment_id']
    
    # Get user info from pre-loaded batch data
    profile_name, points = users_info.get(commenter_uid, ("Anonymous", 0))
    
    tag_parts = []
    if commenter_uid == confession_owner_id:
        tag_parts.append("👑 Author")
    if commenter_uid == user_id:
        tag_parts.append("👤 You")
    tag_str = f" ({', '.join(tag_parts)})" if tag_parts else ""
    
    # Generate profile link
    encoded_profile_link = encode_user_id(commenter_uid)
    profile_link = f"https://t.me/{bot_info.username}?start=profile_{encoded_profile_link}"
    display_name = f"<a href='{profile_link}'>{profile_name}</a> 🏅{points}{tag_str}"
    admin_info = f" [UID: <code>{commenter_uid}</code>]" if is_admin_user else ""
    
    # Build keyboard with reactions
    likes, dislikes = reaction_counts
    builder = InlineKeyboardBuilder()
    
    if commenter_uid != user_id:
        builder.button(text=f"👍 {likes}", callback_data=f"react_like_{db_id}")
        builder.button(text=f"👎 {dislikes}", callback_data=f"react_dislike_{db_id}")
    else:
        builder.button(text=f"👍 {likes}", callback_data="noop")
        builder.button(text=f"👎 {dislikes}", callback_data="noop")
    
    builder.button(text="↪️ Reply", callback_data=f"reply_{db_id}")
    builder.adjust(3)
    keyboard = builder.as_markup()
    
    # Determine reply-to message for threading
    reply_to_message_id = None
    reply_prefix = ""
    
    if parent_comment_id:
        if parent_comment_id in message_id_map:
            # We have the message ID for the parent comment
            reply_to_message_id = message_id_map[parent_comment_id]
            reply_prefix = f"↪️ <i>Replying to comment #{await get_comment_sequence_number(confession_id, parent_comment_id)}</i>\n\n"
        else:
            # Parent comment is on a different page, just show text reference
            parent_seq = await get_comment_sequence_number(confession_id, parent_comment_id)
            reply_prefix = f"↪️ <i>Replying to comment #{parent_seq}</i>\n\n"
    
    try:
        sent_message = None
        
        if c_data['sticker_file_id']:
            # Send sticker and store the message
            sticker_msg = await bot.send_sticker(
                user_id, 
                sticker=c_data['sticker_file_id'],
                reply_to_message_id=reply_to_message_id
            )
            
            # Log that we sent a sticker (optional)
            logger.debug(f"Sent sticker for comment #{seq_num}: {sticker_msg.message_id}")
            
            # Then send the attribution message
            text_content = f"{reply_prefix}{display_name}{admin_info}"
            text_msg = await bot.send_message(
                user_id, 
                text_content, 
                reply_markup=keyboard
            )
            
            # Store the text message ID for future replies (not the sticker)
            sent_message = text_msg
            
        elif c_data['animation_file_id']:
            # Send animation and store the message
            animation_msg = await bot.send_animation(
                user_id, 
                animation=c_data['animation_file_id'],
                reply_to_message_id=reply_to_message_id
            )
            
            # Log that we sent an animation (optional)
            logger.debug(f"Sent animation for comment #{seq_num}: {animation_msg.message_id}")
            
            # Then send the attribution message
            text_content = f"{reply_prefix}{display_name}{admin_info}"
            text_msg = await bot.send_message(
                user_id, 
                text_content, 
                reply_markup=keyboard
            )
            
            # Store the text message ID for future replies
            sent_message = text_msg
            
        elif c_data['text']:
            # Combine everything into one message for text comments
            full_text = f"{reply_prefix}💬 {html.quote(c_data['text'])}\n\n{display_name}{admin_info}"
            sent_message = await bot.send_message(
                user_id, 
                full_text, 
                reply_markup=keyboard, 
                disable_web_page_preview=True,
                reply_to_message_id=reply_to_message_id
            )
        
        return sent_message
        
    except Exception as e:
        logger.warning(f"Could not send comment #{seq_num} to {user_id}: {e}")
        return None

def create_profile_pagination_keyboard(base_callback: str, current_page: int, total_pages: int):
    builder = InlineKeyboardBuilder()
    row = []
    if current_page > 1:
        row.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"{base_callback}_{current_page - 1}"))
    if total_pages > 1:
        row.append(InlineKeyboardButton(text=f"Page {current_page}/{total_pages}", callback_data="noop"))
    if current_page < total_pages:
        row.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"{base_callback}_{current_page + 1}"))
    if row:
        builder.row(*row)
    builder.row(InlineKeyboardButton(text="⬅️ Back to Profile", callback_data="profile_main"))
    return builder.as_markup()


# --- Start Command ---
@dp.message(Command("start"))
async def start(message: types.Message, state: FSMContext, command: Optional[CommandObject] = None):
    await state.clear()
    user_id = message.from_user.id

    row = await fetch_one("SELECT has_accepted_rules FROM user_status WHERE user_id = $1", user_id)
    has_accepted = row['has_accepted_rules'] if row else False

    if not has_accepted:
        rules_text = (
            "<b>📜 Bot Rules & Regulations</b>\n\n"
            "<b>To keep the community safe, respectful, and meaningful, please follow these guidelines:</b>\n\n"
            "1. <b>Stay Relevant:</b> This space is mainly for sharing confessions, experiences, and thoughts.\n\n"
            "2. <b>Respectful Communication:</b> Sensitive topics are allowed but must be discussed with respect.\n\n"
            "3. <b>No Harmful Content:</b> You may mention names, but at your own risk.\n\n"
            "4. <b>Names & Responsibility:</b> Do not share personal identifying information about yourself or others.\n\n"
            "5. <b>Anonymity & Privacy:</b> Don't reveal private details of others without consent.\n\n"
            "6. <b>Constructive Environment:</b> Keep confessions genuine. Avoid spam, trolling, or repeated submissions.\n\n"
            "<i>Use this space to connect, share, and learn.</i>"
        )
        accept_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ I Accept the Rules", callback_data="accept_rules")]
        ])
        await message.answer(rules_text, reply_markup=accept_keyboard)
        return

    deep_link_args = command.args if command else None
    if deep_link_args:
        if deep_link_args.startswith("view_"):
            try:
                conf_id = int(deep_link_args.split("_", 1)[1])
                conf_data = await fetch_one("""
                    SELECT c.text, c.categories, c.status, c.user_id, c.photo_file_id, COUNT(com.id) as comment_count 
                    FROM confessions c LEFT JOIN comments com ON c.id = com.confession_id
                    WHERE c.id = $1 GROUP BY c.id
                """, conf_id)
                
                if not conf_data or conf_data['status'] != 'approved':
                    await message.answer(f"Confession #{conf_id} not found or not approved.")
                    return
                
                comm_count = conf_data['comment_count']
                categories = conf_data['categories'] or []
                category_tags = " ".join([f"#{html.quote(cat)}" for cat in categories]) if categories else "#Unknown"
                
                if conf_data['photo_file_id']:
                    caption = f"<b>Confession #{conf_id}</b>\n\n{html.quote(conf_data['text'])}\n\n{category_tags}\n---"
                    builder = InlineKeyboardBuilder()
                    builder.button(text="➕ Add Comment", callback_data=f"add_{conf_id}")
                    builder.button(text=f"💬 Browse Comments ({comm_count})", callback_data=f"browse_{conf_id}")
                    builder.adjust(1, 1)
                    await bot.send_photo(chat_id=user_id, photo=conf_data['photo_file_id'], caption=caption, reply_markup=builder.as_markup())
                else:
                    txt = f"<b>Confession #{conf_id}</b>\n\n{html.quote(conf_data['text'])}\n\n{category_tags}\n---"
                    builder = InlineKeyboardBuilder()
                    builder.button(text="➕ Add Comment", callback_data=f"add_{conf_id}")
                    builder.button(text=f"💬 Browse Comments ({comm_count})", callback_data=f"browse_{conf_id}")
                    builder.adjust(1, 1)
                    await message.answer(txt, reply_markup=builder.as_markup())  # ← FIXED: removed extra spaces
            except (ValueError, IndexError):
                await message.answer("Invalid link.")
            except Exception as e:
                logger.error(f"Error handling deep link '{deep_link_args}': {e}")
                await message.answer("Error processing link.")
                
        elif deep_link_args.startswith("profile_"):
            try:
                encoded_user_id = deep_link_args.split("_", 1)[1]
                target_user_id = await get_user_id_from_encoded(encoded_user_id)
                
                if not target_user_id:
                    await message.answer("Profile not found or link expired.")
                    return
                
                if target_user_id == user_id:
                    await user_profile(message)
                    return
                
                # Get user info
                profile_name = await get_profile_name(target_user_id)
                points = await get_user_points(target_user_id)
                
                # Check if already have active chat
                existing_chat = await fetch_one("""
                    SELECT id FROM active_chats 
                    WHERE ((user1_id = $1 AND user2_id = $2) OR (user1_id = $2 AND user2_id = $1)) 
                    AND is_active = 1
                """, user_id, target_user_id)
                
                # Check if pending request exists
                pending_req = await fetch_one("""
                    SELECT id FROM contact_requests 
                    WHERE requester_user_id = $1 AND requested_user_id = $2 AND status = 'pending'
                """, user_id, target_user_id)
                
                # Check if approved request exists
                approved_req = await fetch_one("""
                    SELECT id FROM contact_requests 
                    WHERE ((requester_user_id = $1 AND requested_user_id = $2) 
                    OR (requester_user_id = $2 AND requested_user_id = $1))
                    AND status = 'approved'
                """, user_id, target_user_id)
                
                profile_text = f"👤 <b>User Profile</b>\n\n"
                profile_text += f"📛 <b>Display Name:</b> {profile_name}\n"
                profile_text += f"🏅 <b>Aura Points:</b> {points}\n\n"
                
                keyboard = InlineKeyboardBuilder()
                
                if existing_chat:
                    profile_text += "<i>You have an active chat with this user.</i>"
                    keyboard.button(text="💬 Go to Chat", callback_data=f"view_chat_{existing_chat['id']}")
                elif pending_req:
                    profile_text += "<i>You have a pending contact request with this user.</i>"
                    keyboard.button(text="⏳ Request Pending", callback_data="noop")
                elif approved_req:
                    profile_text += "<i>Contact approved! Start chatting.</i>"
                    keyboard.button(text="💬 Start Chat", callback_data=f"start_chat_{target_user_id}")
                else:
                    profile_text += "<i>You can request to chat with this user.</i>"
                    keyboard.button(text="🤝 Request Contact", callback_data=f"req_contact_profile_{target_user_id}")
                
                # Report user button
                if user_id != target_user_id:
                    keyboard.button(text="⚠️ Report User", callback_data=f"report_user_{target_user_id}")
                
                keyboard.button(text="⬅️ Back", callback_data="noop")
                keyboard.adjust(1)
                
                await message.answer(profile_text, reply_markup=keyboard.as_markup())
                
            except Exception as e:
                logger.error(f"Error handling profile deep link: {e}")
                await message.answer("Error processing profile link.")
    
    else:  # ← This else should be aligned with "if deep_link_args:"
        profile_name = await get_profile_name(user_id)
        points = await get_user_points(user_id)
        
        welcome_text = (
            f"👋 Welcome back, <b>{profile_name}</b>!\n\n"
            f"🏅 <b>Your Aura:</b> {points}\n\n"
            "<b>Available Commands:</b>\n"
            "🔹 /confess - Submit anonymous confession\n"
            "🔹 /profile - View and manage your profile\n"
            "🔹 /help - Show all commands\n"
            "🔹 /rules - View bot rules\n"
            "🔹 /privacy - Privacy information\n"
            "🔹 /cancel - Cancel current action\n"
            "🔹 /endchat - End current chat"
        )
        await message.answer(welcome_text, reply_markup=ReplyKeyboardRemove())

@dp.callback_query(F.data == "accept_rules")
async def handle_accept_rules(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    await execute_update(
        "INSERT INTO user_status (user_id, has_accepted_rules, profile_name) VALUES ($1, TRUE, 'Anonymous') "
        "ON CONFLICT (user_id) DO UPDATE SET has_accepted_rules = TRUE",
        user_id
    )
    
    await callback_query.message.edit_text(
        "✅ <b>Rules Accepted!</b>\n\n"
        "Welcome to the confession bot!\n\n"
        "Use /confess to share anonymously, /profile to customize your profile.",
        reply_markup=None
    )
    await callback_query.answer("Rules accepted!")

# --- Help Command ---
@dp.message(Command("help"))
async def help_command(message: types.Message):
    help_text = (
        "🤖 <b>Confession Bot Help</b>\n\n"
        "<b>Main Commands:</b>\n"
        "📝 /confess - Submit an anonymous confession\n"
        "👤 /profile - View and manage your profile\n"
        "📜 /rules - View the bot's rules\n"
        "🔒 /privacy - View privacy information\n"
        "❌ /cancel - Cancel current action\n"
        "💬 /endchat - End current chat\n\n"
        "<b>Profile Features:</b>\n"
        "• View your aura points\n"
        "• Change your display name\n"
        "• See your confessions and comments\n"
        "• Manage active chats\n"
        "• Request contact with other users\n\n"
        "<i>Click on user names in comments to view their profiles.</i>"
    )
    
    if await is_admin(message.from_user.id):
        help_text += "\n\n<b>Admin Commands:</b>\n/admin - Admin panel\n/id - Get user info\n/warn - Warn a user\n/block - Block user\n/unblock - Unblock user\n/stats - Bot statistics\n/broadcast - Broadcast message"
    
    await message.answer(help_text)

# --- Rules Command ---
@dp.message(Command("rules"))
async def rules_command(message: types.Message):
    rules_text = (
        "<b>📜 Bot Rules & Regulations</b>\n\n"
        "1. <b>Stay Relevant:</b> This space is for sharing confessions, experiences, and thoughts.\n\n"
        "2. <b>Respectful Communication:</b> Sensitive topics are allowed but must be discussed with respect.\n\n"
        "3. <b>No Harmful Content:</b> You may mention names, but at your own risk.\n\n"
        "4. <b>Names & Responsibility:</b> Do not share personal identifying information.\n\n"
        "5. <b>Anonymity & Privacy:</b> Don't reveal private details of others without consent.\n\n"
        "6. <b>Constructive Environment:</b> Keep confessions genuine. Avoid spam or repeated submissions."
    )
    await message.answer(rules_text)

# --- Privacy Command ---
@dp.message(Command("privacy"))
async def privacy_command(message: types.Message):
    privacy_text = (
        "🔒 <b>Privacy Information</b>\n\n"
        "<b>What we store:</b>\n"
        "• Your Telegram User ID\n"
        "• Confessions you submit (anonymous)\n"
        "• Comments you make (with display name)\n"
        "• Your display name preference\n"
        "• Your aura points\n\n"
        "<b>What we don't store:</b>\n"
        "• Your phone number\n"
        "• Your profile photo\n"
        "• Personal identifying information\n\n"
        "<i>By using this bot, you agree to these privacy terms.</i>"
    )
    await message.answer(privacy_text)

# --- Cancel Command ---
@dp.message(Command("cancel"))
async def cancel_command(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        await message.answer("No active operation to cancel.")
        return
    await state.clear()
    await message.answer("❌ Operation cancelled.", reply_markup=ReplyKeyboardRemove())

# --- End Chat Command ---
@dp.message(Command("endchat"))
async def end_chat_command(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state != ChatForm.chatting:
        await message.answer("You are not in a chat.")
        return
    
    state_data = await state.get_data()
    chat_id = state_data.get('chat_id')
    
    if chat_id:
        await execute_update("UPDATE active_chats SET is_active = 0 WHERE id = $1", chat_id)
        await state.clear()
        await message.answer("✅ Chat ended.")
    else:
        await state.clear()
        await message.answer("Chat ended.")

# --- Profile Command and Handlers ---
@dp.message(Command("profile"))
async def user_profile(message: types.Message):
    user_id = message.from_user.id
    row = await fetch_one("SELECT has_accepted_rules FROM user_status WHERE user_id = $1", user_id)
    has_accepted = row['has_accepted_rules'] if row else False
    
    if not has_accepted:
        await message.answer("⚠️ Please use /start first to accept the rules.")
        return
    
    points = await get_user_points(user_id)
    profile_name = await get_profile_name(user_id)
    
    profile_text = f"👤 <b>Your Profile</b>\n\n"
    profile_text += f"🏅 <b>Aura Points:</b> {points}\n"
    profile_text += f"👁️ <b>Display Name:</b> {profile_name}\n\n"
    profile_text += "<b>What would you like to do?</b>"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Change Display Name", callback_data="change_profile_name")],
        [InlineKeyboardButton(text="📜 My Confessions", callback_data="profile_confessions_1")],
        [InlineKeyboardButton(text="💬 My Comments", callback_data="profile_comments_1")],
        [InlineKeyboardButton(text="💬 My Active Chats", callback_data="my_active_chats")],
        [InlineKeyboardButton(text="📨 Pending Contact Requests", callback_data="pending_contact_requests")]
    ])
    
    await message.answer(profile_text, reply_markup=keyboard)

@dp.callback_query(F.data == "profile_main")
async def back_to_profile(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    points = await get_user_points(user_id)
    profile_name = await get_profile_name(user_id)
    
    profile_text = f"👤 <b>Your Profile</b>\n\n"
    profile_text += f"🏅 <b>Aura Points:</b> {points}\n"
    profile_text += f"👁️ <b>Display Name:</b> {profile_name}\n\n"
    profile_text += "<b>What would you like to do?</b>"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Change Display Name", callback_data="change_profile_name")],
        [InlineKeyboardButton(text="📜 My Confessions", callback_data="profile_confessions_1")],
        [InlineKeyboardButton(text="💬 My Comments", callback_data="profile_comments_1")],
        [InlineKeyboardButton(text="💬 My Active Chats", callback_data="my_active_chats")],
        [InlineKeyboardButton(text="📨 Pending Contact Requests", callback_data="pending_contact_requests")]
    ])
    
    await callback_query.message.edit_text(profile_text, reply_markup=keyboard)
    await callback_query.answer()

@dp.callback_query(F.data == "change_profile_name")
async def change_profile_name_start(callback_query: types.CallbackQuery, state: FSMContext):
    await state.set_state(UserProfileForm.waiting_for_profile_name)
    await callback_query.message.answer("Please enter your new display name (max 32 characters):")
    await callback_query.answer()

@dp.message(UserProfileForm.waiting_for_profile_name, F.text)
async def receive_profile_name(message: types.Message, state: FSMContext):
    profile_name = message.text.strip()
    
    if len(profile_name) > 32:
        await message.answer("Profile name too long. Maximum 32 characters. Please try again:")
        return
    if len(profile_name) < 2:
        await message.answer("Profile name too short. Minimum 2 characters. Please try again:")
        return
    if not re.match(r'^[a-zA-Z0-9_ ]+$', profile_name):
        await message.answer("Profile name can only contain letters, numbers, spaces and underscores. Please try again:")
        return
    
    await execute_update(
        "INSERT INTO user_status (user_id, profile_name) VALUES ($1, $2) "
        "ON CONFLICT (user_id) DO UPDATE SET profile_name = EXCLUDED.profile_name",
        message.from_user.id, profile_name
    )
    # Add this line after the database update:
    profile_cache.invalidate(message.from_user.id)
    await message.answer(f"✅ Your display name has been updated to: <b>{html.quote(profile_name)}</b>")
    await state.clear()

@dp.callback_query(F.data.startswith("profile_confessions_"))
async def show_user_confessions(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    try:
        page = int(callback_query.data.split("_")[-1])
    except ValueError:
        page = 1
    
    total_row = await fetch_one("SELECT COUNT(*) as count FROM confessions WHERE user_id = $1", user_id)
    total_count = total_row['count'] if total_row else 0
    
    if total_count == 0:
        await callback_query.message.edit_text(
            "📭 <b>Your Confessions</b>\n\nYou haven't submitted any confessions yet.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Back to Profile", callback_data="profile_main")]
            ])
        )
        await callback_query.answer()
        return
    
    total_pages = (total_count + 5 - 1) // 5
    page = max(1, min(page, total_pages))
    offset = (page - 1) * 5
    
    confessions = await execute_query("""
        SELECT id, text, status, created_at, photo_file_id
        FROM confessions 
        WHERE user_id = $1 
        ORDER BY created_at DESC 
        LIMIT 5 OFFSET $2
    """, user_id, offset)
    
    response_text = f"<b>📜 Your Confessions (Page {page}/{total_pages})</b>\n\n"
    builder = InlineKeyboardBuilder()
    
    for conf in confessions:
        snippet = html.quote(conf['text'][:60]) + ('...' if len(conf['text']) > 60 else '')
        status_emoji = {"approved": "✅", "pending": "⏳", "rejected": "❌", "deleted": "🗑️"}.get(conf['status'], "❓")
        photo_indicator = " 📷" if conf['photo_file_id'] else ""
        response_text += f"<b>ID:</b> #{conf['id']} ({status_emoji} {conf['status'].capitalize()}{photo_indicator})\n"
        response_text += f"<i>\"{snippet}\"</i>\n\n"
        
        if conf['status'] in ['approved', 'pending']:
            builder.row(InlineKeyboardButton(text=f"🗑️ Request Deletion for #{conf['id']}", callback_data=f"req_del_conf_{conf['id']}"))
    
    nav_keyboard = create_profile_pagination_keyboard("profile_confessions", page, total_pages)
    final_markup = builder.attach(InlineKeyboardBuilder.from_markup(nav_keyboard)).as_markup()
    
    await callback_query.message.edit_text(response_text, reply_markup=final_markup)
    await callback_query.answer()

@dp.callback_query(F.data.startswith("profile_comments_"))
async def show_user_comments(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    try:
        page = int(callback_query.data.split("_")[-1])
    except ValueError:
        page = 1
    
    total_row = await fetch_one("SELECT COUNT(*) as count FROM comments WHERE user_id = $1", user_id)
    total_count = total_row['count'] if total_row else 0
    
    if total_count == 0:
        await callback_query.message.edit_text(
            "💬 <b>Your Comments</b>\n\nYou haven't made any comments yet.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Back to Profile", callback_data="profile_main")]
            ])
        )
        await callback_query.answer()
        return
    
    total_pages = (total_count + 5 - 1) // 5
    page = max(1, min(page, total_pages))
    offset = (page - 1) * 5
    
    comments = await execute_query("""
        SELECT c.id, c.text, c.sticker_file_id, c.animation_file_id, c.confession_id, c.created_at,
               conf.text as confession_text
        FROM comments c
        LEFT JOIN confessions conf ON c.confession_id = conf.id
        WHERE c.user_id = $1
        ORDER BY c.created_at DESC 
        LIMIT 5 OFFSET $2
    """, user_id, offset)
    
    response_text = f"<b>💬 Your Comments (Page {page}/{total_pages})</b>\n\n"
    
    for comm in comments:
        if comm['text']:
            snippet = "💬 " + html.quote(comm['text'][:60]) + ('...' if len(comm['text']) > 60 else '')
        elif comm['sticker_file_id']:
            snippet = "[Sticker]"
        elif comm['animation_file_id']:
            snippet = "[GIF]"
        else:
            snippet = "[Unknown Content]"
        
        conf_snippet = html.quote(comm['confession_text'][:40]) + ('...' if len(comm['confession_text']) > 40 else '') if comm['confession_text'] else "Unknown"
        link = f"https://t.me/{bot_info.username}?start=view_{comm['confession_id']}"
        response_text += f"<b>On Confession:</b> <a href='{link}'>#{comm['confession_id']}</a>\n"
        response_text += f"<i>\"{conf_snippet}\"</i>\n"
        response_text += f"<b>Your comment:</b> {snippet}\n\n"
    
    nav_keyboard = create_profile_pagination_keyboard("profile_comments", page, total_pages)
    await callback_query.message.edit_text(response_text, reply_markup=nav_keyboard, disable_web_page_preview=True)
    await callback_query.answer()

@dp.callback_query(F.data.startswith("req_del_conf_"))
async def request_deletion_prompt(callback_query: types.CallbackQuery):
    conf_id = int(callback_query.data.split("_")[-1])
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Yes, Request Deletion", callback_data=f"confirm_del_conf_{conf_id}")],
        [InlineKeyboardButton(text="❌ No, Cancel", callback_data="profile_main")]
    ])
    await callback_query.message.edit_text(
        f"Are you sure you want to request the deletion of Confession #{conf_id}?",
        reply_markup=keyboard
    )
    await callback_query.answer()

@dp.callback_query(F.data.startswith("confirm_del_conf_"))
async def confirm_deletion_request(callback_query: types.CallbackQuery):
    conf_id = int(callback_query.data.split("_")[-1])
    user_id = callback_query.from_user.id
    
    conf_data = await fetch_one("SELECT user_id, text, status FROM confessions WHERE id = $1", conf_id)
    
    if not conf_data or conf_data['user_id'] != user_id:
        await callback_query.answer("This is not your confession.", show_alert=True)
        return
    
    if conf_data['status'] not in ['approved', 'pending']:
        await callback_query.answer(f"This confession cannot be deleted.", show_alert=True)
        return
    
    existing_req = await fetch_one("SELECT id FROM deletion_requests WHERE confession_id = $1 AND user_id = $2", conf_id, user_id)
    if existing_req:
        await callback_query.answer("You have already requested deletion.", show_alert=True)
        return
    
    await execute_update(
        "INSERT INTO deletion_requests (confession_id, user_id, status, created_at) VALUES ($1, $2, 'pending', CURRENT_TIMESTAMP)",
        conf_id, user_id
    )
    
    snippet = html.quote(conf_data['text'][:200])
    admin_text = f"🗑️ <b>New Deletion Request</b>\n\n<b>User ID:</b> <code>{user_id}</code>\n<b>Confession ID:</b> <code>{conf_id}</code>\n\n<b>Content Snippet:</b>\n<i>\"{snippet}...\"</i>"
    admin_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Approve Deletion", callback_data=f"admin_approve_delete_{conf_id}")],
        [InlineKeyboardButton(text="❌ Reject Deletion", callback_data=f"admin_reject_delete_{conf_id}")]
    ])
    
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, admin_text, reply_markup=admin_keyboard)
        except Exception as e:
            logger.warning(f"Could not notify admin {admin_id}: {e}")
    
    await callback_query.answer("✅ Deletion request sent.", show_alert=True)
    callback_query.data = "profile_confessions_1"
    await show_user_confessions(callback_query)

@dp.callback_query(F.data == "my_active_chats")
async def show_active_chats(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    
    chats = await execute_query("""
        SELECT ac.id, 
               CASE WHEN ac.user1_id = $1 THEN ac.user2_id ELSE ac.user1_id END as other_user_id,
               us.profile_name as other_user_name,
               ac.last_message_at
        FROM active_chats ac
        LEFT JOIN user_status us ON (CASE WHEN ac.user1_id = $1 THEN ac.user2_id ELSE ac.user1_id END) = us.user_id
        WHERE (ac.user1_id = $1 OR ac.user2_id = $1) AND ac.is_active = 1
        ORDER BY ac.last_message_at DESC
    """, user_id)
    
    if not chats:
        await callback_query.message.edit_text(
            "💬 <b>Your Active Chats</b>\n\nYou have no active chats.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Back to Profile", callback_data="profile_main")]
            ])
        )
        await callback_query.answer()
        return
    
    response_text = "💬 <b>Your Active Chats</b>\n\n"
    keyboard = InlineKeyboardBuilder()
    
    for chat in chats:
        other_user_name = chat['other_user_name'] or "Anonymous"
        last_msg_time = chat['last_message_at'].strftime('%Y-%m-%d %H:%M') if chat['last_message_at'] else "No messages"
        response_text += f"👤 <b>{other_user_name}</b>\n   Last activity: {last_msg_time}\n\n"
        keyboard.button(text=f"💬 Chat with {other_user_name[:15]}", callback_data=f"view_chat_{chat['id']}")
    
    keyboard.button(text="⬅️ Back to Profile", callback_data="profile_main")
    keyboard.adjust(1)
    
    await callback_query.message.edit_text(response_text, reply_markup=keyboard.as_markup())
    await callback_query.answer()

@dp.callback_query(F.data.startswith("view_chat_"))
async def view_chat_messages(callback_query: types.CallbackQuery, state: FSMContext):
    chat_id = int(callback_query.data.split("_")[-1])
    user_id = callback_query.from_user.id
    
    chat_data = await fetch_one("""
        SELECT ac.id, 
               CASE WHEN ac.user1_id = $1 THEN ac.user2_id ELSE ac.user1_id END as other_user_id,
               us.profile_name as other_user_name
        FROM active_chats ac
        LEFT JOIN user_status us ON (CASE WHEN ac.user1_id = $1 THEN ac.user2_id ELSE ac.user1_id END) = us.user_id
        WHERE ac.id = $2 AND (ac.user1_id = $1 OR ac.user2_id = $1) AND ac.is_active = 1
    """, user_id, chat_id)
    
    if not chat_data:
        await callback_query.answer("Chat not found.", show_alert=True)
        return
    
    other_user_id = chat_data['other_user_id']
    other_user_name = chat_data['other_user_name'] or "Anonymous"
    
    await state.set_state(ChatForm.chatting)
    await state.update_data(chat_id=chat_id, other_user_id=other_user_id)
    
    messages = await execute_query("""
        SELECT cm.*, us.profile_name as sender_name
        FROM chat_messages cm
        LEFT JOIN user_status us ON cm.sender_id = us.user_id
        WHERE cm.chat_id = $1
        ORDER BY cm.created_at DESC
        LIMIT 10
    """, chat_id)
    
    response_text = f"💬 <b>Chat with {other_user_name}</b>\n\n"
    
    if not messages:
        response_text += "<i>No messages yet. Start the conversation!</i>\n\n"
    else:
        for msg in reversed(messages):
            sender_name = msg['sender_name'] or ("You" if msg['sender_id'] == user_id else "Anonymous")
            time_str = msg['created_at'].strftime('%H:%M') if msg['created_at'] else ""
            
            if msg['message_text']:
                response_text += f"<b>{sender_name}</b> ({time_str}):\n{html.quote(msg['message_text'])}\n\n"
            elif msg['sticker_file_id']:
                response_text += f"<b>{sender_name}</b> ({time_str}): [Sticker]\n\n"
            elif msg['animation_file_id']:
                response_text += f"<b>{sender_name}</b> ({time_str}): [GIF]\n\n"
    
    response_text += "<i>Send a message below to continue. Type /endchat to disconnect.</i>"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚫 Disconnect Chat", callback_data=f"disconnect_chat_{chat_id}")],
        [InlineKeyboardButton(text="⬅️ Back to Chats", callback_data="my_active_chats")]
    ])
    
    await callback_query.message.edit_text(response_text, reply_markup=keyboard)
    await callback_query.answer()

@dp.message(ChatForm.chatting)
async def handle_chat_message(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    state_data = await state.get_data()
    chat_id = state_data.get('chat_id')
    other_user_id = state_data.get('other_user_id')
    
    if not chat_id:
        await state.clear()
        return
    
    if message.text and message.text.startswith('/'):
        if message.text.startswith('/endchat'):
            await end_chat_command(message, state)
        elif message.text.startswith('/cancel'):
            await cancel_command(message, state)
        elif message.text.startswith('/start'):
            await start(message, state)
        elif message.text.startswith('/help'):
            await help_command(message)
        elif message.text.startswith('/profile'):
            await user_profile(message)
        elif message.text.startswith('/rules'):
            await rules_command(message)
        elif message.text.startswith('/privacy'):
            await privacy_command(message)
        else:
            await message.answer("Commands are not forwarded in chats.")
        return
    
    try:
        if message.text:
            await execute_update("""
                INSERT INTO chat_messages (chat_id, sender_id, message_text)
                VALUES ($1, $2, $3)
            """, chat_id, user_id, message.text)
        elif message.sticker:
            await execute_update("""
                INSERT INTO chat_messages (chat_id, sender_id, sticker_file_id)
                VALUES ($1, $2, $3)
            """, chat_id, user_id, message.sticker.file_id)
        elif message.animation:
            await execute_update("""
                INSERT INTO chat_messages (chat_id, sender_id, animation_file_id)
                VALUES ($1, $2, $3)
            """, chat_id, user_id, message.animation.file_id)
        else:
            await message.answer("Only text, stickers, and GIFs are supported.")
            return
        
        await execute_update("UPDATE active_chats SET last_message_at = CURRENT_TIMESTAMP WHERE id = $1", chat_id)
        
        try:
            if message.text:
                await bot.send_message(
                    other_user_id,
                    f"💬 <b>New message in chat:</b>\n\n{html.quote(message.text)}",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="💬 Go to Chat", callback_data=f"view_chat_{chat_id}")]
                    ])
                )
            elif message.sticker:
                await bot.send_sticker(other_user_id, sticker=message.sticker.file_id)
            elif message.animation:
                await bot.send_animation(other_user_id, animation=message.animation.file_id)
        except Exception as e:
            logger.warning(f"Could not forward message to user {other_user_id}: {e}")
        
        await message.answer("✅ Message sent!")
        
    except Exception as e:
        logger.error(f"Error handling chat message: {e}")
        await message.answer("❌ Error sending message.")

@dp.callback_query(F.data.startswith("disconnect_chat_"))
async def disconnect_chat(callback_query: types.CallbackQuery, state: FSMContext):
    chat_id = int(callback_query.data.split("_")[-1])
    user_id = callback_query.from_user.id
    
    chat_data = await fetch_one("""
        SELECT user1_id, user2_id FROM active_chats 
        WHERE id = $1 AND (user1_id = $2 OR user2_id = $2) AND is_active = 1
    """, chat_id, user_id)
    
    if not chat_data:
        await callback_query.answer("Chat not found.", show_alert=True)
        return
    
    other_user_id = chat_data['user1_id'] if chat_data['user2_id'] == user_id else chat_data['user2_id']
    
    await execute_update("UPDATE active_chats SET is_active = 0 WHERE id = $1", chat_id)
    
    current_state = await state.get_state()
    if current_state == ChatForm.chatting:
        await state.clear()
    
    try:
        await bot.send_message(other_user_id, "⚠️ <b>Chat disconnected</b>\n\nThe other user has ended the chat.")
    except Exception as e:
        logger.warning(f"Could not notify user {other_user_id}: {e}")
    
    await callback_query.message.edit_text(
        "✅ Chat disconnected.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back to Profile", callback_data="profile_main")]
        ])
    )
    await callback_query.answer()

@dp.callback_query(F.data == "pending_contact_requests")
async def show_pending_contact_requests(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    
    requests = await execute_query("""
        SELECT cr.id, cr.requester_user_id, cr.message, cr.created_at, us.profile_name
        FROM contact_requests cr
        LEFT JOIN user_status us ON cr.requester_user_id = us.user_id
        WHERE cr.requested_user_id = $1 AND cr.status = 'pending'
        ORDER BY cr.created_at DESC
    """, user_id)
    
    if not requests:
        await callback_query.message.edit_text(
            "📨 <b>Pending Contact Requests</b>\n\nYou have no pending contact requests.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Back to Profile", callback_data="profile_main")]
            ])
        )
        await callback_query.answer()
        return
    
    response_text = "📨 <b>Pending Chat Requests</b>\n\n"
    keyboard = InlineKeyboardBuilder()
    
    for req in requests:
        profile_name = req['profile_name'] or "Anonymous"
        message = req['message'] or "No message"
        time_str = req['created_at'].strftime('%Y-%m-%d %H:%M') if req['created_at'] else ""
        
        response_text += f"👤 <b>{profile_name}</b>\n"
        response_text += f"<i>Message:</i> {html.quote(message[:50])}{'...' if len(message) > 50 else ''}\n"
        response_text += f"<i>Time:</i> {time_str}\n\n"
        
        # ✅ FIX: Use request ID instead of user ID
        keyboard.row(
            InlineKeyboardButton(text=f"✅ Approve", callback_data=f"approve_contact_{req['id']}"),
            InlineKeyboardButton(text=f"❌ Reject", callback_data=f"reject_contact_{req['id']}")
        )
    
    keyboard.row(InlineKeyboardButton(text="⬅️ Back to Profile", callback_data="profile_main"))
    
    await callback_query.message.edit_text(response_text, reply_markup=keyboard.as_markup())
    await callback_query.answer()

@dp.callback_query(F.data.startswith("view_profile_"))
async def view_user_profile(callback_query: types.CallbackQuery):
    """Show user profile when name is clicked"""
    try:
        # Format: view_profile_123456789
        target_user_id = int(callback_query.data.split("_")[-1])
        viewer_id = callback_query.from_user.id
    except (ValueError, IndexError):
        await callback_query.answer("Invalid profile link.", show_alert=True)
        return
    
    if target_user_id == viewer_id:
        await user_profile(callback_query.message)
        await callback_query.answer()
        return
    
    # Get user info
    profile_name = await get_profile_name(target_user_id)
    points = await get_user_points(target_user_id)
    
    # Check if already have active chat
    existing_chat = await fetch_one("""
        SELECT id FROM active_chats 
        WHERE ((user1_id = $1 AND user2_id = $2) OR (user1_id = $2 AND user2_id = $1)) 
        AND is_active = 1
    """, viewer_id, target_user_id)
    
    # Check if pending request exists
    pending_req = await fetch_one("""
        SELECT id FROM contact_requests 
        WHERE requester_user_id = $1 AND requested_user_id = $2 AND status = 'pending'
    """, viewer_id, target_user_id)
    
    # Check if approved request exists
    approved_req = await fetch_one("""
        SELECT id FROM contact_requests 
        WHERE ((requester_user_id = $1 AND requested_user_id = $2) 
        OR (requester_user_id = $2 AND requested_user_id = $1))
        AND status = 'approved'
    """, viewer_id, target_user_id)
    
    profile_text = f"👤 <b>User Profile</b>\n\n"
    profile_text += f"📛 <b>Display Name:</b> {profile_name}\n"
    profile_text += f"🏅 <b>Aura Points:</b> {points}\n\n"
    
    keyboard = InlineKeyboardBuilder()
    
    if existing_chat:
        profile_text += "<i>You have an active chat with this user.</i>"
        keyboard.button(text="💬 Go to Chat", callback_data=f"view_chat_{existing_chat['id']}")
    elif pending_req:
        profile_text += "<i>You have a pending contact request with this user.</i>"
        keyboard.button(text="⏳ Request Pending", callback_data="noop")
    elif approved_req:
        profile_text += "<i>Contact approved! Start chatting.</i>"
        keyboard.button(text="💬 Start Chat", callback_data=f"start_chat_{target_user_id}")
    else:
        profile_text += "<i>You can request to chat with this user.</i>"
        # Contact request button
        keyboard.button(text="🤝 Request Contact", callback_data=f"req_contact_profile_{target_user_id}")
    
    # Report user button (for all viewers except self)
    if viewer_id != target_user_id:
        keyboard.button(text="⚠️ Report User", callback_data=f"report_user_{target_user_id}")
    
    keyboard.button(text="⬅️ Back", callback_data="noop")
    keyboard.adjust(1)
    
    await callback_query.message.edit_text(profile_text, reply_markup=keyboard.as_markup())
    await callback_query.answer()

@dp.callback_query(F.data.startswith("req_contact_profile_"))
async def handle_profile_contact_request(callback_query: types.CallbackQuery):
    """Handle contact request from profile view"""
    logger.info(f"🔴 REQ CONTACT HANDLER - CALLED with: {callback_query.data}")
    logger.info(f"🔴 REQ CONTACT HANDLER - From user: {callback_query.from_user.id}")
    
    try:
        target_user_id = int(callback_query.data.split("_")[-1])
        requester_id = callback_query.from_user.id
        logger.info(f"🔴 REQ CONTACT HANDLER - Target: {target_user_id}, Requester: {requester_id}")
    except (ValueError, IndexError):
        await callback_query.answer("Invalid request.", show_alert=True)
        return
    
    if target_user_id == requester_id:
        await callback_query.answer("You cannot request contact with yourself.", show_alert=True)
        return
    
    # Check if already have active chat
    existing_chat = await fetch_one("""
        SELECT id FROM active_chats 
        WHERE ((user1_id = $1 AND user2_id = $2) OR (user1_id = $2 AND user2_id = $1)) 
        AND is_active = 1
    """, requester_id, target_user_id)
    
    if existing_chat:
        await callback_query.answer("You already have an active chat with this user.", show_alert=True)
        return
    
    # Check if request already exists
    existing_req = await fetch_one("""
        SELECT id, status FROM contact_requests 
        WHERE requester_user_id = $1 AND requested_user_id = $2
        ORDER BY id DESC LIMIT 1
    """, requester_id, target_user_id)
    
    if existing_req:
        if existing_req['status'] == 'pending':
            await callback_query.answer("You already have a pending request with this user.", show_alert=True)
            return
        elif existing_req['status'] == 'approved':
            await callback_query.answer("You already have an approved contact with this user.", show_alert=True)
            return
    
    # Create contact request with NULL values for confession_id and comment_id
    try:
        req_id = await execute_insert_return_id("""
            INSERT INTO contact_requests 
            (confession_id, comment_id, requester_user_id, requested_user_id, status, message) 
            VALUES (NULL, NULL, $1, $2, 'pending', NULL)
            RETURNING id
        """, requester_id, target_user_id)
        
        if not req_id:
            await callback_query.answer("Error creating request.", show_alert=True)
            return
        
        # Get requester's profile name
        requester_name = await get_profile_name(requester_id)
        
        # UPDATED NOTIFICATION TEXT - Now mentions chat instead of sharing username
        notification = (
            f"🤝 <b>New Chat Request</b>\n\n"
            f"<b>{requester_name}</b> wants to start a private chat with you!\n\n"
            f"Do you want to connect?"
        )
        
        # IMPORTANT: Use request ID in callback, not user ID
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Approve", callback_data=f"approve_contact_{req_id}"),
             InlineKeyboardButton(text="❌ Reject", callback_data=f"reject_contact_{req_id}")]
        ])
        
        sent = await safe_send_message(target_user_id, notification, reply_markup=keyboard)
        
        if sent:
            await callback_query.answer("✅ Chat request sent!")
            await callback_query.message.answer("✅ Your chat request has been sent to the user.")
        else:
            await execute_update("UPDATE contact_requests SET status = 'failed_to_notify' WHERE id = $1", req_id)
            await callback_query.answer("❌ Could not notify the user. They may have blocked the bot.", show_alert=True)
            
    except Exception as e:
        logger.error(f"Error creating chat request: {e}")
        await callback_query.answer("Error processing request", show_alert=True)


@dp.callback_query(F.data.startswith("report_user_"))
async def report_user(callback_query: types.CallbackQuery):
    """Report a user from their profile"""
    try:
        target_user_id = int(callback_query.data.split("_")[-1])
        reporter_id = callback_query.from_user.id
    except (ValueError, IndexError):
        await callback_query.answer("Invalid user ID.", show_alert=True)
        return
    
    if target_user_id == reporter_id:
        await callback_query.answer("You cannot report yourself.", show_alert=True)
        return
    
    # Get user info
    profile_name = await get_profile_name(target_user_id)
    
    # Send report to admins
    admin_text = (
        f"⚠️ <b>User Report</b>\n\n"
        f"<b>Reported User:</b> {profile_name}\n"
        f"<b>Reported User ID:</b> <code>{target_user_id}</code>\n"
        f"<b>Reported By:</b> <code>{reporter_id}</code>\n\n"
        f"<i>User was reported from their profile.</i>"
    )
    
    for admin_id in ADMIN_IDS:
        await safe_send_message(admin_id, admin_text)
    
    await callback_query.answer("✅ User reported. Admins have been notified.", show_alert=True)

@dp.callback_query(F.data.startswith("start_chat_"))
async def start_chat_from_profile(callback_query: types.CallbackQuery, state: FSMContext):
    """Start a chat from profile after approved contact request"""
    try:
        target_user_id = int(callback_query.data.split("_")[-1])
        user_id = callback_query.from_user.id
    except (ValueError, IndexError):
        await callback_query.answer("Invalid user ID.", show_alert=True)
        return
    
    # Check if chat already exists
    existing_chat = await fetch_one("""
        SELECT id FROM active_chats 
        WHERE ((user1_id = $1 AND user2_id = $2) OR (user1_id = $2 AND user2_id = $1)) 
        AND is_active = 1
    """, user_id, target_user_id)
    
    if existing_chat:
        # Go to existing chat
        callback_query.data = f"view_chat_{existing_chat['id']}"
        await view_chat_messages(callback_query, state)
        return
    
    # Create new chat
    chat_id = await execute_insert_return_id("""
        INSERT INTO active_chats (user1_id, user2_id, started_by)
        VALUES ($1, $2, $1)
        RETURNING id
    """, user_id, target_user_id)
    
    if chat_id:
        target_profile = await get_profile_name(target_user_id)
        
        # Notify target user
        try:
            await bot.send_message(
                target_user_id,
                f"💬 <b>New Chat Started</b>\n\n"
                f"{await get_profile_name(user_id)} has started a chat with you!",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="💬 Go to Chat", callback_data=f"view_chat_{chat_id}")]
                ])
            )
        except Exception as e:
            logger.warning(f"Could not notify user {target_user_id} about new chat: {e}")
        
        await callback_query.answer("Chat started!")
        
        # Redirect to chat view
        callback_query.data = f"view_chat_{chat_id}"
        await view_chat_messages(callback_query, state)
    else:
        await callback_query.answer("Error starting chat.", show_alert=True)

# --- Confession Submission Flow ---
@dp.message(Command("confess"), StateFilter(None))
async def start_confession(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    row = await fetch_one("SELECT has_accepted_rules FROM user_status WHERE user_id = $1", user_id)
    has_accepted = row['has_accepted_rules'] if row else False
    
    if not has_accepted:
        await message.answer("⚠️ Please use /start first to accept the rules.")
        return
    
    await state.update_data(selected_categories=[])
    await message.answer(
        f"📝 <b>Confession Submission</b>\n\n"
        f"Please choose 1 to {MAX_CATEGORIES} categories. Click 'Done Selecting' when finished.",
        reply_markup=create_category_keyboard([])
    )
    await state.set_state(ConfessionForm.selecting_categories)

@dp.callback_query(StateFilter(ConfessionForm.selecting_categories), F.data.startswith("category_"))
async def handle_category_selection(callback_query: types.CallbackQuery, state: FSMContext):
    action = callback_query.data.split("_", 1)[1]
    user_data = await state.get_data()
    selected_categories: List[str] = user_data.get("selected_categories", [])
    
    if action == "cancel":
        await state.clear()
        await callback_query.message.edit_text("Confession submission cancelled.", reply_markup=None)
        await callback_query.answer()
        return
    
    if action == "done":
        if not selected_categories:
            await callback_query.answer("Please select at least 1 category.", show_alert=True)
            return
        if len(selected_categories) > MAX_CATEGORIES:
            await callback_query.answer(f"Too many categories (max {MAX_CATEGORIES}).", show_alert=True)
            return
        
        await state.set_state(ConfessionForm.waiting_for_text)
        category_tags = " ".join([f"#{html.quote(cat)}" for cat in selected_categories])
        
        await callback_query.message.edit_text(
            f"✅ <b>Categories selected:</b> {category_tags}\n\n"
            f"📝 <b>Now, send your confession:</b>\n\n"
            f"• Text only: Send your confession text\n"
            f"• Text with photo: Send a photo with caption\n\n"
            f"<i>Type /cancel to abort.</i>"
        )
        await callback_query.answer()
        return
    
    category = action
    if category in CATEGORIES:
        if category in selected_categories:
            selected_categories.remove(category)
        elif len(selected_categories) < MAX_CATEGORIES:
            selected_categories.append(category)
        else:
            await callback_query.answer(f"You can only select up to {MAX_CATEGORIES} categories.", show_alert=True)
            return
        
        await state.update_data(selected_categories=selected_categories)
        await callback_query.message.edit_reply_markup(reply_markup=create_category_keyboard(selected_categories))
        await callback_query.answer(f"'{category}' {'selected' if category in selected_categories else 'deselected'}.")

@dp.message(ConfessionForm.waiting_for_text, F.text)
async def receive_text_confession(message: types.Message, state: FSMContext):
    if message.text.startswith('/'):
        return
    
    if not rate_limiter.check_and_update(message.from_user.id):
        await message.answer(f"⏳ Please wait {RATE_LIMIT_SECONDS} seconds between submissions.")
        return
    
    await process_confession(message, state, text=message.text, photo_file_id=None)

@dp.message(ConfessionForm.waiting_for_text, F.photo)
async def receive_photo_confession(message: types.Message, state: FSMContext):
    if not rate_limiter.check_and_update(message.from_user.id):
        await message.answer(f"⏳ Please wait {RATE_LIMIT_SECONDS} seconds between submissions.")
        return
    
    photo_file_id = message.photo[-1].file_id
    text = message.caption or ""
    
    if not text.strip():
        await message.answer("❌ Please add a caption to your photo.")
        return
    
    file_size_mb = message.photo[-1].file_size / (1024 * 1024) if message.photo[-1].file_size else 0
    if file_size_mb > MAX_PHOTO_SIZE_MB:
        await message.answer(f"❌ Photo is too large ({file_size_mb:.1f}MB). Maximum size is {MAX_PHOTO_SIZE_MB}MB.")
        return
    
    await process_confession(message, state, text=text, photo_file_id=photo_file_id)

async def process_confession(message: types.Message, state: FSMContext, text: str, photo_file_id: Optional[str] = None):
    user_id = message.from_user.id
    state_data = await state.get_data()
    selected_categories: List[str] = state_data.get("selected_categories", [])
    
    if not selected_categories:
        await message.answer("⚠️ Error: Category info lost. Please start again with /confess.")
        await state.clear()
        return
    
    if len(text) < 10:
        await message.answer("❌ Confession too short (minimum 10 characters).")
        return
    
    if len(text) > 3900:
        await message.answer("❌ Confession too long (maximum 3900 characters).")
        return
    
    try:
        query = """
            INSERT INTO confessions (text, user_id, categories, status, photo_file_id) 
            VALUES ($1, $2, $3::text[], 'pending', $4)
            RETURNING id
        """
        conf_id = await execute_insert_return_id(query, text, user_id, selected_categories, photo_file_id)
        
        if not conf_id:
            raise Exception("Failed to get confession ID")
        
        await update_user_points(user_id, POINTS_PER_CONFESSION)
        
        category_tags = " ".join([f"#{html.quote(cat)}" for cat in selected_categories])
        
        if photo_file_id:
            admin_caption = (
                f"🖼️ <b>New Photo Confession Review</b>\n"
                f"<b>ID:</b> {conf_id}\n"
                f"<b>Categories:</b> {category_tags}\n"
                f"<b>User ID:</b> <code>{user_id}</code>\n\n"
                f"<b>Caption:</b>\n{html.quote(text)}"
            )
            admin_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Approve", callback_data=f"approve_{conf_id}"),
                 InlineKeyboardButton(text="❌ Reject", callback_data=f"reject_{conf_id}")]
            ])
            
            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_photo(chat_id=admin_id, photo=photo_file_id, caption=admin_caption, reply_markup=admin_keyboard)
                except Exception as e:
                    logger.warning(f"Could not send to admin {admin_id}: {e}")
        else:
            admin_msg_text = (
                f"📝 <b>New Confession Review</b>\n"
                f"<b>ID:</b> {conf_id}\n"
                f"<b>Categories:</b> {category_tags}\n"
                f"<b>User ID:</b> <code>{user_id}</code>\n\n"
                f"<b>Text:</b>\n{html.quote(text)}"
            )
            admin_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Approve", callback_data=f"approve_{conf_id}"),
                 InlineKeyboardButton(text="❌ Reject", callback_data=f"reject_{conf_id}")]
            ])
            
            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(admin_id, admin_msg_text, reply_markup=admin_keyboard)
                except Exception as e:
                    logger.warning(f"Could not send to admin {admin_id}: {e}")
        
        await message.answer(f"✅ Your confession has been submitted! (ID: #{conf_id})")
        
    except Exception as e:
        logger.error(f"Error processing confession from {user_id}: {e}", exc_info=True)
        await message.answer("❌ An error occurred. Please try again.")
    finally:
        await state.clear()
@dp.callback_query(F.data.startswith(("approve_contact_", "reject_contact_", "deny_contact_")))
async def handle_contact_response(callback_query: types.CallbackQuery):
    """Handle approve/reject contact request"""
    try:
        logger.info(f"🔍 CONTACT - Callback data: '{callback_query.data}'")
        logger.info(f"🔍 CONTACT - From user: {callback_query.from_user.id}")
        
        # Parse the callback data
        data_parts = callback_query.data.split("_")
        
        if len(data_parts) < 3:
            logger.error(f"Invalid callback format: {callback_query.data}")
            await callback_query.answer("Invalid request format.", show_alert=True)
            return
        
        action = data_parts[0]  # "approve" or "reject" or "deny"
        identifier = data_parts[-1]  # Request ID
        
        logger.info(f"🔍 CONTACT - Action: {action}, Identifier: {identifier}")
        
        responder_uid = callback_query.from_user.id
        
        # Try to find the request by ID
        try:
            req_id = int(identifier)
            req_data = await fetch_one("""
                SELECT * FROM contact_requests 
                WHERE id = $1 AND status = 'pending'
            """, req_id)
        except ValueError:
            await callback_query.answer("Invalid request ID.", show_alert=True)
            return
        
        if not req_data:
            logger.error(f"No pending request found for ID {req_id}")
            await callback_query.answer("No pending request found.", show_alert=True)
            return
        
        req_id = req_data['id']
        author_uid = req_data['requester_user_id']
        
        logger.info(f"Processing request ID: {req_id}, from user: {author_uid}, to user: {req_data['requested_user_id']}")
        
        # Verify this user is the intended recipient
        if responder_uid != req_data['requested_user_id']:
            logger.warning(f"UNAUTHORIZED: User {responder_uid} tried to respond to request {req_id} meant for {req_data['requested_user_id']}")
            await callback_query.answer("You are not authorized to respond to this request.", show_alert=True)
            return
        
        if action == "approve":
            try:
                # Update request status
                await execute_update(
                    "UPDATE contact_requests SET status = 'approved', updated_at = CURRENT_TIMESTAMP WHERE id = $1",
                    req_id
                )
                
                # Create active chat
                chat_id = await execute_insert_return_id("""
                    INSERT INTO active_chats (user1_id, user2_id, started_by) 
                    VALUES ($1, $2, $2) RETURNING id
                """, author_uid, responder_uid)
                
                if not chat_id:
                    raise Exception("Failed to create chat")
                
                # Get profile names for notifications
                requester_name = await get_profile_name(author_uid)
                responder_name = await get_profile_name(responder_uid)
                
                # Notify requester (the one who sent the request)
                await safe_send_message(
                    author_uid,
                    f"✅ <b>Contact Request Approved!</b>\n\n"
                    f"<b>{responder_name}</b> has approved your contact request!\n\n"
                    f"You can now chat with them.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="💬 Go to Chat", callback_data=f"view_chat_{chat_id}")]
                    ])
                )
                
                # Send chat invitation to responder (the one who approved)
                chat_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="💬 Go to Chat", callback_data=f"view_chat_{chat_id}")]
                ])
                
                await safe_send_message(
                    responder_uid,
                    f"💬 <b>Chat Started!</b>\n\n"
                    f"You are now connected with <b>{requester_name}</b>.\n\n"
                    f"Click the button below to start chatting.",
                    reply_markup=chat_keyboard
                )
                
                # Update the approval message
                await callback_query.message.edit_text(
                    callback_query.message.html_text + "\n\n✅ <b>Approved! Chat started.</b>",
                    reply_markup=None
                )
                
                await callback_query.answer("✅ Contact request approved! Chat started.")
                
            except Exception as e:
                logger.error(f"Error in approval: {e}")
                await callback_query.answer("Error creating chat. Please try again.", show_alert=True)
                return
        else:  # reject or deny
            await execute_update(
                "UPDATE contact_requests SET status = 'rejected', updated_at = CURRENT_TIMESTAMP WHERE id = $1",
                req_id
            )
            
            requester_name = await get_profile_name(author_uid)
            
            await safe_send_message(
                author_uid,
                f"❌ <b>Contact Request Rejected</b>\n\n"
                f"Your contact request has been rejected."
            )
            
            await callback_query.message.edit_text(
                callback_query.message.html_text + "\n\n❌ <b>Rejected.</b>",
                reply_markup=None
            )
            
            await callback_query.answer("❌ Request rejected.")
        
    except Exception as e:
        logger.error(f"Error in handle_contact_response: {e}", exc_info=True)
        await callback_query.answer("Error processing request. Please try again.", show_alert=True)
# --- Admin Action Handlers ---
@dp.callback_query(F.data.startswith("approve_"))
async def handle_approve_confession(callback_query: types.CallbackQuery, state: FSMContext):
    if not await is_admin(callback_query.from_user.id):
        await callback_query.answer("Unauthorized.", show_alert=True)
        return
    
    conf_id = int(callback_query.data.split("_")[1])
    conf = await fetch_one("SELECT id, text, user_id, categories, status, photo_file_id FROM confessions WHERE id = $1", conf_id)
    
    if not conf:
        await callback_query.answer("Confession not found.", show_alert=True)
        return
    
    if conf['status'] != 'pending':
        await callback_query.answer(f"Already '{conf['status']}'.", show_alert=True)
        return
    
    try:
        link = f"https://t.me/{bot_info.username}?start=view_{conf['id']}"
        categories = conf['categories'] or []
        category_tags = " ".join([f"#{html.quote(cat)}" for cat in categories])
        
        if conf['photo_file_id']:
            channel_caption = f"<b>Confession #{conf['id']}</b>\n\n{html.quote(conf['text'])}\n\n{category_tags}"
            channel_kbd = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💬 View / Add Comments (0)", url=link)]])
            msg = await bot.send_photo(chat_id=CHANNEL_ID, photo=conf['photo_file_id'], caption=channel_caption, reply_markup=channel_kbd)
        else:
            channel_post_text = f"<b>Confession #{conf['id']}</b>\n\n{html.quote(conf['text'])}\n\n{category_tags}"
            channel_kbd = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💬 View / Add Comments (0)", url=link)]])
            msg = await bot.send_message(CHANNEL_ID, channel_post_text, reply_markup=channel_kbd)
        
        await execute_update("UPDATE confessions SET status = 'approved', message_id = $1 WHERE id = $2", msg.message_id, conf_id)
        await safe_send_message(conf['user_id'], f"✅ Your confession (#{conf_id}) has been approved!")
        
        try:
            await callback_query.message.edit_text(callback_query.message.html_text + "\n\n-- ✅ Approved --", reply_markup=None)
        except:
            pass
        
        await callback_query.answer(f"Confession #{conf_id} approved.")
        
    except Exception as e:
        logger.error(f"Error approving Confession {conf_id}: {e}", exc_info=True)
        await callback_query.answer(f"Error: {str(e)[:100]}", show_alert=True)

@dp.callback_query(F.data.startswith("reject_"))
async def handle_reject_confession(callback_query: types.CallbackQuery, state: FSMContext):
    if not await is_admin(callback_query.from_user.id):
        await callback_query.answer("Unauthorized.", show_alert=True)
        return
    
    conf_id = int(callback_query.data.split("_")[1])
    await state.update_data(
        rejecting_conf_id=conf_id,
        original_admin_text=callback_query.message.html_text,
        admin_review_message_id=callback_query.message.message_id
    )
    await state.set_state(AdminActions.waiting_for_rejection_reason)
    
    reason_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="/skip")], [KeyboardButton(text="/cancel")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    
    await callback_query.answer("❓ Provide rejection reason")
    await bot.send_message(
        callback_query.from_user.id,
        f"Reason for rejecting Confession #{conf_id}?\nUse /skip or /cancel.",
        reply_markup=reason_keyboard
    )

@dp.message(AdminActions.waiting_for_rejection_reason, F.text)
async def receive_rejection_reason(message: types.Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        await state.clear()
        return
    
    data = await state.get_data()
    conf_id = data.get("rejecting_conf_id")
    admin_message_id = data.get("admin_review_message_id")
    
    if not conf_id:
        await message.answer("Error: Context lost.")
        await state.clear()
        return
    
    reason = message.text.strip()
    
    if reason.lower() == "/cancel":
        await message.answer("Rejection cancelled.", reply_markup=ReplyKeyboardRemove())
        await state.clear()
        return
    
    if reason.lower() == "/skip":
        reason = "No reason provided"
    
    await execute_update("UPDATE confessions SET status = 'rejected', rejection_reason = $1 WHERE id = $2", reason, conf_id)
    
    conf_data = await fetch_one("SELECT user_id FROM confessions WHERE id = $1", conf_id)
    if conf_data:
        await safe_send_message(conf_data['user_id'], f"❌ Your confession (#{conf_id}) was rejected.\n\nReason: {html.quote(reason)}")
    
    if admin_message_id:
        try:
            await bot.edit_message_text(
                chat_id=message.from_user.id,
                message_id=admin_message_id,
                text=data.get("original_admin_text", "") + f"\n\n-- Rejected --\nReason: {html.quote(reason)}",
                reply_markup=None
            )
        except Exception as e:
            logger.error(f"Could not edit admin review message: {e}")
    
    await message.answer(f"Confession #{conf_id} rejected.", reply_markup=ReplyKeyboardRemove())
    await state.clear()

# --- Admin Deletion Request Handlers ---
@dp.callback_query(F.data.startswith("admin_approve_delete_"))
async def admin_approve_delete(callback_query: types.CallbackQuery):
    if not await is_admin(callback_query.from_user.id):
        await callback_query.answer("Unauthorized.", show_alert=True)
        return
    
    conf_id = int(callback_query.data.split("_")[-1])
    
    conf_data = await fetch_one("SELECT user_id, message_id FROM confessions WHERE id = $1", conf_id)
    if not conf_data:
        await callback_query.answer("Confession not found.", show_alert=True)
        return
    
    if CHANNEL_ID and conf_data['message_id']:
        try:
            await bot.delete_message(CHANNEL_ID, conf_data['message_id'])
        except Exception as e:
            logger.warning(f"Could not delete message from channel: {e}")
    
    await execute_update("UPDATE confessions SET status = 'deleted' WHERE id = $1", conf_id)
    await execute_update("UPDATE deletion_requests SET status = 'approved', reviewed_at = CURRENT_TIMESTAMP WHERE confession_id = $1", conf_id)
    await safe_send_message(conf_data['user_id'], f"🗑️ Your deletion request for Confession #{conf_id} has been approved.")
    
    await callback_query.message.edit_text(f"✅ Deletion approved for Confession #{conf_id}.", reply_markup=None)
    await callback_query.answer("Deletion approved!")

@dp.callback_query(F.data.startswith("admin_reject_delete_"))
async def admin_reject_delete(callback_query: types.CallbackQuery):
    if not await is_admin(callback_query.from_user.id):
        await callback_query.answer("Unauthorized.", show_alert=True)
        return
    
    conf_id = int(callback_query.data.split("_")[-1])
    
    conf_data = await fetch_one("SELECT user_id FROM confessions WHERE id = $1", conf_id)
    if conf_data:
        await execute_update("UPDATE deletion_requests SET status = 'rejected', reviewed_at = CURRENT_TIMESTAMP WHERE confession_id = $1", conf_id)
        await safe_send_message(conf_data['user_id'], f"❌ Your deletion request for Confession #{conf_id} was rejected.")
    
    await callback_query.message.edit_text(f"❌ Deletion rejected for Confession #{conf_id}.", reply_markup=None)
    await callback_query.answer("Deletion rejected!")

# --- Comment Handlers ---
@dp.callback_query(F.data.startswith("browse_"))
async def browse_comments(callback_query: types.CallbackQuery):
    """Browse comments for a confession"""
    try:
        logger.info(f"📝 BROWSE COMMENTS - Callback: '{callback_query.data}'")
        
        # Parse confession ID
        parts = callback_query.data.split("_")
        if len(parts) < 2:
            await callback_query.answer("Invalid callback data", show_alert=True)
            return
        
        conf_id = int(parts[1])
        user_id = callback_query.from_user.id
        
        # Check if confession exists and is approved
        conf_data = await fetch_one("SELECT status, user_id FROM confessions WHERE id = $1", conf_id)
        
        if not conf_data or conf_data['status'] != 'approved':
            await callback_query.answer("This confession is not available", show_alert=True)
            return
        
        # Check if there are any comments
        count_row = await fetch_one("SELECT COUNT(*) as count FROM comments WHERE confession_id = $1", conf_id)
        comment_count = count_row['count'] if count_row else 0
        
        if comment_count == 0:
            await callback_query.answer("No comments yet", show_alert=True)
            nav = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Add Comment", callback_data=f"add_{conf_id}")]
            ])
            await safe_send_message(user_id, "No comments yet. Be the first to add one!", reply_markup=nav)
            return
        
        # Acknowledge and show comments
        await callback_query.answer("Loading comments...")
        
        # Delete the original message if it exists to avoid clutter
        try:
            await callback_query.message.delete()
        except:
            pass
        
        # Show comments
        await show_comments_for_confession(user_id, conf_id)
        
    except Exception as e:
        logger.error(f"Error in browse_comments: {e}", exc_info=True)
        await callback_query.answer("Error loading comments", show_alert=True)
@dp.callback_query(F.data.startswith("add_"))
async def add_comment_start(callback_query: types.CallbackQuery, state: FSMContext):
    conf_id = int(callback_query.data.split("_")[1])
    conf_data = await fetch_one("SELECT status FROM confessions WHERE id = $1", conf_id)
    
    if not conf_data or conf_data['status'] != 'approved':
        await callback_query.answer("This confession is not available for comments.", show_alert=True)
        return
    
    await state.update_data(confession_id=conf_id, parent_comment_id=None)
    await state.set_state(CommentForm.waiting_for_comment)
    
    await callback_query.message.answer(
        "💬 <b>Add a Comment</b>\n\nSend your comment text, sticker, or GIF.\nType /cancel to abort."
    )
    await callback_query.answer()

@dp.callback_query(F.data.startswith("reply_"))
async def reply_comment_start(callback_query: types.CallbackQuery, state: FSMContext):
    comment_id = int(callback_query.data.split("_")[1])
    comment_data = await fetch_one("SELECT confession_id FROM comments WHERE id = $1", comment_id)
    
    if not comment_data:
        await callback_query.answer("Comment not found.", show_alert=True)
        return
    
    conf_data = await fetch_one("SELECT status FROM confessions WHERE id = $1", comment_data['confession_id'])
    if not conf_data or conf_data['status'] != 'approved':
        await callback_query.answer("This confession is not available.", show_alert=True)
        return
    
    await state.update_data(confession_id=comment_data['confession_id'], parent_comment_id=comment_id)
    await state.set_state(CommentForm.waiting_for_comment)
    
    await callback_query.message.answer(
        "↪️ <b>Reply to Comment</b>\n\nSend your reply text, sticker, or GIF.\nType /cancel to abort."
    )
    await callback_query.answer()

@dp.message(CommentForm.waiting_for_comment, F.text)
async def receive_comment_text(message: types.Message, state: FSMContext):
    if message.text.startswith('/'):
        return
    await process_comment(message, state, text=message.text, sticker_file_id=None, animation_file_id=None)

@dp.message(CommentForm.waiting_for_comment, F.sticker)
async def receive_comment_sticker(message: types.Message, state: FSMContext):
    await process_comment(message, state, text=None, sticker_file_id=message.sticker.file_id, animation_file_id=None)

@dp.message(CommentForm.waiting_for_comment, F.animation)
async def receive_comment_gif(message: types.Message, state: FSMContext):
    await process_comment(message, state, text=None, sticker_file_id=None, animation_file_id=message.animation.file_id)

async def process_comment(message: types.Message, state: FSMContext, text: Optional[str] = None, 
                         sticker_file_id: Optional[str] = None, animation_file_id: Optional[str] = None):
    user_id = message.from_user.id
    state_data = await state.get_data()
    confession_id = state_data.get('confession_id')
    parent_comment_id = state_data.get('parent_comment_id')
    
    if not confession_id:
        await message.answer("Error: No confession selected.")
        await state.clear()
        return
    
    if text and len(text) > 2000:
        await message.answer("Comment too long (max 2000 characters).")
        return
    
    try:
        query = """
            INSERT INTO comments (confession_id, user_id, text, sticker_file_id, animation_file_id, parent_comment_id)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id
        """
        comment_id = await execute_insert_return_id(query, confession_id, user_id, text, sticker_file_id, animation_file_id, parent_comment_id)
        
        if not comment_id:
            raise Exception("Failed to get comment ID")
        
        await update_user_points(user_id, 1)
        
        conf_owner = await fetch_one("SELECT user_id FROM confessions WHERE id = $1", confession_id)
        if conf_owner and conf_owner['user_id'] != user_id:
            await safe_send_message(
                conf_owner['user_id'],
                f"💬 <b>New comment on your confession #{confession_id}</b>\n\n"
                f"Use this link to view: https://t.me/{bot_info.username}?start=view_{confession_id}"
            )
        
        await update_channel_post_button(confession_id)
        await message.answer("✅ Comment posted!")
        
    except Exception as e:
        logger.error(f"Error adding comment: {e}")
        await message.answer("❌ Error posting comment.")
    finally:
        await state.clear()

@dp.callback_query(F.data.startswith("react_like_"))
async def react_like(callback_query: types.CallbackQuery):
    await handle_reaction(callback_query, "like")

@dp.callback_query(F.data.startswith("react_dislike_"))
async def react_dislike(callback_query: types.CallbackQuery):
    await handle_reaction(callback_query, "dislike")

async def handle_reaction(callback_query: types.CallbackQuery, reaction_type: str):
    comment_id = int(callback_query.data.split("_")[-1])
    user_id = callback_query.from_user.id
    
    comment_data = await fetch_one("SELECT user_id FROM comments WHERE id = $1", comment_id)
    if not comment_data:
        await callback_query.answer("Comment not found.", show_alert=True)
        return
    
    comment_owner_id = comment_data['user_id']
    
    if comment_owner_id == user_id:
        await callback_query.answer("You cannot react to your own comment.", show_alert=True)
        return
    
    existing = await fetch_one("SELECT reaction_type FROM reactions WHERE comment_id = $1 AND user_id = $2", comment_id, user_id)
    
    if existing:
        if existing['reaction_type'] == reaction_type:
            await execute_update("DELETE FROM reactions WHERE comment_id = $1 AND user_id = $2", comment_id, user_id)
            points_change = -POINTS_PER_LIKE_RECEIVED if reaction_type == "like" else -POINTS_PER_DISLIKE_RECEIVED
            await update_user_points(comment_owner_id, points_change)
        else:
            old_points = POINTS_PER_LIKE_RECEIVED if existing['reaction_type'] == "like" else POINTS_PER_DISLIKE_RECEIVED
            await update_user_points(comment_owner_id, -old_points)
            await execute_update("UPDATE reactions SET reaction_type = $1 WHERE comment_id = $2 AND user_id = $3", reaction_type, comment_id, user_id)
            new_points = POINTS_PER_LIKE_RECEIVED if reaction_type == "like" else POINTS_PER_DISLIKE_RECEIVED
            await update_user_points(comment_owner_id, new_points)
    else:
        await execute_update("INSERT INTO reactions (comment_id, user_id, reaction_type) VALUES ($1, $2, $3)", comment_id, user_id, reaction_type)
        points_change = POINTS_PER_LIKE_RECEIVED if reaction_type == "like" else POINTS_PER_DISLIKE_RECEIVED
        await update_user_points(comment_owner_id, points_change)
    
    likes, dislikes = await get_comment_reactions(comment_id)
    
    message = callback_query.message
    if message and message.reply_markup:
        inline_keyboard = message.reply_markup.inline_keyboard
        new_inline_keyboard = []
        for row in inline_keyboard:
            new_row = []
            for button in row:
                text = button.text
                callback_data = button.callback_data
                if callback_data == f"react_like_{comment_id}":
                    text = f"👍 {likes}"
                elif callback_data == f"react_dislike_{comment_id}":
                    text = f"👎 {dislikes}"
                new_row.append(InlineKeyboardButton(text=text, callback_data=callback_data))
            new_inline_keyboard.append(new_row)
        new_keyboard = InlineKeyboardMarkup(inline_keyboard=new_inline_keyboard)
        try:
            await message.edit_reply_markup(reply_markup=new_keyboard)
        except TelegramBadRequest as e:
            if "message is not modified" not in str(e):
                logger.error(f"Error updating reaction UI: {e}")
    
    await callback_query.answer(f"{'👍' if reaction_type == 'like' else '👎'} reaction updated!")

@dp.callback_query(F.data.startswith("report_confirm_"))
async def report_confirm(callback_query: types.CallbackQuery):
    comment_id = int(callback_query.data.split("_")[-1])
    comment_data = await fetch_one("SELECT text, user_id FROM comments WHERE id = $1", comment_id)
    
    if not comment_data:
        await callback_query.answer("Comment deleted.", show_alert=True)
        return
    
    if comment_data['user_id'] == callback_query.from_user.id:
        await callback_query.answer("You cannot report yourself.", show_alert=True)
        return
    
    snippet = html.quote(comment_data['text'][:100]) if comment_data['text'] else "[Sticker/GIF]"
    confirm_text = f"Report this comment?\n\n<i>\"{snippet}...\"</i>"
    kbd = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Yes", callback_data=f"report_execute_{comment_id}"),
        InlineKeyboardButton(text="❌ No", callback_data="report_cancel")
    ]])
    await safe_send_message(callback_query.from_user.id, confirm_text, reply_markup=kbd)
    await callback_query.answer()

@dp.callback_query(F.data.startswith("report_execute_"))
async def report_execute(callback_query: types.CallbackQuery):
    comment_id = int(callback_query.data.split("_")[-1])
    reporter_id = callback_query.from_user.id
    
    comment_data = await fetch_one("SELECT user_id, confession_id, text FROM comments WHERE id = $1", comment_id)
    if not comment_data:
        await callback_query.message.edit_text("Report failed: Comment no longer exists.")
        return
    
    if comment_data['user_id'] == reporter_id:
        await callback_query.message.edit_text("Cannot report own comment.")
        return
    
    await execute_update(
        "INSERT INTO reports (comment_id, reporter_user_id, reported_user_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
        comment_id, reporter_id, comment_data['user_id']
    )
    
    snippet = html.quote(comment_data['text'][:200]) if comment_data['text'] else "[Media]"
    conf_link = f"https://t.me/{bot_info.username}?start=view_{comment_data['confession_id']}"
    admin_notification = (
        f"⚠️ <b>New Comment Report</b>\n\n"
        f"<b>Confession:</b> <a href='{conf_link}'>#{comment_data['confession_id']}</a>\n"
        f"<b>Comment ID:</b> <code>{comment_id}</code>\n"
        f"<b>Content:</b>\n<i>{snippet}</i>\n\n"
        f"<b>Reported User:</b> <code>{comment_data['user_id']}</code>\n"
        f"<b>Reporter:</b> <code>{reporter_id}</code>"
    )
    
    for admin_id in ADMIN_IDS:
        await safe_send_message(admin_id, admin_notification, disable_web_page_preview=True)
    
    await callback_query.message.edit_text("✅ Report submitted.")
    await callback_query.answer()

@dp.callback_query(F.data == "report_cancel")
async def report_cancel(callback_query: types.CallbackQuery):
    await callback_query.message.edit_text("Report cancelled.")
    await callback_query.answer()

@dp.callback_query(F.data.startswith("req_contact_"))
async def handle_request_contact(callback_query: types.CallbackQuery):
    comm_id = int(callback_query.data.split("_")[-1])
    requester_uid = callback_query.from_user.id
    
    comm_data = await fetch_one("""
        SELECT c.user_id as comm_uid, c.text, co.id as conf_id, co.user_id as conf_owner_id 
        FROM comments c JOIN confessions co ON c.confession_id = co.id WHERE c.id = $1
    """, comm_id)
    
    if not comm_data:
        await callback_query.answer("Comment not found.", show_alert=True)
        return
    
    commenter_uid, conf_id, conf_owner_id = comm_data['comm_uid'], comm_data['conf_id'], comm_data['conf_owner_id']
    
    if requester_uid != conf_owner_id:
        await callback_query.answer("Only the confession author can do this.", show_alert=True)
        return
    
    if requester_uid == commenter_uid:
        await callback_query.answer("You cannot contact yourself.", show_alert=True)
        return
    
    existing = await fetch_one("SELECT status FROM contact_requests WHERE comment_id = $1 AND requester_user_id = $2", comm_id, requester_uid)
    if existing and existing['status'] != 'denied':
        await callback_query.answer(f"Request already {existing['status']}.", show_alert=True)
        return
    
    req_id = await execute_insert_return_id("""
        INSERT INTO contact_requests (confession_id, comment_id, requester_user_id, requested_user_id, status) 
        VALUES ($1, $2, $3, $4, 'pending')
        ON CONFLICT (comment_id, requester_user_id) DO UPDATE SET status = 'pending', updated_at = CURRENT_TIMESTAMP
        RETURNING id
    """, conf_id, comm_id, requester_uid, commenter_uid)
    
    snippet = html.quote(comm_data['text'][:100]) if comm_data['text'] else "[Sticker/GIF]"
    notification = (
        f"🤝 The author of Confession #{conf_id} wants to contact you about your comment:\n\n"
        f"<i>\"{snippet}...\"</i>\n\n"
        f"Approve sharing your username?"
    )
    kbd = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Approve", callback_data=f"approve_contact_{req_id}")],
        [InlineKeyboardButton(text="❌ Deny", callback_data=f"deny_contact_{req_id}")]
    ])
    
    sent = await safe_send_message(commenter_uid, notification, reply_markup=kbd)
    if sent:
        await callback_query.answer("✅ Request sent.")
    else:
        await execute_update("UPDATE contact_requests SET status = 'failed_to_notify' WHERE id = $1", req_id)
        await callback_query.answer("⚠️ Could not notify user.", show_alert=True)



# --- Admin Commands ---
@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer("This command is for admins only.")
        return
    
    admin_text = (
        "👑 <b>Admin Panel</b>\n\n"
        "<b>Commands:</b>\n"
        "📊 /stats - Show bot statistics\n"
        "🆔 /id - Get user info\n"
        "⚠️ /warn - Warn a user\n"
        "⏸️ /block - Temporarily block\n"
        "🚫 /pblock - Permanently block\n"
        "✅ /unblock - Unblock user\n"
        "📢 /broadcast - Broadcast message"
    )
    await message.answer(admin_text)

@dp.message(Command("stats"))
async def show_stats(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer("This command is for admins only.")
        return
    
    total_conf = await fetch_one("SELECT COUNT(*) as count FROM confessions")
    approved_conf = await fetch_one("SELECT COUNT(*) as count FROM confessions WHERE status = 'approved'")
    pending_conf = await fetch_one("SELECT COUNT(*) as count FROM confessions WHERE status = 'pending'")
    total_comments = await fetch_one("SELECT COUNT(*) as count FROM comments")
    total_users = await fetch_one("SELECT COUNT(*) as count FROM user_status WHERE has_accepted_rules = TRUE")
    blocked_users = await fetch_one("SELECT COUNT(*) as count FROM user_status WHERE is_blocked = TRUE")
    active_chats = await fetch_one("SELECT COUNT(*) as count FROM active_chats WHERE is_active = 1")
    
    stats_text = (
        f"📊 <b>Bot Statistics</b>\n\n"
        f"<b>Users:</b>\n• Total: {total_users['count'] if total_users else 0}\n• Blocked: {blocked_users['count'] if blocked_users else 0}\n\n"
        f"<b>Confessions:</b>\n• Total: {total_conf['count'] if total_conf else 0}\n• Approved: {approved_conf['count'] if approved_conf else 0}\n• Pending: {pending_conf['count'] if pending_conf else 0}\n\n"
        f"<b>Comments:</b> {total_comments['count'] if total_comments else 0}\n"
        f"<b>Active Chats:</b> {active_chats['count'] if active_chats else 0}"
    )
    await message.answer(stats_text)

@dp.message(Command("id"))
async def get_user_id_command(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer("This command is for admins only.")
        return
    
    target_id = None
    target_name = None
    target_username = None
    
    # Check if replying to a message
    if message.reply_to_message:
        target_id = message.reply_to_message.from_user.id
        target_name = message.reply_to_message.from_user.full_name
        target_username = message.reply_to_message.from_user.username
    else:
        # Try to get ID from command arguments
        parts = message.text.split()
        if len(parts) > 1:
            try:
                target_id = int(parts[1])
                # Try to get user info from Telegram
                try:
                    chat = await bot.get_chat(target_id)
                    target_name = chat.full_name
                    target_username = chat.username
                except:
                    target_name = "Unknown"
                    target_username = None
            except ValueError:
                await message.answer("Invalid user ID. Please provide a valid numeric ID.")
                return
        else:
            # No arguments, show admin's own ID
            target_id = message.from_user.id
            target_name = message.from_user.full_name
            target_username = message.from_user.username
    
    # Get profile info from database
    profile_name = await get_profile_name(target_id)
    points = await get_user_points(target_id)
    
    # Generate profile link
    profile_link = await get_encoded_profile_link(target_id)
    
    # Check block status
    status = await fetch_one("SELECT is_blocked, blocked_until, block_reason FROM user_status WHERE user_id = $1", target_id)
    
    response = f"👤 <b>User Information</b>\n\n"
    response += f"<b>ID:</b> <code>{target_id}</code>\n"
    response += f"<b>Telegram Name:</b> {html.quote(target_name) if target_name else 'Unknown'}\n"
    
    if target_username:
        response += f"<b>Username:</b> @{target_username}\n"
    else:
        response += f"<b>Username:</b> None (private)\n"
    
    response += f"<b>Profile Name:</b> {html.quote(profile_name)}\n"
    response += f"<b>Profile Link:</b> {profile_link}\n"
    response += f"<b>Aura Points:</b> {points}\n"
    
    if status and status['is_blocked']:
        if status['blocked_until']:
            response += f"<b>Status:</b> ⛔ Blocked until {status['blocked_until'].strftime('%Y-%m-%d %H:%M UTC')}\n"
            if status['block_reason']:
                response += f"<b>Reason:</b> {html.quote(status['block_reason'])}\n"
        else:
            response += f"<b>Status:</b> ⛔ Permanently blocked\n"
            if status['block_reason']:
                response += f"<b>Reason:</b> {html.quote(status['block_reason'])}\n"
    else:
        response += f"<b>Status:</b> ✅ Active\n"
    
    # Add quick action buttons for admins
    keyboard = InlineKeyboardBuilder()
    if status and status['is_blocked']:
        keyboard.button(text="✅ Unblock", callback_data=f"admin_unblock_{target_id}")
    else:
        keyboard.button(text="⛔ Block", callback_data=f"admin_block_{target_id}")
    
    keyboard.button(text="👤 View Profile", url=profile_link)
    keyboard.adjust(2)
    
    await message.answer(response, reply_markup=keyboard.as_markup(), disable_web_page_preview=True)

@dp.message(Command("warn"))
async def warn_user(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer("This command is for admins only.")
        return
    
    if not message.reply_to_message:
        await message.answer("Please reply to a user's message to warn them.")
        return
    
    target_id = message.reply_to_message.from_user.id
    reason = message.text.split(maxsplit=1)[1] if len(message.text.split()) > 1 else "No reason provided"
    
    try:
        await bot.send_message(
            target_id,
            f"⚠️ <b>Warning from Admin</b>\n\nReason: {html.quote(reason)}"
        )
        await message.answer(f"✅ Warning sent to user ID {target_id}")
    except Exception as e:
        await message.answer(f"❌ Could not send warning: {e}")

@dp.message(Command("unblock"))
async def unblock_user(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer("This command is for admins only.")
        return
    
    if not message.reply_to_message:
        await message.answer("Please reply to a user's message to unblock them.")
        return
    
    target_id = message.reply_to_message.from_user.id
    
    await execute_update(
        "UPDATE user_status SET is_blocked = FALSE, blocked_until = NULL, block_reason = NULL WHERE user_id = $1",
        target_id
    )
    
    try:
        await bot.send_message(target_id, "✅ You have been unblocked.")
    except:
        pass
    
    await message.answer(f"✅ User {target_id} unblocked.")

@dp.message(Command("broadcast"))
async def broadcast_command(message: types.Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        await message.answer("This command is for admins only.")
        return
    
    if not message.reply_to_message:
        await message.answer("Please reply to a message to broadcast it.")
        return
    
    # Show a preview of what will be broadcast
    preview_text = "📢 <b>Broadcast Preview</b>\n\n"
    preview_text += "The following message will be sent to all users:\n\n"
    
    if message.reply_to_message.photo:
        preview_text += "📷 <i>[Photo" + (" with caption" if message.reply_to_message.caption else "") + "]</i>"
    elif message.reply_to_message.video:
        preview_text += "🎥 <i>[Video" + (" with caption" if message.reply_to_message.caption else "") + "]</i>"
    elif message.reply_to_message.document:
        preview_text += "📄 <i>[Document" + (" with caption" if message.reply_to_message.caption else "") + "]</i>"
    elif message.reply_to_message.sticker:
        preview_text += "🎭 <i>[Sticker]</i>"
    elif message.reply_to_message.animation:
        preview_text += "🎬 <i>[GIF/Animation" + (" with caption" if message.reply_to_message.caption else "") + "]</i>"
    elif message.reply_to_message.text:
        preview_text += f"💬 <i>\"{message.reply_to_message.text[:100]}\"</i>"
        if len(message.reply_to_message.text) > 100:
            preview_text += "..."
    else:
        preview_text += "❓ <i>[Unsupported message type]</i>"
    
    confirm_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Yes, Broadcast", callback_data="confirm_broadcast"),
         InlineKeyboardButton(text="❌ Cancel", callback_data="cancel_broadcast")]
    ])
    
    await message.answer(
        preview_text + "\n\n⚠️ <b>Confirm Broadcast</b>\n\nAre you sure you want to broadcast this message to all users?",
        reply_markup=confirm_keyboard
    )
    
    # Store the message info
    await state.update_data(
        broadcast_from_chat_id=message.reply_to_message.chat.id,
        broadcast_message_id=message.reply_to_message.message_id
    )

@dp.callback_query(F.data == "confirm_broadcast")
async def confirm_broadcast(callback_query: types.CallbackQuery, state: FSMContext):
    if not await is_admin(callback_query.from_user.id):
        await callback_query.answer("Unauthorized.", show_alert=True)
        return
    
    data = await state.get_data()
    from_chat_id = data.get('broadcast_from_chat_id')
    msg_id = data.get('broadcast_message_id')
    
    if not from_chat_id or not msg_id:
        await callback_query.answer("Error: Message data not found.", show_alert=True)
        await state.clear()
        return
    
    # Get all active users
    users = await execute_query("SELECT user_id FROM user_status WHERE has_accepted_rules = TRUE AND is_blocked = FALSE")
    total = len(users)
    successful = 0
    failed = 0
    
    progress = await callback_query.message.answer(f"📤 Broadcasting... 0/{total}")
    
    for i, row in enumerate(users):
        try:
            # copy_message works for ALL message types (photos, videos, documents, stickers, etc.)
            await bot.copy_message(
                chat_id=row['user_id'],
                from_chat_id=from_chat_id,
                message_id=msg_id
            )
            successful += 1
        except Exception as e:
            failed += 1
            logger.warning(f"Broadcast failed to {row['user_id']}: {e}")
        
        if i % 10 == 0:
            try:
                await progress.edit_text(f"📤 Broadcasting... {successful + failed}/{total}")
            except:
                pass
        await asyncio.sleep(0.05)  # Small delay to avoid flooding
    
    await progress.edit_text(f"✅ Broadcast complete!\nSuccess: {successful}\nFailed: {failed}")
    await callback_query.message.edit_text(f"✅ Broadcast completed.")
    await state.clear()
    await callback_query.answer()




@dp.callback_query(F.data == "cancel_broadcast")
async def cancel_broadcast(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.message.edit_text("❌ Broadcast cancelled.")
    await state.clear()
    await callback_query.answer()

@dp.callback_query(F.data == "noop")
async def noop_handler(callback_query: types.CallbackQuery):
    await callback_query.answer()

# --- Fallback Handler ---
@dp.message(StateFilter(None), F.text & ~F.text.startswith('/'))
async def handle_text_without_state(message: types.Message):
    await message.reply("Hi! 👋 Use /confess to share anonymously, /profile to see your history, or /help for commands.")

# --- HTTP Server for Health Checks ---
async def health_check_handler(request):
    try:
        bot_status = f"@{bot_info.username}" if bot_info else "unknown"
        db_status = "connected" if db else "disconnected"
        if db:
            try:
                async with db.acquire() as conn:
                    await conn.fetchval('SELECT 1')
                db_status = "connected"
            except:
                db_status = "disconnected"
        
        return web.json_response({
            "status": "healthy",
            "bot": bot_status,
            "database": db_status,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return web.json_response({"status": "unhealthy", "error": str(e)}, status=500)

async def start_http_server():
    try:
        app = web.Application()
        app.router.add_get('/', health_check_handler)
        app.router.add_get('/healthz', health_check_handler)
        app.router.add_get('/health', health_check_handler)
        
        runner = web.AppRunner(app)
        await runner.setup()
        
        port = int(HTTP_PORT_STR) if HTTP_PORT_STR else 8080
        site = web.TCPSite(runner, '0.0.0.0', port)
        await site.start()
        
        logger.info(f"✅ HTTP server started on port {port}")
        return runner
    except Exception as e:
        logger.error(f"Failed to start HTTP server: {e}")
        raise

async def set_bot_commands():
    user_commands = [
        types.BotCommand(command="start", description="Start"),
        types.BotCommand(command="confess", description="Submit confession"),
        types.BotCommand(command="profile", description="Your profile"),
        types.BotCommand(command="help", description="Help"),
        types.BotCommand(command="rules", description="Rules"),
        types.BotCommand(command="privacy", description="Privacy"),
        types.BotCommand(command="cancel", description="Cancel"),
        types.BotCommand(command="endchat", description="End chat"),
    ]
    
    admin_commands = user_commands + [
        types.BotCommand(command="admin", description="Admin panel"),
        types.BotCommand(command="stats", description="Statistics"),
        types.BotCommand(command="id", description="Get user info"),
        types.BotCommand(command="warn", description="Warn user"),
        types.BotCommand(command="block", description="Block user"),
        types.BotCommand(command="unblock", description="Unblock user"),
        types.BotCommand(command="broadcast", description="Broadcast"),
    ]
    
    await bot.set_my_commands(user_commands)
    for admin_id in ADMIN_IDS:
        try:
            await bot.set_my_commands(admin_commands, scope=types.BotCommandScopeChat(chat_id=admin_id))
        except Exception as e:
            logger.warning(f"Could not set admin commands for {admin_id}: {e}")

async def monitor_database():
    while True:
        await asyncio.sleep(300)
        try:
            if db_manager and db_manager.pool:
                async with db_manager.pool.acquire() as conn:
                    await conn.fetchval('SELECT 1')
            elif db:
                async with db.acquire() as conn:
                    await conn.fetchval('SELECT 1')
        except Exception as e:
            logger.error(f"Database monitor error: {e}")
            # Don't try to reconnect here - let the next query handle it

# --- Main Execution ---
async def main():
    http_runner = None
    try:
        # Clear any existing webhook first
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("✅ Webhook cleared")
        
        # DON'T close the bot session here - it causes flood control
        # Just proceed with setup
        
        await setup()
        
        dp.message.middleware(BlockUserMiddleware())
        dp.callback_query.middleware(BlockUserMiddleware())
        
        await set_bot_commands()
        
        http_runner = await start_http_server()
        
        logger.info(f"🚀 Bot @{bot_info.username} started")
        await dp.start_polling(bot, skip_updates=True, allowed_updates=dp.resolve_used_update_types())
        
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
        if http_runner:
            await http_runner.cleanup()
        if db:
            await db.close()
        raise

async def shutdown():
    logger.info("Shutting down...")
    if db:
        await db.close()
    logger.info("Bot stopped.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
        asyncio.run(shutdown())
    except Exception as e:
        logger.critical(f"Unhandled exception: {e}")
        asyncio.run(shutdown())







































