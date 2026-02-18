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

async def execute_query(query: str, *params):
    """Execute query and return results"""
    if not await ensure_db_connection():
        raise Exception("Database connection not available")
    async with db.acquire() as conn:
        return await conn.fetch(query, *params)

async def fetch_one(query: str, *params):
    """Fetch single row"""
    if not await ensure_db_connection():
        raise Exception("Database connection not available")
    async with db.acquire() as conn:
        return await conn.fetchrow(query, *params)

async def execute_update(query: str, *params):
    """Execute INSERT/UPDATE/DELETE query"""
    if not await ensure_db_connection():
        raise Exception("Database connection not available")
    async with db.acquire() as conn:
        return await conn.execute(query, *params)

async def execute_insert_return_id(query: str, *params):
    """Execute INSERT and return inserted ID"""
    if not await ensure_db_connection():
        raise Exception("Database connection not available")
    async with db.acquire() as conn:
        result = await conn.fetchval(query, *params)
        if result is None:
            result = await conn.fetchval("SELECT LASTVAL();")
        return result

async def is_admin(user_id: int) -> bool:
    """Check if user is admin"""
    return user_id in ADMIN_IDS

async def get_profile_name(user_id: int) -> str:
    """Get user's profile name"""
    row = await fetch_one("SELECT profile_name FROM user_status WHERE user_id = $1", user_id)
    if row and row['profile_name']:
        return row['profile_name']
    return "Anonymous"

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

async def check_rate_limit(user_id: int) -> bool:
    """Check if user is rate limited"""
    current_time = time.time()
    if current_time - user_last_action[user_id] < RATE_LIMIT_SECONDS:
        return False
    user_last_action[user_id] = current_time
    return True

# --- Database Setup ---
async def create_db_pool():
    """Create PostgreSQL connection pool for Supabase - FIXED for PgBouncer"""
    global db
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
        
        logger.info("Connecting to database with statement_cache_size=0 (for PgBouncer compatibility)...")
        
        # CRITICAL FIX: Set statement_cache_size=0 for PgBouncer compatibility
        db = await asyncpg.create_pool(
            dsn=connection_string,
            min_size=1,
            max_size=10,
            command_timeout=60,
            max_queries=50000,
            max_inactive_connection_lifetime=300,
            timeout=30,
            statement_cache_size=0  # This fixes the prepared statement error!
        )
        logger.info("✅ Database connection pool created (PgBouncer compatible)")
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
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS confessions (
                    id SERIAL PRIMARY KEY,
                    text TEXT NOT NULL,
                    user_id BIGINT NOT NULL,
                    status VARCHAR(10) DEFAULT 'pending',
                    message_id BIGINT,
                    photo_file_id TEXT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    rejection_reason TEXT NULL,
                    categories TEXT[] NULL
                );
            """)
            
            # Ensure photo_file_id column exists
            await conn.execute("""
                DO $$ 
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                  WHERE table_name='confessions' AND column_name='photo_file_id') THEN
                        ALTER TABLE confessions ADD COLUMN photo_file_id TEXT NULL;
                    END IF;
                END $$;
            """)
            
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
                CREATE TABLE IF NOT EXISTS contact_requests (
                    id SERIAL PRIMARY KEY,
                    confession_id INTEGER NOT NULL REFERENCES confessions(id) ON DELETE CASCADE,
                    comment_id INTEGER NOT NULL REFERENCES comments(id) ON DELETE CASCADE,
                    requester_user_id BIGINT NOT NULL,
                    requested_user_id BIGINT NOT NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (comment_id, requester_user_id)
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

# --- Middleware ---
class BlockUserMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: types.TelegramObject, data: Dict[str, Any]) -> Any:
        user = data.get('event_from_user')
        if not user:
            return await handler(event, data)

        user_id = user.id
        
        # Allow /start and /help commands even for blocked users
        if isinstance(event, types.Message) and event.text:
            if event.text.startswith('/start') or event.text.startswith('/help'):
                return await handler(event, data)
        
        if await is_admin(user_id):
            return await handler(event, data)

        try:
            row = await fetch_one("SELECT is_blocked, blocked_until, block_reason FROM user_status WHERE user_id = $1", user_id)
            
            if row and row['is_blocked']:
                now = datetime.now(datetime.utcnow().astimezone().tzinfo)
                blocked_until = row['blocked_until']
                
                if blocked_until and blocked_until < now:
                    await execute_update("UPDATE user_status SET is_blocked = FALSE, blocked_until = NULL, block_reason = NULL WHERE user_id = $1", user_id)
                    return await handler(event, data)
                else:
                    expiry_info = f"until {blocked_until.strftime('%Y-%m-%d %H:%M %Z')}" if blocked_until else "permanently"
                    reason_info = f"\nReason: <i>{html.quote(row['block_reason'])}</i>" if row['block_reason'] else ""
                    
                    if isinstance(event, types.CallbackQuery):
                        await event.answer(f"You are blocked {expiry_info}.", show_alert=True)
                    elif isinstance(event, types.Message):
                        with suppress(Exception):
                            await event.answer(f"❌ <b>You are blocked from using this bot {expiry_info}.</b>{reason_info}")
                    return
        except Exception as e:
            logger.error(f"Error checking block status for user {user_id}: {e}")
        
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
    
    # REMOVE THIS LINE - no profile button
    # builder.button(text="👤 Profile", callback_data=f"view_profile_{commenter_user_id}")
    
    # Add reaction buttons
    if commenter_user_id != viewer_user_id:
        builder.button(text=f"👍 {likes}", callback_data=f"react_like_{comment_id}")
        builder.button(text=f"👎 {dislikes}", callback_data=f"react_dislike_{comment_id}")
    else:
        builder.button(text=f"👍 {likes}", callback_data="noop")
        builder.button(text=f"👎 {dislikes}", callback_data="noop")
    
    # Add other buttons
    builder.button(text="↪️ Reply", callback_data=f"reply_{comment_id}")
    builder.button(text="⚠️", callback_data=f"report_confirm_{comment_id}")
    
    # Add contact request button for confession author
    if viewer_user_id == confession_owner_id and viewer_user_id != commenter_user_id:
        builder.button(text="🤝 Request Contact", callback_data=f"req_contact_{comment_id}")
        builder.adjust(4, 1)  # 4 buttons in first row, 1 in second
    else:
        builder.adjust(4)  # All 4 buttons in one row
    
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
        SELECT c.id, c.user_id, c.text, c.sticker_file_id, c.animation_file_id, c.parent_comment_id, c.created_at, 
               COALESCE(up.points, 0) as user_points, us.profile_name
        FROM comments c 
        LEFT JOIN user_points up ON c.user_id = up.user_id 
        LEFT JOIN user_status us ON c.user_id = us.user_id
        WHERE c.confession_id = $1 
        ORDER BY c.created_at ASC LIMIT $2 OFFSET $3
    """, confession_id, PAGE_SIZE, offset)

    db_id_to_message_id: Dict[int, int] = {}
    is_admin_user = await is_admin(user_id)

    if not comments_raw:
        await safe_send_message(user_id, f"<i>No comments on page {page}.</i>")
    else:
        for i, c_data in enumerate(comments_raw):
            seq_num, db_id, commenter_uid = offset + i + 1, c_data['id'], c_data['user_id']
            profile_name = c_data['profile_name'] if c_data['profile_name'] else "Anonymous"
            aura_points = c_data['user_points'] if c_data['user_points'] is not None else 0
            
            tag_parts = []
            if commenter_uid == confession_owner_id:
                tag_parts.append("👑 Author")
            if commenter_uid == user_id:
                tag_parts.append("👤 You")
            tag_str = f" ({', '.join(tag_parts)})" if tag_parts else ""
            
            encoded_profile_link = await get_encoded_profile_link(commenter_uid)
            profile_link = f"https://t.me/{bot_info.username}?start=profile_{encode_user_id(commenter_uid)}"
            display_name = f"<a href='{profile_link}'>{profile_name}</a> 🏅{aura_points}{tag_str}"
            admin_info = f" [UID: <code>{commenter_uid}</code>]" if is_admin_user else ""

            reply_to_msg_id = None
            text_reply_prefix = ""
            parent_db_id = c_data['parent_comment_id']
            
            if parent_db_id:
                if parent_db_id in db_id_to_message_id:
                    reply_to_msg_id = db_id_to_message_id[parent_db_id]
                else:
                    parent_seq_num = await get_comment_sequence_number(confession_id, parent_db_id)
                    if parent_seq_num:
                        text_reply_prefix = f"↪️ <i>Replying to comment #{parent_seq_num}...</i>\n"
                    else:
                        text_reply_prefix = "↪️ <i>Replying to another comment...</i>\n"

            keyboard = await build_comment_keyboard(db_id, commenter_uid, user_id, confession_owner_id, is_admin_user)
            
            sent_message = None
            try:
                if c_data['sticker_file_id']:
                    sent_message = await bot.send_sticker(user_id, sticker=c_data['sticker_file_id'], reply_to_message_id=reply_to_msg_id)
                    await bot.send_message(user_id, f"{text_reply_prefix}{display_name}{admin_info}", reply_markup=keyboard)
                    
                    
                    
                elif c_data['animation_file_id']:
                    sent_message = await bot.send_animation(user_id, animation=c_data['animation_file_id'], reply_to_message_id=reply_to_msg_id)
                    await bot.send_message(user_id, f"{text_reply_prefix}{display_name}{admin_info}", reply_markup=keyboard)
                    
                   
                    
                elif c_data['text']:
                    full_text = f"{text_reply_prefix}💬 {html.quote(c_data['text'])}\n\n{display_name}{admin_info}"
                    sent_message = await bot.send_message(user_id, full_text, reply_markup=keyboard, disable_web_page_preview=True, reply_to_message_id=reply_to_msg_id)
                    
                    
                
                if sent_message:
                    db_id_to_message_id[db_id] = sent_message.message_id
            except Exception as e:
                logger.warning(f"Could not send comment #{seq_num} to {user_id}: {e}")
                await safe_send_message(user_id, f"⚠️ Error displaying comment #{seq_num}.")
            await asyncio.sleep(0.1)

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
                    await message.answer(txt, reply_markup=builder.as_markup())
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
                
                profile_name = await get_profile_name(target_user_id)
                points = await get_user_points(target_user_id)
                
                profile_text = f"👤 <b>User Profile</b>\n\n"
                profile_text += f"📛 <b>Display Name:</b> {profile_name}\n"
                profile_text += f"🏅 <b>Aura Points:</b> {points}\n\n"
                profile_text += "<i>This user's profile information</i>"
                await message.answer(profile_text)
            except Exception as e:
                logger.error(f"Error handling profile deep link: {e}")
                await message.answer("Error processing profile link.")
    else:
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
    
    response_text = "📨 <b>Pending Contact Requests</b>\n\n"
    keyboard = InlineKeyboardBuilder()
    
    for req in requests:
        profile_name = req['profile_name'] or "Anonymous"
        message = req['message'] or "No message"
        time_str = req['created_at'].strftime('%Y-%m-%d %H:%M') if req['created_at'] else ""
        
        response_text += f"👤 <b>{profile_name}</b>\n"
        response_text += f"<i>Message:</i> {html.quote(message[:50])}{'...' if len(message) > 50 else ''}\n"
        response_text += f"<i>Time:</i> {time_str}\n\n"
        
        keyboard.row(
            InlineKeyboardButton(text=f"✅ Approve", callback_data=f"approve_contact_{req['requester_user_id']}"),
            InlineKeyboardButton(text=f"❌ Reject", callback_data=f"reject_contact_{req['requester_user_id']}")
        )
    
    keyboard.row(InlineKeyboardButton(text="⬅️ Back to Profile", callback_data="profile_main"))
    
    await callback_query.message.edit_text(response_text, reply_markup=keyboard.as_markup())
    await callback_query.answer()

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
    
    if not await check_rate_limit(message.from_user.id):
        await message.answer(f"⏳ Please wait {RATE_LIMIT_SECONDS} seconds between submissions.")
        return
    
    await process_confession(message, state, text=message.text, photo_file_id=None)

@dp.message(ConfessionForm.waiting_for_text, F.photo)
async def receive_photo_confession(message: types.Message, state: FSMContext):
    if not await check_rate_limit(message.from_user.id):
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
    conf_id = int(callback_query.data.split("_")[1])
    await show_comments_for_confession(callback_query.from_user.id, conf_id)
    await callback_query.answer()

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

@dp.callback_query(F.data.startswith(("approve_contact_", "deny_contact_")))
async def handle_contact_response(callback_query: types.CallbackQuery):
    action = callback_query.data.split("_")[0]
    req_id = int(callback_query.data.split("_")[-1])
    responder_uid = callback_query.from_user.id
    
    req_data = await fetch_one("SELECT * FROM contact_requests WHERE id = $1", req_id)
    if not req_data:
        await callback_query.answer("Request not found.", show_alert=True)
        return
    
    if responder_uid != req_data['requested_user_id']:
        await callback_query.answer("Not for you.", show_alert=True)
        return
    
    if req_data['status'] != 'pending':
        await callback_query.answer(f"Already {req_data['status']}.", show_alert=True)
        return
    
    author_uid = req_data['requester_user_id']
    conf_id = req_data['confession_id']
    
    if action == "approve":
        try:
            responder_info = await bot.get_chat(responder_uid)
            username = responder_info.username
            if username:
                await execute_update("UPDATE contact_requests SET status = 'approved', updated_at = CURRENT_TIMESTAMP WHERE id = $1", req_id)
                await execute_update("INSERT INTO active_chats (user1_id, user2_id, started_by) VALUES ($1, $2, $2)", author_uid, responder_uid)
                await safe_send_message(author_uid, f"✅ Contact approved! Username: @{html.quote(username)}")
                await callback_query.message.edit_text(callback_query.message.html_text + "\n\n-- Approved. Username shared. --", reply_markup=None)
            else:
                await execute_update("UPDATE contact_requests SET status = 'approved_no_username', updated_at = CURRENT_TIMESTAMP WHERE id = $1", req_id)
                await safe_send_message(author_uid, f"⚠️ Contact approved, but user has no public username.")
                await callback_query.message.edit_text(callback_query.message.html_text + "\n\n-- Approved, but no username to share. --", reply_markup=None)
        except Exception as e:
            logger.error(f"Error in contact approval: {e}")
            await callback_query.answer("Error fetching username.", show_alert=True)
            return
    else:
        await execute_update("UPDATE contact_requests SET status = 'denied', updated_at = CURRENT_TIMESTAMP WHERE id = $1", req_id)
        await safe_send_message(author_uid, f"❌ Contact request for Confession #{conf_id} was denied.")
        await callback_query.message.edit_text(callback_query.message.html_text + "\n\n-- Denied. --", reply_markup=None)
    
    await callback_query.answer("Response recorded.")

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
    
    if message.reply_to_message:
        target_id = message.reply_to_message.from_user.id
        target_name = message.reply_to_message.from_user.full_name
        target_username = message.reply_to_message.from_user.username
        
        profile_name = await get_profile_name(target_id)
        points = await get_user_points(target_id)
        
        response = f"👤 <b>User Information</b>\n\n<b>ID:</b> <code>{target_id}</code>\n<b>Name:</b> {target_name}\n"
        if target_username:
            response += f"<b>Username:</b> @{target_username}\n"
        response += f"<b>Profile:</b> {profile_name}\n<b>Points:</b> {points}\n"
        
        status = await fetch_one("SELECT is_blocked, blocked_until FROM user_status WHERE user_id = $1", target_id)
        if status and status['is_blocked']:
            if status['blocked_until']:
                response += f"<b>Status:</b> Blocked until {status['blocked_until'].strftime('%Y-%m-%d %H:%M')}\n"
            else:
                response += f"<b>Status:</b> Permanently blocked\n"
        else:
            response += f"<b>Status:</b> Active\n"
        
        await message.answer(response)
    else:
        parts = message.text.split()
        if len(parts) > 1:
            try:
                target_id = int(parts[1])
                profile_name = await get_profile_name(target_id)
                points = await get_user_points(target_id)
                response = f"👤 <b>User Information</b>\n\n<b>ID:</b> <code>{target_id}</code>\n<b>Profile:</b> {profile_name}\n<b>Points:</b> {points}\n"
                await message.answer(response)
            except ValueError:
                await message.answer("Invalid user ID.")
        else:
            await message.answer(f"<b>Your ID:</b> <code>{message.from_user.id}</code>")

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
    
    confirm_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Yes, Broadcast", callback_data="confirm_broadcast"),
         InlineKeyboardButton(text="❌ Cancel", callback_data="cancel_broadcast")]
    ])
    
    await message.answer(
        "⚠️ <b>Confirm Broadcast</b>\n\nAre you sure you want to broadcast this message to all users?",
        reply_markup=confirm_keyboard
    )
    
    await state.update_data(
        broadcast_message_id=message.reply_to_message.message_id,
        broadcast_chat_id=message.reply_to_message.chat.id
    )

@dp.callback_query(F.data == "confirm_broadcast")
async def confirm_broadcast(callback_query: types.CallbackQuery, state: FSMContext):
    if not await is_admin(callback_query.from_user.id):
        await callback_query.answer("Unauthorized.", show_alert=True)
        return
    
    data = await state.get_data()
    msg_id = data.get('broadcast_message_id')
    chat_id = data.get('broadcast_chat_id')
    
    if not msg_id or not chat_id:
        await callback_query.answer("Error: Message data not found.", show_alert=True)
        await state.clear()
        return
    
    users = await execute_query("SELECT user_id FROM user_status WHERE has_accepted_rules = TRUE AND is_blocked = FALSE")
    total = len(users)
    successful = 0
    failed = 0
    
    progress = await callback_query.message.answer(f"📤 Broadcasting... 0/{total}")
    
    for i, row in enumerate(users):
        try:
            await bot.copy_message(chat_id=row['user_id'], from_chat_id=chat_id, message_id=msg_id)
            successful += 1
        except Exception as e:
            failed += 1
            logger.warning(f"Broadcast failed to {row['user_id']}: {e}")
        
        if i % 10 == 0:
            try:
                await progress.edit_text(f"📤 Broadcasting... {successful+failed}/{total}")
            except:
                pass
        await asyncio.sleep(0.05)
    
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
        try:
            if db:
                async with db.acquire() as conn:
                    await conn.fetchval('SELECT 1')
        except Exception as e:
            logger.error(f"Database monitor error: {e}")
            try:
                await db.close()
            except:
                pass
            try:
                await create_db_pool()
                logger.info("Database reconnected")
            except Exception as reconnect_error:
                logger.error(f"Failed to reconnect: {reconnect_error}")
        await asyncio.sleep(300)

# --- Main Execution ---
async def main():
    http_runner = None
    try:
        # Clear any existing webhook first
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("✅ Webhook cleared")
        
        await setup()
        
        dp.message.middleware(BlockUserMiddleware())
        dp.callback_query.middleware(BlockUserMiddleware())
        
        await set_bot_commands()
        
        http_runner = await start_http_server()
        
        # OPTIONAL: Comment out if causing issues
        # asyncio.create_task(monitor_database())
        
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




