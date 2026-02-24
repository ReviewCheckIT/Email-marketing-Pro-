# -*- coding: utf-8 -*-
import logging
import os
import json
import asyncio
import csv
import io
import sys
import re
import aiohttp
import time
from datetime import datetime

# Third-party imports
import dns.resolver
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters
from google_play_scraper import search as play_search, app as app_details

# Firebase
import firebase_admin
from firebase_admin import credentials, db, firestore

# --- Logging Setup ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Configuration & Env Variables ---
TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')

# Multiple Admin Setup: Split IDs by comma
OWNER_IDS_ENV = os.environ.get('BOT_OWNER_ID', '')
# Example env: "123456,789012,345678"
OWNER_IDS = [str(oid).strip() for oid in OWNER_IDS_ENV.split(',') if oid.strip()]

FB_JSON = os.environ.get('FIREBASE_CREDENTIALS_JSON')
FB_URL = os.environ.get('FIREBASE_DATABASE_URL')
RENDER_URL = os.environ.get('RENDER_EXTERNAL_URL')
PORT = int(os.environ.get('PORT', '8080'))

# Groq Keys
KEY_ENV = os.environ.get('GROQ_API_KEY', '')
GROQ_KEYS = [k.strip() for k in KEY_ENV.split(',') if k.strip()]

# Global State
active_tasks = {}
# 'active_by_name' stores the name of the admin currently running the task
session_stats = {
    'total_leads': 0, 
    'start_time': None, 
    'status': 'Idle', 
    'active_by_id': None,
    'active_by_name': None
}

# --- Firebase Initialization ---
try:
    if not firebase_admin._apps:
        if isinstance(FB_JSON, str):
            cred_dict = json.loads(FB_JSON)
        else:
            cred_dict = FB_JSON
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred, {'databaseURL': FB_URL})
    fs_client = firestore.client()
    logger.info("✅ Firebase Connected")
except Exception as e:
    logger.error(f"❌ Firebase Error: {e}")
    fs_client = None

# --- Helper Functions ---

def is_owner(uid):
    """Check if user is one of the admins"""
    return str(uid) in OWNER_IDS

def parse_installs(install_str):
    if not install_str: return 0
    try:
        clean = re.sub(r'[^\d]', '', str(install_str))
        return int(clean) if clean else 0
    except: return 0

async def validate_email(email):
    """Email validation with DNS check"""
    if not email: return False
    if not re.match(r"[^@]+@[^@]+\.[^@]+", email): return False
    domain = email.split('@')[-1]
    try:
        answers = await asyncio.to_thread(dns.resolver.resolve, domain, 'MX')
        return bool(answers)
    except: return False

async def send_log(context, uid, message):
    try:
        await context.bot.send_message(uid, f"🛠 **Log:** {message}", parse_mode='Markdown')
    except: pass

# --- Groq Logic ---
async def get_expanded_keywords(base_kw):
    if not GROQ_KEYS: return [base_kw]
    
    url = "https://api.groq.com/openai/v1/chat/completions"
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": f"Generate 100 Play Store search terms for '{base_kw}'. CSV format only."}]
    }

    for i, api_key in enumerate(GROQ_KEYS):
        headers = {'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=payload, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        res = data['choices'][0]['message']['content']
                        return [k.strip() for k in res.split(',') if k.strip()][:50]
        except: continue
    return [base_kw]

# --- Scraper Engine ---
async def scrape_task(base_kw, context, uid, user_name, is_auto=False):
    context.user_data['stop_signal'] = False
    
    # Set Global Busy State
    session_stats['status'] = f"Running: {base_kw}"
    session_stats['start_time'] = datetime.now()
    session_stats['active_by_id'] = str(uid)
    session_stats['active_by_name'] = user_name
    
    # Updated status message with score filter
    status_text = (
        f"🚀 **Search Started by {user_name}**\n"
        f"🔑 Keyword: `{base_kw}`\n"
        f"🎯 Filter: <50k Installs, Score ≤ 3.8, and at least one 1-2⭐ rating\n"
        f"📞 Features: Email + Phone Extraction + Rating Details\n"
        f"💾 Saving to: `scraped_emails`\n"
        f"⏳ Generating Keywords..."
    )
    
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Live Stats", callback_data='stats'), InlineKeyboardButton("🛑 STOP", callback_data='stop_loop')]
    ])
    
    status_msg = await context.bot.send_message(uid, status_text, parse_mode='Markdown', reply_markup=markup)
    
    leads = []
    new_count = 0
    ref = db.reference('scraped_emails')
    
    keywords = await get_expanded_keywords(base_kw)
    await context.bot.edit_message_text(f"✅ Generated {len(keywords)} keywords. Starting scraper...", chat_id=uid, message_id=status_msg.message_id, reply_markup=markup)

    countries = ['us', 'gb', 'ca', 'au', 'de', 'fr', 'sg', 'ae', 'nz']

    try:
        for kw_idx, kw in enumerate(keywords):
            if context.user_data.get('stop_signal'): break
            if kw_idx % 5 == 0:
                try:
                    await context.bot.edit_message_text(
                        f"🔄 **Processing...**\n👤 User: {user_name}\n🗂 Key: `{kw}`\n📥 Leads Found: {new_count}\n⏳ Progress: {kw_idx}/{len(keywords)}",
                        chat_id=uid, message_id=status_msg.message_id, parse_mode='Markdown', reply_markup=markup
                    )
                except: pass

            for country in countries:
                if context.user_data.get('stop_signal'): break
                await asyncio.sleep(0.5)

                try:
                    results = await asyncio.to_thread(play_search, kw, n_hits=100, lang='en', country=country)
                    if not results: continue

                    for r in results:
                        if context.user_data.get('stop_signal'): break
                        app_id = r['appId']
                        
                        try:
                            app = await asyncio.to_thread(app_details, app_id, lang='en', country=country)
                            if not app: continue

                            installs = parse_installs(app.get('installs', '0'))
                            if installs >= 50000: continue

                            # --- NEW: Filter by average score (≤ 3.8) ---
                            score = app.get('score', 0.0)
                            if score > 3.8:
                                continue

                            # Filter by low ratings (1-star or 2-star)
                            histogram = app.get('histogram')
                            if not histogram or len(histogram) < 5:
                                continue  # No ratings at all -> skip
                            if histogram[0] == 0 and histogram[1] == 0:
                                continue  # No 1-star or 2-star ratings -> skip
                            
                            email = app.get('developerEmail', '').lower().strip()
                            if not await validate_email(email): continue
                            
                            # Extract Phone Number (improved: use fallback if missing or empty)
                            phone = app.get('developerPhone')
                            if not phone:
                                phone = 'N/A'
                            
                            # Rating details
                            ratings_1 = histogram[0] if len(histogram) > 0 else 0
                            ratings_2 = histogram[1] if len(histogram) > 1 else 0
                            ratings_3 = histogram[2] if len(histogram) > 2 else 0
                            ratings_4 = histogram[3] if len(histogram) > 3 else 0
                            ratings_5 = histogram[4] if len(histogram) > 4 else 0
                            total_ratings = sum(histogram)
                            
                            safe_key = email.replace('.', '_').replace('@', '_at_')
                            if ref.child(safe_key).get(): continue

                            data = {
                                'app_name': app.get('title'),
                                'app_id': app_id,
                                'email': email,
                                'phone': phone,
                                'website': app.get('developerWebsite', 'N/A'),
                                'installs': app.get('installs'),
                                'country': country,
                                'keyword': kw,
                                'date': datetime.now().isoformat(),
                                # Rating fields
                                'score': score,
                                'total_ratings': total_ratings,
                                'ratings_1': ratings_1,
                                'ratings_2': ratings_2,
                                'ratings_3': ratings_3,
                                'ratings_4': ratings_4,
                                'ratings_5': ratings_5
                            }
                            
                            ref.child(safe_key).set(data)
                            leads.append(data)
                            new_count += 1
                            session_stats['total_leads'] += 1

                        except: continue
                except: continue
        
        # Reset Busy State
        session_stats['status'] = "Idle"
        session_stats['active_by_id'] = None
        session_stats['active_by_name'] = None

        if leads:
            si = io.StringIO()
            cw = csv.writer(si)
            # Extended header with rating details
            cw.writerow([
                'Name', 'Email', 'Phone', 'Website', 'Installs', 'Country',
                'Score', 'Total Ratings', '1-Star', '2-Star', '3-Star', '4-Star', '5-Star'
            ])
            for v in leads: 
                cw.writerow([
                    v['app_name'], 
                    v['email'], 
                    v.get('phone', 'N/A'), 
                    v['website'], 
                    v['installs'], 
                    v['country'],
                    v.get('score', 0.0),
                    v.get('total_ratings', 0),
                    v.get('ratings_1', 0),
                    v.get('ratings_2', 0),
                    v.get('ratings_3', 0),
                    v.get('ratings_4', 0),
                    v.get('ratings_5', 0)
                ])
            output = io.BytesIO(si.getvalue().encode('utf-8'))
            output.name = f"Leads_{base_kw}.csv"
            
            await context.bot.send_document(uid, output, caption=f"✅ Done! Found {new_count} leads.\nSaved to: scraped_emails")
        else:
            await context.bot.send_message(uid, "❌ No valid leads found for this search.")

    except Exception as e:
        await send_log(context, uid, f"Crash Error: {e}")
        # Reset on crash
        session_stats['status'] = "Idle"
        session_stats['active_by_id'] = None
    finally:
        if not context.user_data.get('stop_signal') and is_auto:
            await asyncio.sleep(2)
            await execute_auto_search(context, uid, user_name)

async def execute_auto_search(context, uid, user_name):
    if context.user_data.get('stop_signal'): return
    try:
        keywords_ref = fs_client.collection('artifacts').document('keyword-bot-pro').collection('public').document('data').collection('keywords')
        docs = keywords_ref.limit(1).get()
        if docs:
            kw = docs[0].to_dict().get('word')
            docs[0].reference.delete()
            active_tasks[uid] = asyncio.create_task(scrape_task(kw, context, uid, user_name, is_auto=True))
        else:
            await context.bot.send_message(uid, "⚠️ Database empty. Auto mode finished.")
            session_stats['status'] = "Idle"
            session_stats['active_by_id'] = None
    except Exception as e:
        await send_log(context, uid, f"Auto Mode Error: {e}")
        session_stats['status'] = "Idle"

# --- Action Functions (used by both command handlers and callback handler) ---

async def health_action(update: Update, context: ContextTypes.DEFAULT_TYPE, is_callback=False):
    """Health check logic"""
    uid = str(update.effective_user.id)
    try:
        db.reference('health_check').set({"status": "ok", "time": str(datetime.now())})
        fb_status = "✅ Connected & Writeable"
    except Exception as e:
        fb_status = f"❌ Error: {str(e)[:50]}"
    
    tasks = len(active_tasks)
    msg = f"🩺 **System Diagnosis:**\n\n• Firebase: {fb_status}\n• DB Path: scraped_emails\n• Active Tasks: {tasks}\n• Groq Keys: {len(GROQ_KEYS)}"
    
    if is_callback:
        await update.callback_query.message.reply_text(msg, parse_mode='Markdown')
    else:
        await update.message.reply_text(msg, parse_mode='Markdown')

async def download_action(update: Update, context: ContextTypes.DEFAULT_TYPE, is_callback=False):
    """Download all database logic"""
    uid = str(update.effective_user.id)
    if is_callback:
        await update.callback_query.message.reply_text("⏳ **Fetching all data from Database...**\nDepending on size, this may take a few seconds.")
    else:
        await update.message.reply_text("⏳ **Fetching all data from Database...**\nDepending on size, this may take a few seconds.")
    
    try:
        ref = db.reference('scraped_emails')
        all_data = await asyncio.to_thread(ref.get)
        
        if all_data:
            si = io.StringIO()
            cw = csv.writer(si)
            cw.writerow([
                'App Name', 'Email', 'Phone', 'Website', 'Installs', 'Country', 'Keyword', 'Date',
                'Score', 'Total Ratings', '1-Star', '2-Star', '3-Star', '4-Star', '5-Star'
            ])
            
            count = 0
            for key, v in all_data.items():
                if isinstance(v, dict):
                    cw.writerow([
                        v.get('app_name', 'N/A'), 
                        v.get('email', 'N/A'), 
                        v.get('phone', 'N/A'),
                        v.get('website', 'N/A'), 
                        v.get('installs', 'N/A'), 
                        v.get('country', 'N/A'), 
                        v.get('keyword', 'N/A'),
                        v.get('date', 'N/A'),
                        v.get('score', 0.0),
                        v.get('total_ratings', 0),
                        v.get('ratings_1', 0),
                        v.get('ratings_2', 0),
                        v.get('ratings_3', 0),
                        v.get('ratings_4', 0),
                        v.get('ratings_5', 0)
                    ])
                    count += 1
            
            output = io.BytesIO(si.getvalue().encode('utf-8'))
            output.name = f"Full_Database_{datetime.now().strftime('%Y%m%d')}.csv"
            
            await context.bot.send_document(uid, output, caption=f"✅ **Database Downloaded**\n\n📂 Total Records: {count}")
        else:
            await context.bot.send_message(uid, "⚠️ Database is empty.")
    except Exception as e:
        logger.error(f"Download Error: {e}")
        await context.bot.send_message(uid, f"❌ Error downloading: {e}")

async def stats_action(update: Update, context: ContextTypes.DEFAULT_TYPE, is_callback=False):
    """Live statistics logic"""
    dur = "0m"
    if session_stats['start_time']:
        delta = datetime.now() - session_stats['start_time']
        dur = f"{delta.seconds // 60}m {delta.seconds % 60}s"
        
    status_info = session_stats['status']
    if session_stats['active_by_name'] and session_stats['status'] != 'Idle':
        status_info += f" (Run by: {session_stats['active_by_name']})"

    msg = (
        f"📊 **Live Statistics**\n\n"
        f"📥 Leads Collected: `{session_stats['total_leads']}`\n"
        f"⏱ Runtime: `{dur}`\n"
        f"⚙️ Status: `{status_info}`"
    )
    
    if is_callback:
        await update.callback_query.message.reply_text(msg, parse_mode='Markdown')
    else:
        await update.message.reply_text(msg, parse_mode='Markdown')

async def auto_action(update: Update, context: ContextTypes.DEFAULT_TYPE, is_callback=False):
    """Auto mode logic"""
    uid = str(update.effective_user.id)
    user_name = update.effective_user.first_name
    
    # Check busy status
    if session_stats['status'] != "Idle" and session_stats['active_by_id'] != uid:
        msg = f"⚠️ **Busy!**\nAdmin **{session_stats['active_by_name']}** is currently running a task.\nPlease wait."
        if is_callback:
            await update.callback_query.message.reply_text(msg)
        else:
            await update.message.reply_text(msg)
        return

    context.user_data['stop_signal'] = False
    if is_callback:
        await update.callback_query.edit_message_text(f"🔄 Initializing Auto Mode (User: {user_name})...")
    else:
        await update.message.reply_text(f"🔄 Initializing Auto Mode (User: {user_name})...")
    await execute_auto_search(context, uid, user_name)

async def refresh_action(update: Update, context: ContextTypes.DEFAULT_TYPE, is_callback=False):
    """Refresh bot logic"""
    uid = str(update.effective_user.id)
    context.user_data.clear()
    session_stats['total_leads'] = 0
    session_stats['status'] = "Idle"
    session_stats['active_by_id'] = None
    session_stats['active_by_name'] = None
    if uid in active_tasks:
        active_tasks[uid].cancel()
    
    msg = "♻️ Bot Refreshed."
    if is_callback:
        await update.callback_query.message.reply_text(msg)
    else:
        await update.message.reply_text(msg)

# --- Command Handlers ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not is_owner(uid): return
    
    status_msg = session_stats['status']
    if session_stats['status'] != "Idle" and session_stats['active_by_name']:
        status_msg += f" (by {session_stats['active_by_name']})"

    text = (
        "🤖 **Play Store Scraper Dashboard (Multi-Admin)**\n\n"
        "🟢 **System Status:** Online\n"
        "📈 **Session Leads:** " + str(session_stats['total_leads']) + "\n"
        "📂 **DB Path:** `scraped_emails`\n"
        "⚙️ **Current Status:** " + status_msg + "\n\n"
        "👇 Select an action:"
    )
    
    btns = [
        [InlineKeyboardButton("✅ Health Check", callback_data='check_health'), InlineKeyboardButton("📥 Download All DB", callback_data='dl_all')],
        [InlineKeyboardButton("🤖 Auto Mode", callback_data='auto_s'), InlineKeyboardButton("♻️ Reset Bot", callback_data='refresh_bot')],
        [InlineKeyboardButton("📊 Live Stats", callback_data='stats')]
    ]
    await update.message.reply_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(btns))

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not is_owner(uid): return
    await stats_action(update, context, is_callback=False)

async def health_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not is_owner(uid): return
    await health_action(update, context, is_callback=False)

async def download_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not is_owner(uid): return
    await download_action(update, context, is_callback=False)

async def auto_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not is_owner(uid): return
    await auto_action(update, context, is_callback=False)

async def refresh_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not is_owner(uid): return
    await refresh_action(update, context, is_callback=False)

# --- Callback Query Handler ---
async def cb_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = str(update.effective_user.id)
    user_name = update.effective_user.first_name
    await q.answer()
    
    if not is_owner(uid): return

    if q.data == 'check_health':
        await health_action(update, context, is_callback=True)
    elif q.data == 'dl_all':
        await download_action(update, context, is_callback=True)
    elif q.data == 'stats':
        await stats_action(update, context, is_callback=True)
    elif q.data == 'auto_s':
        # Special case: auto mode uses its own logic with edit_message
        # Check busy status
        if session_stats['status'] != "Idle" and session_stats['active_by_id'] != uid:
            await q.message.reply_text(f"⚠️ **Busy!**\nAdmin **{session_stats['active_by_name']}** is currently running a task.\nPlease wait.")
            return
        context.user_data['stop_signal'] = False
        await q.edit_message_text(f"🔄 Initializing Auto Mode (User: {user_name})...")
        await execute_auto_search(context, uid, user_name)
    elif q.data == 'refresh_bot':
        await refresh_action(update, context, is_callback=True)
    elif q.data == 'stop_loop':
        # Stop logic remains here as it's specific to running task
        if session_stats['active_by_id'] and session_stats['active_by_id'] != uid:
            await q.message.reply_text(f"⚠️ This task was started by **{session_stats['active_by_name']}**. Only they can stop it.")
            return
        context.user_data['stop_signal'] = True
        if uid in active_tasks:
            active_tasks[uid].cancel()
        session_stats['status'] = "Stopped"
        session_stats['active_by_id'] = None
        await q.message.reply_text("🛑 Process Forcefully Stopped.")

# --- Message Handler (for keyword input) ---
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not is_owner(uid): return
    
    # BUSY CHECK LOGIC
    if session_stats['status'] != "Idle" and not "Stopped" in session_stats['status']:
        # If the user is NOT the one who started the task
        if session_stats['active_by_id'] != uid:
            worker_name = session_stats['active_by_name']
            await update.message.reply_text(
                f"⚠️ **System Busy!**\n\n"
                f"👤 Admin **{worker_name}** is currently running a task.\n"
                f"⚙️ Status: {session_stats['status']}\n\n"
                f"Please wait until they finish or ask them to stop."
            )
            return
        else:
             await update.message.reply_text(f"⚠️ You already have a task running! Press STOP first.")
             return

    user_name = update.effective_user.first_name
    active_tasks[uid] = asyncio.create_task(scrape_task(update.message.text, context, uid, user_name))

# --- Function to set persistent menu ---
async def setup_persistent_menu(app: Application):
    """Set the bot's command list (persistent menu)."""
    commands = [
        BotCommand("start", "ড্যাশবোর্ড দেখুন"),
        BotCommand("stats", "লাইভ পরিসংখ্যান"),
        BotCommand("health", "সিস্টেম হেলথ চেক"),
        BotCommand("download", "সম্পূর্ণ ডাটাবেস ডাউনলোড"),
        BotCommand("auto", "অটো মোড চালু করুন"),
        BotCommand("refresh", "বট রিফ্রেশ করুন"),
    ]
    await app.bot.set_my_commands(commands)
    logger.info("✅ Persistent menu set up with commands.")

# --- Main ---
def main():
    if not TOKEN:
        sys.exit("Missing TOKEN")
    
    app = Application.builder().token(TOKEN).build()
    
    # Setup persistent menu (async call)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(setup_persistent_menu(app))
    
    # Add command handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("health", health_command))
    app.add_handler(CommandHandler("download", download_command))
    app.add_handler(CommandHandler("auto", auto_command))
    app.add_handler(CommandHandler("refresh", refresh_command))
    
    # Add callback query handler and message handler
    app.add_handler(CallbackQueryHandler(cb_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    
    if RENDER_URL:
        app.run_webhook(listen="0.0.0.0", port=PORT, url_path=TOKEN, webhook_url=f"{RENDER_URL}/{TOKEN}")
    else:
        print("⚠️ No RENDER_URL found. Using Polling...")
        app.run_polling()

if __name__ == "__main__":
    main()
