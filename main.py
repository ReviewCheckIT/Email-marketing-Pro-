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
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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
OWNER_ID = os.environ.get('BOT_OWNER_ID')
FB_JSON = os.environ.get('FIREBASE_CREDENTIALS_JSON')
FB_URL = os.environ.get('FIREBASE_DATABASE_URL')
RENDER_URL = os.environ.get('RENDER_EXTERNAL_URL')
PORT = int(os.environ.get('PORT', '8080'))

# Groq Keys
KEY_ENV = os.environ.get('GROQ_API_KEY', '')
GROQ_KEYS = [k.strip() for k in KEY_ENV.split(',') if k.strip()]

# Global State
active_tasks = {}
session_stats = {'total_emails': 0, 'start_time': None, 'status': 'Idle'}

# --- Firebase Initialization ---
try:
    if not firebase_admin._apps:
        if isinstance(FB_JSON, str):
            cred_dict = json.emails(FB_JSON)
        else:
            cred_dict = FB_JSON
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred, {'databaseURL': FB_URL})
    fs_client = firestore.client()
    logger.info("✅ Firebase Connected")
except Exception as e:
    logger.error(f"❌ Firebase Error: {e}")
    # We don't exit here to let the bot report the error to user via Telegram
    fs_client = None

# --- Helper Functions ---

def is_owner(uid):
    return str(uid) == str(OWNER_ID)

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
    """Send debug logs directly to Telegram chat"""
    try:
        await context.bot.send_message(uid, f"🛠 **Log:** {message}", parse_mode='Markdown')
    except: pass

# --- Groq Logic ---
async def get_expanded_keywords(base_kw):
    if not GROQ_KEYS: return [base_kw]
    
    url = "https://api.groq.com/openai/v1/chat/completions"
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": f"Generate 50 Play Store search terms for '{base_kw}'. CSV format only."}]
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
async def scrape_task(base_kw, context, uid, is_auto=False):
    context.user_data['stop_signal'] = False
    session_stats['status'] = f"Running: {base_kw}"
    session_stats['start_time'] = datetime.now()
    
    # Dashboard Control Message
    status_text = (
        f"🚀 **Search Started**\n"
        f"🔑 Keyword: `{base_kw}`\n"
        f"🎯 Filter: <10k Installs\n"
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

    countries = ['us', 'gb', 'ca', 'au', 'in']

    try:
        for kw_idx, kw in enumerate(keywords):
            if context.user_data.get('stop_signal'): break
            
            # Update user every 5 keywords so they know it's working
            if kw_idx % 5 == 0:
                try:
                    await context.bot.edit_message_text(
                        f"🔄 **Processing...**\n🗂 Key: `{kw}`\n📥 Leads Found: {new_count}\n⏳ Progress: {kw_idx}/{len(keywords)}",
                        chat_id=uid, message_id=status_msg.message_id, parse_mode='Markdown', reply_markup=markup
                    )
                except: pass

            for country in countries:
                if context.user_data.get('stop_signal'): break
                await asyncio.sleep(0.5)

                try:
                    results = await asyncio.to_thread(play_search, kw, n_hits=30, lang='en', country=country)
                    if not results: continue

                    for r in results:
                        if context.user_data.get('stop_signal'): break
                        
                        app_id = r['appId']
                        safe_id = app_id.replace('.', '_')
                        
                        # Check local duplicates first
                        if any(l['app_id'] == app_id for l in leads): continue
                        # Check DB duplicates
                        if ref.child(safe_id).get(): continue

                        try:
                            app = await asyncio.to_thread(app_details, app_id, lang='en', country=country)
                            if not app: continue

                            # Filters
                            installs = parse_installs(app.get('installs', '0'))
                            if installs >= 10000: continue
                            
                            email = app.get('developerEmail', '').lower().strip()
                            if not await validate_email(email): continue

                            data = {
                                'app_name': app.get('title'),
                                'app_id': app_id,
                                'email': email,
                                'website': app.get('developerWebsite', 'N/A'),
                                'installs': app.get('installs'),
                                'country': country,
                                'keyword': kw,
                                'date': datetime.now().isoformat()
                            }
                            
                            ref.child(safe_id).set(data)
                            leads.append(data)
                            new_count += 1
                            session_stats['total_emails'] += 1

                        except: continue
                except: continue
        
        # Finish
        session_stats['status'] = "Idle"
        if leads:
            si = io.StringIO()
            cw = csv.writer(si)
            cw.writerow(['Name', 'Email', 'Website', 'Installs', 'Country'])
            for v in leads: cw.writerow([v['app_name'], v['email'], v['website'], v['installs'], v['country']])
            output = io.BytesIO(si.getvalue().encode('utf-8'))
            output.name = f"Leads_{base_kw}.csv"
            
            await context.bot.send_document(uid, output, caption=f"✅ Done! Found {new_count} leads.")
        else:
            await context.bot.send_message(uid, "❌ No valid leads found for this search.")

    except Exception as e:
        await send_log(context, uid, f"Crash Error: {e}")
    finally:
        if not context.user_data.get('stop_signal') and is_auto:
            await asyncio.sleep(2)
            await execute_auto_search(context, uid)

async def execute_auto_search(context, uid):
    if context.user_data.get('stop_signal'): return
    try:
        keywords_ref = fs_client.collection('artifacts').document('keyword-bot-pro').collection('public').document('data').collection('keywords')
        docs = keywords_ref.limit(1).get()
        if docs:
            kw = docs[0].to_dict().get('word')
            docs[0].reference.delete()
            active_tasks[uid] = asyncio.create_task(scrape_task(kw, context, uid, is_auto=True))
        else:
            await context.bot.send_message(uid, "⚠️ Database empty. Auto mode finished.")
    except Exception as e:
        await send_log(context, uid, f"Auto Mode Error: {e}")

# --- Handlers ---

async def start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not is_owner(u.effective_user.id): return
    
    text = (
        "🤖 **Play Store Scraper Dashboard**\n\n"
        "🟢 **System Status:** Online\n"
        "📈 **Session emails:** " + str(session_stats['total_emails']) + "\n"
        "⚙️ **Current Status:** " + session_stats['status'] + "\n\n"
        "👇 Select an action:"
    )
    
    btns = [
        [InlineKeyboardButton("✅ Health Check", callback_data='check_health'), InlineKeyboardButton("📊 Get Stats", callback_data='stats')],
        [InlineKeyboardButton("🤖 Auto Mode", callback_data='auto_s'), InlineKeyboardButton("♻️ Reset Bot", callback_data='refresh_bot')]
    ]
    await u.message.reply_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(btns))

async def cb_handler(u: Update, c: ContextTypes.DEFAULT_TYPE):
    q = u.callback_query
    uid = u.effective_user.id
    await q.answer()
    
    if q.data == 'check_health':
        fb_status = "✅ Connected" if fs_client else "❌ Failed"
        tasks = len(active_tasks)
        await q.message.reply_text(f"🩺 **System Diagnosis:**\n\n• Firebase: {fb_status}\n• Active Tasks: {tasks}\n• Groq Keys: {len(GROQ_KEYS)}\n• Memory: OK", parse_mode='Markdown')

    elif q.data == 'stats':
        dur = "0m"
        if session_stats['start_time']:
            delta = datetime.now() - session_stats['start_time']
            dur = f"{delta.seconds // 60}m {delta.seconds % 60}s"
            
        msg = (
            f"📊 **Live Statistics**\n\n"
            f"📥 Leads Collected: `{session_stats['total_leads']}`\n"
            f"⏱ Runtime: `{dur}`\n"
            f"⚙️ Status: `{session_stats['status']}`"
        )
        await q.message.reply_text(msg, parse_mode='Markdown')

    elif q.data == 'auto_s':
        c.user_data['stop_signal'] = False
        await q.edit_message_text("🔄 Initializing Auto Mode from Database...")
        await execute_auto_search(c, uid)

    elif q.data == 'stop_loop':
        c.user_data['stop_signal'] = True
        if uid in active_tasks:
            active_tasks[uid].cancel()
        session_stats['status'] = "Stopped"
        await q.message.reply_text("🛑 Process Forcefully Stopped.")

    elif q.data == 'refresh_bot':
        c.user_data.clear()
        session_stats['total_leads'] = 0
        session_stats['status'] = "Idle"
        if uid in active_tasks: active_tasks[uid].cancel()
        await q.message.reply_text("♻️ Bot Refreshed.")

async def message_handler(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not is_owner(u.effective_user.id): return
    uid = u.effective_user.id
    
    if session_stats['status'] != "Idle" and not "Stopped" in session_stats['status']:
        await u.message.reply_text(f"⚠️ Bot is busy! \nCurrent Task: {session_stats['status']}\nPress STOP first.")
        return

    active_tasks[uid] = asyncio.create_task(scrape_task(u.message.text, c, uid))

# --- Main ---
def main():
    if not TOKEN: sys.exit("Missing TOKEN")
    
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(cb_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    
    # Robust Deployment Logic
    if RENDER_URL:
        # Use Webhook for Render
        app.run_webhook(listen="0.0.0.0", port=PORT, url_path=TOKEN, webhook_url=f"{RENDER_URL}/{TOKEN}")
    else:
        # Fallback to Polling (Good for testing)
        print("⚠️ No RENDER_URL found. Using Polling...")
        app.run_polling()

if __name__ == "__main__":
    main()
