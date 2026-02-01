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

# Groq Keys (Comma Separated)
KEY_ENV = os.environ.get('GROQ_API_KEY', '')
GROQ_KEYS = [k.strip() for k in KEY_ENV.split(',') if k.strip()]

# Global State
active_tasks = {}

# --- Firebase Initialization ---
try:
    if not firebase_admin._apps:
        # Handle cases where JSON might be passed as a string or raw
        if isinstance(FB_JSON, str):
            cred_dict = json.loads(FB_JSON)
        else:
            cred_dict = FB_JSON
            
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred, {'databaseURL': FB_URL})
    fs_client = firestore.client()
    logger.info("✅ Firebase Connected Successfully")
except Exception as e:
    logger.error(f"❌ Firebase Connection Failed: {e}")
    sys.exit(1)

# --- Helper Functions ---

def is_owner(uid):
    return str(uid) == str(OWNER_ID)

def parse_installs(install_str):
    """Converts '1,000+' string to integer 1000"""
    if not install_str: return 0
    try:
        # Remove + and , and spaces
        clean = re.sub(r'[^\d]', '', str(install_str))
        return int(clean) if clean else 0
    except:
        return 0

async def validate_email(email):
    """
    1. Checks Regex format.
    2. Checks DNS MX records to ensure domain can receive email.
    """
    if not email: return False
    
    # 1. Basic Regex
    if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
        return False
        
    # 2. DNS Check (Non-blocking)
    domain = email.split('@')[-1]
    try:
        # Run DNS lookup in thread to prevent blocking bot
        answers = await asyncio.to_thread(dns.resolver.resolve, domain, 'MX')
        return bool(answers)
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.NoNameservers, Exception):
        return False

# --- Groq AI Logic (Optimized Rotation) ---
async def get_expanded_keywords(base_kw):
    if not GROQ_KEYS: 
        return [base_kw]
    
    url = "https://api.groq.com/openai/v1/chat/completions"
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": f"Generate 50 relevant Play Store search terms related to '{base_kw}'. Provide output strictly as a comma-separated list."}]
    }

    # Rotation Logic: Try keys until one works
    for i in range(len(GROQ_KEYS)):
        # Calculate key index based on random or sequential logic?
        # Simple round-robin approach isn't strictly needed for low volume, just try next if fail.
        api_key = GROQ_KEYS[i]
        headers = {'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'}
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=payload, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        res = data['choices'][0]['message']['content']
                        # Cleanup response
                        keywords = [k.strip() for k in res.split(',') if k.strip()]
                        return keywords[:50] # Limit to 50
                    elif resp.status == 429:
                        logger.warning(f"⚠️ Key {i} Rate Limited. Switching...")
                        continue # Try next key
                    else:
                        logger.error(f"Groq API Error {resp.status}: {await resp.text()}")
        except Exception as e:
            logger.error(f"Groq Connection Error: {e}")
            continue

    logger.warning("⚠️ All Groq keys failed. Using base keyword.")
    return [base_kw]

# --- Core Scraper Engine ---
async def scrape_task(base_kw, context, uid, is_auto=False):
    context.user_data['stop_signal'] = False
    
    # Send initial message
    stop_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🛑 STOP SEARCH", callback_data='stop_loop')]])
    status_msg = await context.bot.send_message(uid, f"🔍 <b>Starting Search:</b> {base_kw}\n⚙️ Filters: <10k Installs, Valid Emails", parse_mode='HTML', reply_markup=stop_markup)
    
    new_count = 0
    session_leads = []
    
    # Get keywords
    keywords = await get_expanded_keywords(base_kw)
    logger.info(f"Keywords generated: {len(keywords)}")
    
    # Priority Countries (Tier 1 & 2)
    countries = ['us', 'gb', 'ca', 'au', 'de', 'fr', 'in', 'br', 'id'] 
    
    ref = db.reference('scraped_leads')

    try:
        for kw in keywords:
            if context.user_data.get('stop_signal'): break

            for country in countries:
                if context.user_data.get('stop_signal'): break
                
                # Small delay to respect rate limits & event loop
                await asyncio.sleep(0.5)

                try:
                    # BLOCKING IO FIX: Run scraper in thread
                    results = await asyncio.to_thread(play_search, kw, n_hits=30, lang='en', country=country)
                    
                    if not results: continue

                    for r in results:
                        if context.user_data.get('stop_signal'): break
                        
                        app_id = r['appId']
                        
                        # Optimization: Check if we already have this App ID to save API calls/Time
                        # Note: Firebase keys cannot contain '.', replace with '_'
                        safe_app_id = app_id.replace('.', '_')
                        if ref.child(safe_app_id).get():
                            continue

                        try:
                            # BLOCKING IO FIX: Run detailed details in thread
                            app = await asyncio.to_thread(app_details, app_id, lang='en', country=country)
                            
                            if not app: continue

                            # --- FILTER 1: Installs < 10,000 ---
                            installs_int = parse_installs(app.get('installs', '0'))
                            if installs_int >= 10000:
                                continue # Skip popular apps

                            # --- FILTER 2: Email Existence ---
                            email = app.get('developerEmail', '').lower().strip()
                            if not email: continue

                            # --- FILTER 3: Email Validation (Regex + DNS) ---
                            is_valid = await validate_email(email)
                            if not is_valid: continue

                            # --- Data Enrichment ---
                            website = app.get('developerWebsite', 'N/A')
                            title = app.get('title', 'Unknown')
                            
                            lead_data = {
                                'app_name': title,
                                'app_id': app_id,
                                'email': email,
                                'website': website,
                                'installs_text': app.get('installs'),
                                'installs_count': installs_int,
                                'country': country,
                                'keyword': kw,
                                'scraped_at': datetime.now().isoformat()
                            }

                            # Save to Firebase
                            ref.child(safe_app_id).set(lead_data)
                            session_leads.append(lead_data)
                            new_count += 1
                            
                            # Update status every 5 leads to avoid spamming API
                            if new_count % 5 == 0:
                                try:
                                    await context.bot.edit_message_text(
                                        chat_id=uid,
                                        message_id=status_msg.message_id,
                                        text=f"🔍 <b>Running:</b> {base_kw}\nFound: {new_count} leads\nCurrent KW: {kw} ({country})",
                                        parse_mode='HTML',
                                        reply_markup=stop_markup
                                    )
                                except: pass

                        except Exception as e:
                            # logger.error(f"App Detail Error: {e}")
                            continue
                except Exception as e:
                    logger.error(f"Search Error: {e}")
                    continue
        
        # Final Report
        if session_leads:
            # Create CSV in memory
            si = io.StringIO()
            cw = csv.writer(si)
            cw.writerow(['App Name', 'Email', 'Website', 'Installs', 'Country', 'Keyword'])
            for v in session_leads:
                cw.writerow([v['app_name'], v['email'], v['website'], v['installs_text'], v['country'], v['keyword']])
            
            output = io.BytesIO(si.getvalue().encode('utf-8'))
            output.name = f"Leads_{base_kw}_{datetime.now().strftime('%Y%m%d')}.csv"
            
            await context.bot.send_document(
                uid, 
                document=output, 
                caption=f"✅ <b>Scraping Completed</b>\n\n🎯 Keyword: {base_kw}\n📥 Total Leads: {new_count}\n📉 Filter: < 10k Installs",
                parse_mode='HTML'
            )
        else:
            await context.bot.send_message(uid, f"❌ No leads found for {base_kw} with current filters.")

    except asyncio.CancelledError:
        logger.info("Task cancelled by user.")
    finally:
        # Check for Auto Mode continuation
        if not context.user_data.get('stop_signal') and is_auto:
            await asyncio.sleep(2)
            await execute_auto_search(context, uid)

# --- Auto Search Controller ---
async def execute_auto_search(context, uid):
    if context.user_data.get('stop_signal'): return
    
    # Path to your keywords in Firestore
    keywords_ref = fs_client.collection('artifacts').document('keyword-bot-pro').collection('public').document('data').collection('keywords')
    
    # Fetch 1 document
    docs = keywords_ref.limit(1).get()
    
    if docs:
        doc = docs[0]
        data = doc.to_dict()
        kw = data.get('word')
        
        # Delete from queue so we don't process again
        doc.reference.delete()
        
        await context.bot.send_message(uid, f"🤖 <b>Auto Mode:</b> Picked '{kw}'", parse_mode='HTML')
        
        # Create Task
        task = asyncio.create_task(scrape_task(kw, context, uid, is_auto=True))
        active_tasks[uid] = task
    else:
        await context.bot.send_message(uid, "⚠️ <b>Auto Mode Finished:</b> No more keywords in database.", parse_mode='HTML')

# --- Telegram Handlers ---

async def start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not is_owner(u.effective_user.id): return
    
    btns = [
        [InlineKeyboardButton("🤖 Start Auto Mode", callback_data='auto_s')],
        [InlineKeyboardButton("♻️ Reset Bot", callback_data='refresh_bot')]
    ]
    await u.message.reply_text(
        "🚀 <b>Play Store Scraper Bot 2.0</b>\n\n"
        "Send any keyword to start scraping immediately.\n"
        "Or use Auto Mode to pull from database.",
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(btns)
    )

async def cb_handler(u: Update, c: ContextTypes.DEFAULT_TYPE):
    q = u.callback_query
    uid = u.effective_user.id
    await q.answer()
    
    if q.data == 'auto_s':
        c.user_data['stop_signal'] = False
        await q.edit_message_text("🔄 Initializing Auto Mode...")
        await execute_auto_search(c, uid)

    elif q.data == 'stop_loop':
        c.user_data['stop_signal'] = True
        
        # Cancel Async Task
        if uid in active_tasks:
            active_tasks[uid].cancel()
            del active_tasks[uid]
            
        await q.edit_message_text("🛑 <b>STOPPED.</b> Bot is now idle.", parse_mode='HTML')

    elif q.data == 'refresh_bot':
        c.user_data.clear()
        if uid in active_tasks:
            active_tasks[uid].cancel()
        await q.edit_message_text("♻️ Bot Memory Cleared. Ready for new tasks.")

async def message_handler(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not is_owner(u.effective_user.id): return
    
    uid = u.effective_user.id
    text = u.message.text
    
    if uid in active_tasks and not active_tasks[uid].done():
        await u.message.reply_text("⚠️ A task is already running. Please STOP it first or wait.")
        return

    c.user_data['stop_signal'] = False
    task = asyncio.create_task(scrape_task(text, c, uid, is_auto=False))
    active_tasks[uid] = task

# --- Main Application ---
def main():
    if not TOKEN:
        print("Error: TELEGRAM_BOT_TOKEN is missing.")
        sys.exit(1)

    app = Application.builder().token(TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(cb_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    
    # Webhook Setup for Render
    if RENDER_URL:
        webhook_url = f"{RENDER_URL}/{TOKEN}"
        logger.info(f"Setting webhook to: {webhook_url}")
        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=TOKEN,
            webhook_url=webhook_url,
            drop_pending_updates=True
        )
    else:
        logger.info("Starting Polling...")
        app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
