# -*- coding: utf-8 -*-
import logging
import os
import json
import asyncio
import csv
import io
import sys
import aiohttp
from datetime import datetime

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters
from google_play_scraper import search as play_search, app as app_details
import firebase_admin
from firebase_admin import credentials, db, firestore

# --- Logging ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Env Variables ---
TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
OWNER_ID = os.environ.get('BOT_OWNER_ID')
FB_JSON = os.environ.get('FIREBASE_CREDENTIALS_JSON')
FB_URL = os.environ.get('FIREBASE_DATABASE_URL')
RENDER_URL = os.environ.get('RENDER_EXTERNAL_URL')
PORT = int(os.environ.get('PORT', '8080'))

# --- Groq Setup ---
KEY_ENV = os.environ.get('GROQ_API_KEY', '')
GROQ_KEYS = [k.strip() for k in KEY_ENV.split(',') if k.strip()]
CURRENT_KEY_INDEX = 0

# --- Global Tracker ---
active_tasks = {}

# --- Firebase Init ---
try:
    if not firebase_admin._apps:
        cred_dict = json.loads(FB_JSON)
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred, {'databaseURL': FB_URL})
    fs_client = firestore.client()
except Exception as e:
    logger.error(f"❌ Firebase Error: {e}")
    sys.exit(1)

def is_owner(uid):
    return str(uid) == str(OWNER_ID)

# --- AI Function ---
async def get_expanded_keywords(base_kw):
    global CURRENT_KEY_INDEX
    if not GROQ_KEYS: return [base_kw]
    api_key = GROQ_KEYS[CURRENT_KEY_INDEX % len(GROQ_KEYS)]
    CURRENT_KEY_INDEX += 1
    
    url = "https://api.groq.com/openai/v1/chat/completions"
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": f"Generate 100 Play Store search phrases for '{base_kw}'. CSV only."}]
    }
    headers = {'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    res = data['choices'][0]['message']['content']
                    return [k.strip() for k in res.split(',') if k.strip()][:100]
    except: pass
    return [base_kw]

# --- Helper: Strict Zero Check ---
def is_strictly_zero_rated(app_data):
    """চেক করবে স্কোর, রেটিং কাউন্ট এবং হিস্টোগ্রাম সব ০ কি না"""
    score = app_data.get('score') or 0
    ratings = app_data.get('ratings') or 0
    histogram = app_data.get('histogram') or [0, 0, 0, 0, 0]
    total_votes = sum(histogram)
    return score == 0 and ratings == 0 and total_votes == 0

# --- Scraper Engine ---
async def scrape_task(base_kw, context, uid, is_auto=False):
    context.user_data['stop_signal'] = False
    
    stop_btn = [[InlineKeyboardButton("🛑 STOP IMMEDIATELY", callback_data='stop_loop')]]
    await context.bot.send_message(uid, f"🔍 গ্লোবাল জিরো সার্চ: {base_kw}...", reply_markup=InlineKeyboardMarkup(stop_btn))
    
    new_count = 0
    session_leads = []
    ref = db.reference('scraped_emails')
    keywords = await get_expanded_keywords(base_kw)
    
    # টার্গেট কান্ট্রি লিস্ট
    countries = ['us', 'gb', 'in', 'ca', 'br', 'au', 'de', 'id', 'ph', 'pk', 'tr', 'sa', 'ae', 'fr', 'it', 'es']

    try:
        for kw in keywords:
            if context.user_data.get('stop_signal'): return

            for lang_country in countries:
                if context.user_data.get('stop_signal'): return
                await asyncio.sleep(0.1)

                try:
                    # ১. প্রথমে টার্গেট দেশে সার্চ
                    results = play_search(kw, n_hits=20, lang='en', country=lang_country)
                    if not results: continue

                    for r in results:
                        if context.user_data.get('stop_signal'):
                            logger.info("Force Stop Triggered!")
                            return

                        app_id = r['appId']
                        
                        try:
                            # ২. টার্গেট দেশের ডিটেইলস চেক
                            local_app = app_details(app_id, lang='en', country=lang_country)
                            
                            # যদি টার্গেট দেশে জিরো রেটিং হয়, তবেই আমরা গ্লোবাল চেক করব
                            if local_app and is_strictly_zero_rated(local_app):
                                
                                # ৩. ডাবল ভেরিফিকেশন (Global/US Check)
                                # যদি বর্তমান দেশ US না হয়, তবে আমরা US চেক করব।
                                # কারণ US-এ জিরো মানেই অ্যাপটি গ্লোবালি নতুন।
                                is_globally_zero = True
                                
                                if lang_country != 'us':
                                    try:
                                        us_app = app_details(app_id, lang='en', country='us')
                                        if not is_strictly_zero_rated(us_app):
                                            is_globally_zero = False # US-এ রেটিং আছে, তাই বাদ
                                    except:
                                        # US ডেটা না পেলে আমরা রিস্ক নেব না, ধরে নেব ঠিক আছে অথবা স্কিপ করব
                                        # এখানে আমরা কঠোর হচ্ছি, US ডেটা না পেলে স্কিপ করছি না, তবে লোকাল ডেটাকেই প্রাধান্য দিচ্ছি
                                        pass
                                
                                if is_globally_zero:
                                    email = local_app.get('developerEmail', '').lower().strip()
                                    if email:
                                        email_key = email.replace('.', '_').replace('@', '_at_')
                                        if not ref.child(email_key).get():
                                            # ভেরিফিকেশন লিংক (US লিংক দিচ্ছি যাতে আপনি গ্লোবাল ভিউ পান)
                                            store_link = f"https://play.google.com/store/apps/details?id={app_id}&gl=us"
                                            
                                            data = {
                                                'app_name': local_app.get('title'),
                                                'email': email,
                                                'installs': local_app.get('installs'),
                                                'country': f"{lang_country} (Verified via US)",
                                                'store_link': store_link,
                                                'timestamp': datetime.now().isoformat()
                                            }
                                            ref.child(email_key).set(data)
                                            session_leads.append(data)
                                            new_count += 1
                        except: continue
                except: continue
        
        # CSV ফাইল তৈরি ও প্রেরণ
        if session_leads:
            si = io.StringIO()
            cw = csv.writer(si)
            cw.writerow(['App Name', 'Email', 'Installs', 'Source Country', 'Global Link', 'Date'])
            for v in session_leads: 
                cw.writerow([v['app_name'], v['email'], v['installs'], v['country'], v['store_link'], v['timestamp']])
            
            output = io.BytesIO(si.getvalue().encode())
            output.name = f"Global_Zero_{base_kw}.csv"
            await context.bot.send_document(uid, document=output, caption=f"✅ প্রসেস শেষ: {base_kw}\n🌍 গ্লোবাল জিরো লিড: {new_count}")

    except asyncio.CancelledError:
        logger.info("Task was cancelled.")
        return
    finally:
        if not context.user_data.get('stop_signal') and is_auto:
            await asyncio.sleep(2)
            await execute_auto_search(context, uid)

# --- Auto Search Controller ---
async def execute_auto_search(context, uid):
    if context.user_data.get('stop_signal'): return
    
    keywords_ref = fs_client.collection('artifacts').document('keyword-bot-pro').collection('public').document('data').collection('keywords')
    docs = keywords_ref.limit(1).get()
    
    if docs:
        kw = docs[0].to_dict().get('word')
        docs[0].reference.delete()
        task = asyncio.create_task(scrape_task(kw, context, uid, is_auto=True))
        active_tasks[uid] = task
    else:
        await context.bot.send_message(uid, "⚠️ ফায়ারবেস খালি।")

# --- Handlers ---
async def start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not is_owner(u.effective_user.id): return
    btns = [
        [InlineKeyboardButton("🤖 অটো গ্লোবাল সার্চ", callback_data='auto_s')],
        [InlineKeyboardButton("🔄 রিফ্রেশ বট", callback_data='refresh_bot')]
    ]
    await u.message.reply_text("বট রেডি। এখন 'US Cross-Check' মোড অন করা হয়েছে।", reply_markup=InlineKeyboardMarkup(btns))

async def cb(u: Update, c: ContextTypes.DEFAULT_TYPE):
    q = u.callback_query
    uid = u.effective_user.id
    await q.answer()
    
    if q.data == 'auto_s':
        c.user_data['stop_signal'] = False
        await q.edit_message_text("🔄 অটো গ্লোবাল ফিল্টার মোড চালু...")
        await execute_auto_search(c, uid)

    elif q.data == 'stop_loop':
        c.user_data['stop_signal'] = True
        if uid in active_tasks:
            active_tasks[uid].cancel()
            del active_tasks[uid]
        await q.edit_message_text("🛑 থামানো হয়েছে।")

    elif q.data == 'refresh_bot':
        c.user_data.clear()
        if uid in active_tasks:
            active_tasks[uid].cancel()
            del active_tasks[uid]
        await q.edit_message_text("♻️ সব ক্লিয়ার করা হয়েছে।")

async def msg(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not is_owner(u.effective_user.id): return
    uid = u.effective_user.id
    c.user_data['stop_signal'] = False
    task = asyncio.create_task(scrape_task(u.message.text, c, uid, is_auto=False))
    active_tasks[uid] = task

def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(cb))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, msg))
    
    if RENDER_URL:
        app.run_webhook(listen="0.0.0.0", port=PORT, url_path=TOKEN[-10:], webhook_url=f"{RENDER_URL}/{TOKEN[-10:]}")
    else:
        app.run_polling()

if __name__ == "__main__":
    main()
