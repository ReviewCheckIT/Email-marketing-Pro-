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

# --- Global Tracker for Tasks ---
# এই ডিকশনারিটি চলমান টাস্কগুলোকে ট্র্যাক করবে যাতে সাথে সাথে ক্যান্সেল করা যায়
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

# --- Scraper Engine with Force Stop ---
async def scrape_task(base_kw, context, uid, is_auto=False):
    # স্টপ ফ্ল্যাগ রিসেট
    context.user_data['stop_signal'] = False
    
    stop_btn = [[InlineKeyboardButton("🛑 STOP IMMEDIATELY", callback_data='stop_loop')]]
    status_msg = await context.bot.send_message(uid, f"🔍 সার্চিং: {base_kw}...", reply_markup=InlineKeyboardMarkup(stop_btn))
    
    new_count = 0
    session_leads = []
    ref = db.reference('scraped_emails')
    keywords = await get_expanded_keywords(base_kw)
    countries = ['us', 'gb', 'in', 'ca', 'br', 'au', 'de', 'id', 'ph', 'pk', 'za', 'mx', 'tr', 'sa', 'ae', 'ru', 'fr', 'it', 'es', 'nl']

    try:
        for kw in keywords:
            # চেক ১: কাজ শুরুতেই থামানো হয়েছে কি না
            if context.user_data.get('stop_signal'): return

            for lang_country in countries:
                # চেক ২: কান্ট্রি পরিবর্তনের সময়
                if context.user_data.get('stop_signal'): return
                
                # লুপকে শ্বাস নেওয়ার সুযোগ দেওয়া (যাতে বাটন ক্লিক প্রসেস হতে পারে)
                await asyncio.sleep(0.1)

                try:
                    # ছোট ব্যাচে সার্চ করা যাতে দ্রুত ইন্টারাপ্ট করা যায়
                    results = play_search(kw, n_hits=20, lang='en', country=lang_country)
                    if not results: continue

                    for r in results:
                        # চেক ৩: প্রতিটি অ্যাপ প্রসেস করার আগে (সবচাইতে শক্তিশালী চেক)
                        if context.user_data.get('stop_signal'):
                            logger.info("Force Stop Triggered!")
                            return # ফাংশন থেকে সরাসরি বের হয়ে যাবে

                        app_id = r['appId']
                        try:
                            app = app_details(app_id, lang='en', country=lang_country)
                            if app and (app.get('score') or 0) == 0:
                                email = app.get('developerEmail', '').lower().strip()
                                if email:
                                    email_key = email.replace('.', '_').replace('@', '_at_')
                                    if not ref.child(email_key).get():
                                        data = {'app_name': app.get('title'), 'email': email, 'installs': app.get('installs'), 'country': lang_country, 'timestamp': datetime.now().isoformat()}
                                        ref.child(email_key).set(data)
                                        session_leads.append(data)
                                        new_count += 1
                        except: continue
                except: continue
        
        # সফলভাবে শেষ হলে ফাইল পাঠানো
        if session_leads:
            si = io.StringIO()
            cw = csv.writer(si)
            cw.writerow(['App Name', 'Email', 'Installs', 'Country', 'Date'])
            for v in session_leads: cw.writerow([v['app_name'], v['email'], v['installs'], v['country'], v['timestamp']])
            output = io.BytesIO(si.getvalue().encode()); output.name = f"Leads_{base_kw}.csv"
            await context.bot.send_document(uid, document=output, caption=f"✅ শেষ: {base_kw}\n🔥 লিড: {new_count}")

    except asyncio.CancelledError:
        # যদি টাস্কটি সিস্টেম থেকে ক্যান্সেল করা হয়
        logger.info("Task was cancelled.")
        return
    finally:
        # অটো লুপ হ্যান্ডলিং (যদি মাঝপথে থামানো না হয়)
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
        # টাস্ক ট্র্যাকিং শুরু
        task = asyncio.create_task(scrape_task(kw, context, uid, is_auto=True))
        active_tasks[uid] = task
    else:
        await context.bot.send_message(uid, "⚠️ ফায়ারবেস খালি।")

# --- Handlers ---
async def start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not is_owner(u.effective_user.id): return
    btns = [
        [InlineKeyboardButton("🤖 অটো সার্চ শুরু", callback_data='auto_s')],
        [InlineKeyboardButton("🔄 রিফ্রেশ/রিসেট বট", callback_data='refresh_bot')]
    ]
    await u.message.reply_text("বট প্রস্তুত। যেকোনো সমস্যা হলে রিফ্রেশ করুন।", reply_markup=InlineKeyboardMarkup(btns))

async def cb(u: Update, c: ContextTypes.DEFAULT_TYPE):
    q = u.callback_query
    uid = u.effective_user.id
    await q.answer()
    
    if q.data == 'auto_s':
        c.user_data['stop_signal'] = False
        await q.edit_message_text("🔄 অটো মোড শুরু হচ্ছে...")
        await execute_auto_search(c, uid)

    elif q.data == 'stop_loop':
        # ১. সিগন্যাল সেট করা
        c.user_data['stop_signal'] = True
        c.user_data['auto_loop'] = False
        # ২. টাস্ক ক্যান্সেল করা (জোরপূর্বক থামালে এটি দ্রুত কাজ করে)
        if uid in active_tasks:
            active_tasks[uid].cancel()
            del active_tasks[uid]
        await q.edit_message_text("🛑 কাজ বন্ধ করা হয়েছে। বট এখন ফ্রি।")

    elif q.data == 'refresh_bot':
        # রিফ্রেশ বাটন যা সবকিছু ক্লিন করবে
        c.user_data.clear()
        if uid in active_tasks:
            active_tasks[uid].cancel()
            del active_tasks[uid]
        await q.edit_message_text("♻️ বট রিফ্রেশ করা হয়েছে। আগের সব প্রসেস ডিলিট করা হয়েছে। আপনি এখন নতুন করে শুরু করতে পারেন।")

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
