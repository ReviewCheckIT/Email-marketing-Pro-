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

# --- Groq Keys Setup ---
KEY_ENV = os.environ.get('GROQ_API_KEY', '')
GROQ_KEYS = [k.strip() for k in KEY_ENV.split(',') if k.strip()]
CURRENT_KEY_INDEX = 0

FIRESTORE_APP_ID = 'keyword-bot-pro'

# --- Firebase Init ---
fs_client = None
try:
    if not firebase_admin._apps:
        cred_dict = json.loads(FB_JSON)
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred, {'databaseURL': FB_URL})
    fs_client = firestore.client()
    logger.info("🔥 Firebase Connected!")
except Exception as e:
    logger.error(f"❌ Firebase Error: {e}")
    sys.exit(1)

def is_owner(uid):
    return str(uid) == str(OWNER_ID)

# --- Groq AI Helper ---
def get_next_api_key():
    global CURRENT_KEY_INDEX
    if not GROQ_KEYS: return None
    key = GROQ_KEYS[CURRENT_KEY_INDEX % len(GROQ_KEYS)]
    CURRENT_KEY_INDEX += 1
    return key

async def get_expanded_keywords(base_kw):
    if not GROQ_KEYS: return [base_kw]
    models_to_try = ["llama-3.3-70b-versatile", "llama3-8b-8192", "mixtral-8x7b-32768"]
    
    for i in range(len(GROQ_KEYS)):
        api_key = get_next_api_key()
        if not api_key: break
        for model in models_to_try:
            url = "https://api.groq.com/openai/v1/chat/completions"
            prompt = f"Generate 100 unique search phrases for Play Store related to '{base_kw}'. CSV only."
            payload = {"model": model, "messages": [{"role": "user", "content": prompt}], "temperature": 0.7}
            headers = {'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'}
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, headers=headers, json=payload, timeout=10) as response:
                        if response.status == 200:
                            res_json = await response.json()
                            text_data = res_json['choices'][0]['message']['content']
                            kws = [k.strip() for k in text_data.split(',') if k.strip()]
                            return list(set([base_kw] + kws))[:100]
            except: continue
    return [base_kw]

# --- শক্তিশালী স্টপ চেক ফাংশন ---
def check_stop(context):
    return context.user_data.get('stop_signal', False)

# --- Auto Search Entry ---
async def execute_auto_search(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    if check_stop(context):
        context.user_data['auto_loop'] = False
        await context.bot.send_message(chat_id=chat_id, text="🛑 অটো সার্চ থামানো হয়েছে।")
        return

    try:
        keywords_ref = fs_client.collection('artifacts').document(FIRESTORE_APP_ID)\
            .collection('public').document('data').collection('keywords')
        docs = keywords_ref.limit(1).get()
        
        if docs:
            doc = docs[0]
            keyword = doc.to_dict().get('word')
            doc.reference.delete()
            context.user_data['from_cloud'] = True
            await scrape_task(keyword, context, chat_id)
        else:
            context.user_data['auto_loop'] = False 
            await context.bot.send_message(chat_id=chat_id, text="⚠️ ফায়ারবেস খালি।")
    except Exception as e:
        logger.error(f"Error: {e}")
        context.user_data['auto_loop'] = False

# --- Main Engine (Strong Intervention) ---
async def scrape_task(base_kw, context, uid):
    context.user_data['stop_signal'] = False
    keywords = await get_expanded_keywords(base_kw)
    countries = ['us', 'gb', 'in', 'ca', 'br', 'au', 'de', 'id', 'ph', 'pk', 'za', 'mx', 'tr', 'sa', 'ae', 'ru', 'fr', 'it', 'es', 'nl'] 
    
    stop_btn = [[InlineKeyboardButton("🛑 STOP IMMEDIATELY", callback_data='stop_loop')]]
    status_msg = await context.bot.send_message(uid, f"🚀 সার্চ শুরু: {base_kw}\n🎯 কিওয়ার্ড: {len(keywords)}", reply_markup=InlineKeyboardMarkup(stop_btn))
    
    new_count = 0
    session_leads = []
    ref = db.reference('scraped_emails')
    processed_apps = set()

    try:
        for kw in keywords:
            # লেভেল ১ স্টপ চেক (কিওয়ার্ড পরিবর্তনকালে)
            if check_stop(context): break
            
            for lang_country in countries:
                # লেভেল ২ স্টপ চেক (কান্ট্রি পরিবর্তনকালে)
                if check_stop(context): break
                
                try:
                    # n_hits কমিয়ে ২৫ করা হয়েছে দ্রুত রেসপন্সের জন্য (বড় লুপ হলে বাটন কাজ করতে দেরি করে)
                    results = play_search(kw, n_hits=25, lang='en', country=lang_country) 
                    if not results: continue

                    for r in results:
                        # লেভেল ৩ স্টপ চেক (প্রতিটি অ্যাপ প্রসেসিংকালে - সবচাইতে পাওয়ারফুল)
                        if check_stop(context): break
                        
                        await asyncio.sleep(0) # ইভেন্ট লুপকে বাটন ক্লিকের সুযোগ দেয়
                        
                        app_id = r['appId']
                        if app_id in processed_apps: continue
                        processed_apps.add(app_id)

                        try:
                            app = app_details(app_id, lang='en', country=lang_country)
                            if app and app.get('developerEmail'):
                                email_raw = app['developerEmail'].lower().strip()
                                if (app.get('score', 0) or 0) == 0:
                                    email_key = email_raw.replace('.', '_').replace('@', '_at_')
                                    if not ref.child(email_key).get():
                                        data = {
                                            'app_name': app.get('title'),
                                            'email': email_raw,
                                            'installs': app.get('installs'),
                                            'country': lang_country,
                                            'timestamp': datetime.now().isoformat()
                                        }
                                        ref.child(email_key).set(data)
                                        session_leads.append(data)
                                        new_count += 1
                        except: continue
                except: continue
    except Exception as e:
        logger.error(f"Task Error: {e}")

    # ফাইনাল মেসেজ
    if check_stop(context):
        await context.bot.send_message(uid, f"🛑 কাজ জোরপূর্বক থামানো হয়েছে!\nনতুন লিড: {new_count}টি।")
    else:
        if session_leads:
            si = io.StringIO()
            cw = csv.writer(si)
            cw.writerow(['App Name', 'Email', 'Installs', 'Country', 'Date'])
            for v in session_leads:
                cw.writerow([v['app_name'], v['email'], v['installs'], v['country'], v['timestamp']])
            output = io.BytesIO(si.getvalue().encode())
            output.name = f"Leads_{base_kw}.csv"
            await context.bot.send_document(chat_id=uid, document=output, caption=f"✅ সম্পূর্ণ হয়েছে: {base_kw}\n🔥 লিড: {new_count}")
        else:
            await context.bot.send_message(uid, f"❌ কোনো লিড পাওয়া যায়নি: {base_kw}")

    # অটো লুপ হ্যান্ডলিং
    if not check_stop(context) and context.user_data.get('auto_loop'):
        await asyncio.sleep(2)
        await execute_auto_search(context, uid)

# --- Handlers ---
async def start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not is_owner(u.effective_user.id): return
    btn = [[InlineKeyboardButton("🤖 অটো সার্চ শুরু করুন", callback_data='auto_s')]]
    await u.message.reply_text("বট অনলাইন। Groq AI এবং Fast-Stop সক্রিয়।", reply_markup=InlineKeyboardMarkup(btn))

async def cb(u: Update, c: ContextTypes.DEFAULT_TYPE):
    q = u.callback_query
    if not is_owner(q.from_user.id): return
    await q.answer()
    
    if q.data == 'auto_s':
        c.user_data['auto_loop'] = True
        c.user_data['stop_signal'] = False
        await q.edit_message_text("🔄 অটো সার্চ মোড সক্রিয়...")
        await execute_auto_search(c, u.effective_chat.id)

    elif q.data == 'stop_loop':
        # এখানে ফ্ল্যাগটিকে True করা হয় যা scrape_task এর ভেতরের ৩টি লেভেলে চেক হচ্ছে
        c.user_data['stop_signal'] = True 
        c.user_data['auto_loop'] = False
        await q.edit_message_text("🛑 থামার নির্দেশ প্রসেস হচ্ছে... (খুব দ্রুতই থেমে যাবে)")

async def stats(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not is_owner(u.effective_user.id): return
    data = db.reference('scraped_emails').get()
    await u.message.reply_text(f"📊 মোট লিড: {len(data) if data else 0}")

async def export(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not is_owner(u.effective_user.id): return
    data = db.reference('scraped_emails').get()
    if not data: return
    si = io.StringIO()
    cw = csv.writer(si)
    cw.writerow(['App Name', 'Email', 'Installs', 'Country', 'Date'])
    for k, v in data.items():
        cw.writerow([v.get('app_name'), v.get('email'), v.get('installs'), v.get('country'), v.get('timestamp')])
    output = io.BytesIO(si.getvalue().encode())
    output.name = "Database_Export.csv"
    await u.message.reply_document(document=output)

async def msg(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not is_owner(u.effective_user.id): return
    keyword = u.message.text
    c.user_data['auto_loop'] = False 
    c.user_data['stop_signal'] = False
    asyncio.create_task(scrape_task(keyword, c, u.effective_user.id))
    await u.message.reply_text(f"🔍 সার্চ চলছে: {keyword}")

def main():
    if not TOKEN: return
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("export", export))
    app.add_handler(CallbackQueryHandler(cb))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, msg))
    
    if RENDER_URL:
        app.run_webhook(listen="0.0.0.0", port=PORT, url_path=TOKEN[-10:], 
                        webhook_url=f"{RENDER_URL}/{TOKEN[-10:]}")
    else:
        app.run_polling()

if __name__ == "__main__":
    main()
