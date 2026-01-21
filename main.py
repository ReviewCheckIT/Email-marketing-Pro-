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

# --- Groq Keys Setup (Updated from Gemini) ---
KEY_ENV = os.environ.get('GROQ_API_KEY', '') # এনভায়রনমেন্টে GROQ_API_KEY নামে কি সেভ করবেন
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

# --- AI Helper Functions (Updated to Groq) ---
def get_next_api_key():
    global CURRENT_KEY_INDEX
    if not GROQ_KEYS: return None
    key = GROQ_KEYS[CURRENT_KEY_INDEX % len(GROQ_KEYS)]
    CURRENT_KEY_INDEX += 1
    return key

async def get_expanded_keywords(base_kw):
    """
    AI আপডেট: এখন Groq LPU ব্যবহার করে অবিশ্বাস্য দ্রুত গতিতে কিওয়ার্ড জেনারেট হবে।
    প্রথমে llama-3.3-70b -> ব্যর্থ হলে llama3-8b -> ব্যর্থ হলে mixtral-8x7b
    """
    if not GROQ_KEYS:
        logger.warning("⚠️ No Groq Keys found!")
        return [base_kw]

    # Groq মডেলের তালিকা
    models_to_try = ["llama-3.3-70b-versatile", "llama3-8b-8192", "mixtral-8x7b-32768"]
    
    for i in range(len(GROQ_KEYS)):
        api_key = get_next_api_key()
        if not api_key: break

        for model in models_to_try:
            url = "https://api.groq.com/openai/v1/chat/completions"
            
            prompt = f"Generate 100 unique, broad, and popular search phrases for Google Play Store to find new and unrated apps related to '{base_kw}'. Focus on terms that return maximum results. Provide only comma-separated values."
            
            payload = {
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.7
            }
            headers = {
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json'
            }

            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, headers=headers, json=payload) as response:
                        if response.status == 200:
                            res_json = await response.json()
                            try:
                                text_data = res_json['choices'][0]['message']['content']
                                kws = [k.strip() for k in text_data.split(',') if k.strip()]
                                final_list = list(set([base_kw] + kws))[:100]
                                logger.info(f"✅ Groq Success with Model: {model}")
                                return final_list
                            except Exception:
                                continue
                        elif response.status == 429:
                            logger.warning(f"⚠️ Groq Rate Limited on {model}. Switching key...")
                            break 
                        else:
                            continue 
            except Exception as e:
                logger.error(f"Groq Connection Error on {model}: {e}")
                continue

    logger.error("❌ All Groq attempts failed. Using base keyword.")
    return [base_kw]

# --- Helper: Fetch Keyword & Trigger Search ---
async def execute_auto_search(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    if context.user_data.get('stop_signal'):
        context.user_data['auto_loop'] = False
        context.user_data['stop_signal'] = False
        await context.bot.send_message(chat_id=chat_id, text="🛑 অটো সার্চ পুরোপুরি থামানো হয়েছে।")
        return

    try:
        keywords_ref = fs_client.collection('artifacts').document(FIRESTORE_APP_ID)\
            .collection('public').document('data').collection('keywords')
        
        docs = keywords_ref.limit(1).get()
        
        if docs:
            doc = docs[0]
            data = doc.to_dict()
            keyword = data.get('word')
            doc.reference.delete()
            
            context.user_data['from_cloud'] = True
            await scrape_task(keyword, context, chat_id)
        else:
            context.user_data['auto_loop'] = False 
            await context.bot.send_message(chat_id=chat_id, text="⚠️ ফায়ারবেসে আর কোনো কিওয়ার্ড নেই। অটো সার্চ সমাপ্ত।")
            
    except Exception as e:
        logger.error(f"Firestore Fetch Error: {e}")
        context.user_data['auto_loop'] = False
        await context.bot.send_message(chat_id=chat_id, text=f"⚠️ এরর: {e}")

# --- Global Scraper Engine ---
async def scrape_task(base_kw, context, uid):
    context.user_data['stop_signal'] = False
    
    keywords = await get_expanded_keywords(base_kw)
    countries = ['us', 'gb', 'in', 'ca', 'br', 'au', 'de', 'id', 'ph', 'pk', 'za', 'mx', 'tr', 'sa', 'ae', 'ru', 'fr', 'it', 'es', 'nl'] 
    
    stop_btn = [[InlineKeyboardButton("🛑 Stop Auto Search", callback_data='stop_loop')]]
    
    msg_text = f"🌍 **মেগা সার্চ শুরু (Groq AI)!** \n🔍 নিস: {base_kw}\n🎯 কিওয়ার্ড: {len(keywords)}টি"
    if context.user_data.get('from_cloud'): msg_text += "\n(Cloud Keyword)"
    
    status_msg = await context.bot.send_message(uid, msg_text, reply_markup=InlineKeyboardMarkup(stop_btn))
    
    new_count = 0
    session_leads = []
    ref = db.reference('scraped_emails')
    processed_apps = set()

    for kw in keywords:
        if context.user_data.get('stop_signal'): break

        for lang_country in countries:
            if context.user_data.get('stop_signal'): break

            try:
                results = play_search(kw, n_hits=250, lang='en', country=lang_country) 
                if not results: continue

                for r in results:
                    if context.user_data.get('stop_signal'): break

                    app_id = r['appId']
                    if app_id in processed_apps: continue
                    processed_apps.add(app_id)

                    try:
                        app = app_details(app_id, lang='en', country=lang_country)
                        if app and app.get('developerEmail'):
                            email_raw = app['developerEmail'].lower().strip()
                            score = app.get('score', 0)
                            reviews = app.get('reviews', 0)

                            if (score == 0 or score is None) and (reviews == 0 or reviews is None):
                                email_key = email_raw.replace('.', '_').replace('@', '_at_')
                                
                                if not ref.child(email_key).get():
                                    data = {
                                        'app_name': app.get('title'),
                                        'email': email_raw,
                                        'rating': 0,
                                        'reviews': 0,
                                        'installs': app.get('installs'),
                                        'country': lang_country,
                                        'dev': app.get('developer'),
                                        'timestamp': datetime.now().isoformat()
                                    }
                                    ref.child(email_key).set(data)
                                    session_leads.append(data)
                                    new_count += 1
                    except: continue
                await asyncio.sleep(1) 
            except: continue
    
    if context.user_data.get('stop_signal'):
        await context.bot.send_message(uid, f"🛑 সার্চ মাঝপথে থামানো হয়েছে।\nসংগৃহীত লিড: {new_count}টি")
    else:
        if session_leads:
            si = io.StringIO()
            cw = csv.writer(si)
            cw.writerow(['App Name', 'Email', 'Rating', 'Reviews', 'Installs', 'Country', 'Developer', 'Date'])
            for v in session_leads:
                cw.writerow([v.get('app_name'), v.get('email'), 0, 0, v.get('installs'), v.get('country'), v.get('dev'), v.get('timestamp')])
            
            output = io.BytesIO(si.getvalue().encode())
            output.name = f"Leads_{base_kw}_{datetime.now().strftime('%d_%m')}.csv"
            await context.bot.send_document(chat_id=uid, document=output, caption=f"✅ কাজ শেষ: '{base_kw}'\n🔥 নতুন লিড: {new_count}টি।")
        else:
            await context.bot.send_message(uid, f"❌ '{base_kw}' দিয়ে কোনো নতুন লিড পাওয়া যায়নি।")

    if not context.user_data.get('stop_signal') and context.user_data.get('auto_loop'):
        await asyncio.sleep(5) 
        await context.bot.send_message(uid, "🔄 পরবর্তী কিওয়ার্ড লোড করা হচ্ছে...")
        await execute_auto_search(context, uid)

# --- Handlers ---
async def start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not is_owner(u.effective_user.id): return
    btn = [[InlineKeyboardButton("🤖 অটো কিওয়ার্ড সার্চ (Firebase Loop)", callback_data='auto_s')]]
    await u.message.reply_text("বট অনলাইন (Groq AI Enabled)! আমি প্রস্তুত।", reply_markup=InlineKeyboardMarkup(btn))

async def stats(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not is_owner(u.effective_user.id): return
    data = db.reference('scraped_emails').get()
    count = len(data) if data else 0
    await u.message.reply_text(f"📊 ডাটাবেজে মোট লিড সংখ্যা: {count}")

async def export(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not is_owner(u.effective_user.id): return
    data = db.reference('scraped_emails').get()
    if not data:
        await u.message.reply_text("কোনো ডেটা নেই!")
        return

    si = io.StringIO()
    cw = csv.writer(si)
    cw.writerow(['App Name', 'Email', 'Rating', 'Reviews', 'Installs', 'Country', 'Developer', 'Date'])
    for k, v in data.items():
        cw.writerow([v.get('app_name'), v.get('email'), 0, 0, v.get('installs'), v.get('country'), v.get('dev'), v.get('timestamp')])
    
    output = io.BytesIO(si.getvalue().encode())
    output.name = f"Global_Database_Export_{datetime.now().strftime('%d_%m')}.csv"
    await u.message.reply_document(document=output, caption="✅ ডাটাবেজের সব লিড লিস্ট।")

async def clear_db(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not is_owner(u.effective_user.id): return
    db.reference('scraped_emails').delete()
    await u.message.reply_text("🗑️ সব ডেটা ডিলিট করা হয়েছে।")

async def cb(u: Update, c: ContextTypes.DEFAULT_TYPE):
    q = u.callback_query
    if not is_owner(q.from_user.id): return
    await q.answer()
    
    if q.data == 'auto_s':
        c.user_data['auto_loop'] = True
        c.user_data['stop_signal'] = False
        await q.edit_message_text("🔄 অটোমেটিক লুপ মোড চালু হয়েছে। ফায়ারবেস চেক করা হচ্ছে...")
        await execute_auto_search(c, u.effective_chat.id)

    elif q.data == 'stop_loop':
        c.user_data['stop_signal'] = True 
        c.user_data['auto_loop'] = False
        await q.edit_message_text("🛑 থামার নির্দেশ পাঠানো হয়েছে... এখনই থেমে যাবে।")

async def msg(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not is_owner(u.effective_user.id): return
    keyword = u.message.text
    c.user_data['auto_loop'] = False 
    c.user_data['stop_signal'] = False
    c.user_data['from_cloud'] = False
    asyncio.create_task(scrape_task(keyword, c, u.effective_user.id))
    await u.message.reply_text(f"🔍 ম্যানুয়াল ইনপুট '{keyword}' গ্রহণ করা হয়েছে। সার্চ চলছে...")

def main():
    if not TOKEN: return
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("export", export))
    app.add_handler(CommandHandler("clear", clear_db))
    app.add_handler(CallbackQueryHandler(cb))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, msg))

    if RENDER_URL:
        app.run_webhook(listen="0.0.0.0", port=PORT, url_path=TOKEN[-10:], 
                        webhook_url=f"{RENDER_URL}/{TOKEN[-10:]}")
    else:
        app.run_polling()

if __name__ == "__main__":
    main()
