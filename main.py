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

# --- Gemini Keys Setup ---
KEY_ENV = os.environ.get('GEMINI_API_KEY', '')
GEMINI_KEYS = [k.strip() for k in KEY_ENV.split(',') if k.strip()]
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

# --- AI Helper Functions (Smart Model Fallback) ---
def get_next_api_key():
    global CURRENT_KEY_INDEX
    if not GEMINI_KEYS: return None
    key = GEMINI_KEYS[CURRENT_KEY_INDEX % len(GEMINI_KEYS)]
    CURRENT_KEY_INDEX += 1
    return key

async def get_expanded_keywords(base_kw):
    """
    AI ফিক্স: এটি এখন একাধিক মডেল ট্রাই করবে।
    প্রথমে 2.0-flash -> ব্যর্থ হলে 1.5-flash -> ব্যর্থ হলে 1.5-pro
    """
    if not GEMINI_KEYS:
        logger.warning("⚠️ No Gemini Keys found!")
        return [base_kw]

    # মডেলের তালিকা (অগ্রাধিকার অনুযায়ী)
    models_to_try = ["gemini-2.0-flash", "gemini-1.5-flash", "gemini-1.5-pro"]
    
    # কী লুপ
    for i in range(len(GEMINI_KEYS)):
        api_key = get_next_api_key()
        if not api_key: break

        # মডেল লুপ (প্রতিটি কী দিয়ে সব মডেল ট্রাই করবে)
        for model in models_to_try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
            
            prompt = f"Generate 100 unique, broad, and popular search phrases for Google Play Store to find new and unrated apps related to '{base_kw}'. Focus on terms that return maximum results. Provide only comma-separated values."
            
            payload = {"contents": [{"parts": [{"text": prompt}]}]}
            headers = {'Content-Type': 'application/json'}

            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, headers=headers, json=payload) as response:
                        if response.status == 200:
                            res_json = await response.json()
                            try:
                                text_data = res_json['candidates'][0]['content']['parts'][0]['text']
                                kws = [k.strip() for k in text_data.split(',') if k.strip()]
                                final_list = list(set([base_kw] + kws))[:100]
                                logger.info(f"✅ Success with Model: {model}")
                                return final_list
                            except Exception:
                                continue # পার্স এরর হলে পরের মডেল দেখবে
                        elif response.status == 429:
                            logger.warning(f"⚠️ Key Rate Limited on {model}. Switching key...")
                            break # এই কী দিয়ে আর লাভ নেই, লুপ ব্রেক করে পরের কী তে যাবে
                        else:
                            # 404 বা অন্য এরর হলে পরের মডেল দেখবে
                            continue 
            except Exception as e:
                logger.error(f"Connection Error on {model}: {e}")
                continue

    logger.error("❌ All AI attempts failed. Using base keyword.")
    return [base_kw]

# --- Helper: Fetch Keyword & Trigger Search ---
async def execute_auto_search(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    # শুরুতেই চেক
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

# --- Global Scraper Engine (Optimized for Immediate Stop) ---
async def scrape_task(base_kw, context, uid):
    # স্টপ সিগন্যাল রিসেট
    context.user_data['stop_signal'] = False
    
    keywords = await get_expanded_keywords(base_kw)
    countries = ['us', 'gb', 'in', 'ca', 'br', 'au', 'de', 'id', 'ph', 'pk', 'za', 'mx', 'tr', 'sa', 'ae', 'ru', 'fr', 'it', 'es', 'nl'] 
    
    # বাটন সেটআপ
    stop_btn = [[InlineKeyboardButton("🛑 Stop Auto Search", callback_data='stop_loop')]]
    
    msg_text = f"🌍 **মেগা সার্চ শুরু!** \n🔍 নিস: {base_kw}\n🎯 কিওয়ার্ড: {len(keywords)}টি\n(Cloud Keyword)" if context.user_data.get('from_cloud') else f"🌍 **মেগা সার্চ শুরু!** \n🔍 নিস: {base_kw}\n🎯 কিওয়ার্ড: {len(keywords)}টি"
    
    status_msg = await context.bot.send_message(uid, msg_text, reply_markup=InlineKeyboardMarkup(stop_btn))
    
    new_count = 0
    session_leads = []
    ref = db.reference('scraped_emails')
    processed_apps = set()

    # মেইন লুপ
    for kw in keywords:
        # 1. কিওয়ার্ড লুপের শুরুতে স্টপ চেক
        if context.user_data.get('stop_signal'): break

        for lang_country in countries:
            # 2. কান্ট্রি লুপের শুরুতে স্টপ চেক (আরও ফাস্ট রেসপন্সের জন্য)
            if context.user_data.get('stop_signal'): break

            try:
                results = play_search(kw, n_hits=250, lang='en', country=lang_country) 
                if not results: continue

                for r in results:
                    # 3. প্রতিটি অ্যাপ প্রসেসিংয়ের আগে স্টপ চেক (তাৎক্ষণিক থামার জন্য)
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
                
                # প্রগ্রেস আপডেট
                if new_count > 0 and new_count % 30 == 0:
                    # লগের বদলে টেলিগ্রামে এডিট করলে ইউজার বুঝতে পারবে কাজ চলছে
                    pass 
                
                await asyncio.sleep(1) 
            except: continue
    
    # লুপ শেষ বা ব্রেক হওয়ার পর
    if context.user_data.get('stop_signal'):
        await context.bot.send_message(uid, f"🛑 সার্চ মাঝপথে থামানো হয়েছে।\nসংগৃহীত লিড: {new_count}টি")
    else:
        # স্বাভাবিক সমাপ্তি
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

    # --- অটোমেটিক লুপ লজিক ---
    # যদি স্টপ সিগন্যাল না থাকে এবং অটো লুপ অন থাকে, তবেই কন্টিনিউ করবে
    if not context.user_data.get('stop_signal') and context.user_data.get('auto_loop'):
        await asyncio.sleep(5) 
        await context.bot.send_message(uid, "🔄 পরবর্তী কিওয়ার্ড লোড করা হচ্ছে...")
        await execute_auto_search(context, uid)

# --- Handlers ---
async def start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not is_owner(u.effective_user.id): return
    btn = [[InlineKeyboardButton("🤖 অটো কিওয়ার্ড সার্চ (Firebase Loop)", callback_data='auto_s')]]
    await u.message.reply_text("বট অনলাইন! আমি প্রস্তুত।", reply_markup=InlineKeyboardMarkup(btn))

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

# --- Callback Handler (Stop Signal Fix) ---
async def cb(u: Update, c: ContextTypes.DEFAULT_TYPE):
    q = u.callback_query
    if not is_owner(q.from_user.id): return
    await q.answer()
    
    if q.data == 'auto_s':
        c.user_data['auto_loop'] = True
        c.user_data['stop_signal'] = False # রিসেট
        await q.edit_message_text("🔄 অটোমেটিক লুপ মোড চালু হয়েছে। ফায়ারবেস চেক করা হচ্ছে...")
        await execute_auto_search(c, u.effective_chat.id)

    elif q.data == 'stop_loop':
        # এখানে ফ্ল্যাগ সেট করা হলো যা লুপের ভেতরে চেক হবে
        c.user_data['stop_signal'] = True 
        c.user_data['auto_loop'] = False
        await q.edit_message_text("🛑 থামার নির্দেশ পাঠানো হয়েছে... এখনই থেমে যাবে।")

async def msg(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not is_owner(u.effective_user.id): return
    
    if c.user_data.get('state') == 'kw':
        c.user_data['state'] = None
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
