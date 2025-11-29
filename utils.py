import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import os
import time
import requests
from datetime import datetime, timedelta

# ==========================================
# [CONFIG] ตั้งค่า Discord Webhook
# ==========================================
DISCORD_WEBHOOK_URL = 'https://discord.com/api/webhooks/1444236316404482139/UJc-I_NRT33p9UKCas5ATGgjAlqlrtxBuPhvKYKnI-Pz2_AyxAnOs_UFNl203_sqLsI5'

# --- Caching System ---
CACHE_DURATION = 60
cache_storage = {
    'Jobs': {'data': None, 'timestamp': 0},
    'Drivers': {'data': None, 'timestamp': 0},
    'Users': {'data': None, 'timestamp': 0}
}

# --- Helper Functions ---
def thai_date_filter(date_str):
    if not date_str: return ""
    try:
        date_obj = datetime.strptime(str(date_str).strip(), "%Y-%m-%d")
        thai_year = date_obj.year + 543
        return date_obj.strftime(f"%d/%m/{thai_year}")
    except ValueError: return date_str

def get_db():
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds_json = os.environ.get('GSPREAD_CREDENTIALS')
    
    if not creds_json:
        if os.path.exists("credentials.json"): return gspread.service_account(filename="credentials.json").open("DriverLogApp")
        else: return None
        
    creds_dict = json.loads(creds_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client.open("DriverLogApp")

def get_cached_records(sheet, worksheet_name):
    current_time = time.time()
    cache_entry = cache_storage.get(worksheet_name)
    
    if cache_entry and cache_entry['data'] is not None:
        if current_time - cache_entry['timestamp'] < CACHE_DURATION:
            return cache_entry['data']
    
    try:
        data = sheet.worksheet(worksheet_name).get_all_records()
        cache_storage[worksheet_name] = {
            'data': data,
            'timestamp': current_time
        }
        return data
    except Exception as e:
        # กรณี Error 429 (Quota) ให้ใช้ Cache เก่าถ้ามี
        if "429" in str(e) and cache_entry and cache_entry['data'] is not None:
            return cache_entry['data']
        raise e

def invalidate_cache(worksheet_name):
    if worksheet_name in cache_storage:
        cache_storage[worksheet_name] = {'data': None, 'timestamp': 0}

# --- Notification Logic ---
def send_discord_msg(message):
    """ฟังก์ชันส่งข้อความเข้า Discord"""
    try:
        if not DISCORD_WEBHOOK_URL or 'วาง_' in DISCORD_WEBHOOK_URL:
            print("Error: Discord Webhook URL is invalid")
            return

        payload = {
            "content": message,
            "username": "LMT Transport Bot",
            "avatar_url": "https://cdn-icons-png.flaticon.com/512/2936/2936956.png"
        }
        
        requests.post(DISCORD_WEBHOOK_URL, json=payload)
            
    except Exception as e:
        print(f"Discord Notify Error: {e}")

def check_and_notify_shift_completion(sheet, target_round_time):
    """ตรวจสอบว่ารถเข้าครบทุกคันหรือยัง (เช็คเฉพาะกะของคนขับที่กดอัปเดต)"""
    try:
        # 1. ระบุกะของผู้กด (Day หรือ Night)
        target_is_day = True
        try:
            h_check = int(str(target_round_time).split(':')[0])
            if h_check < 6 or h_check >= 19: target_is_day = False
        except: 
            print("Invalid round time, skipping check")
            return

        # 2. ดึงข้อมูลมานับยอด (ดึงสด ไม่ใช้ Cache เพื่อความแม่นยำในการแจ้งเตือน)
        raw_jobs = sheet.worksheet('Jobs').get_all_records()
        
        now_thai = datetime.now() + timedelta(hours=7)
        today_str = now_thai.strftime("%Y-%m-%d")
        todays_jobs = [j for j in raw_jobs if str(j['PO_Date']).strip() == today_str]

        stats = {
            'day': {'total': 0, 'entered': 0},
            'night': {'total': 0, 'entered': 0}
        }
        
        unique_cars = {}
        for job in todays_jobs:
            if str(job.get('Status', '')).lower() == 'cancel': continue
            key = (str(job['PO_Date']), str(job['Round']), str(job['Car_No']))
            if key not in unique_cars: unique_cars[key] = job

        for key, job in unique_cars.items():
            r_time = str(job.get('Round', '')).strip()
            is_day = True
            try:
                h = int(r_time.split(':')[0])
                if h < 6 or h >= 19: is_day = False
            except: pass
            
            target = stats['day'] if is_day else stats['night']
            target['total'] += 1
            
            if str(job.get('T1_Enter', '')).strip() != '':
                target['entered'] += 1

        # 3. ส่งแจ้งเตือน (เฉพาะกะที่ตรงกับคนกด)
        if target_is_day:
            if stats['day']['total'] > 0 and stats['day']['total'] == stats['day']['entered']:
                msg = f"☀️ **แจ้งเตือน: รถรอบเช้าเข้าโรงงาน ครบแล้ว!**\n✅ จำนวนทั้งหมด: `{stats['day']['total']}` คัน\n🕒 เวลา: `{now_thai.strftime('%H:%M')} น.`"
                send_discord_msg(msg)
        else:
            if stats['night']['total'] > 0 and stats['night']['total'] == stats['night']['entered']:
                msg = f"🌙 **แจ้งเตือน: รถรอบดึกเข้าโรงงาน ครบแล้ว!**\n✅ จำนวนทั้งหมด: `{stats['night']['total']}` คัน\n🕒 เวลา: `{now_thai.strftime('%H:%M')} น.`"
                send_discord_msg(msg)

    except Exception as e:
        print(f"Check Notify Error: {e}")