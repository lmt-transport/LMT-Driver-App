from flask import Flask, render_template, request, redirect, url_for, session, send_file, make_response
from flask_cors import CORS
from fpdf import FPDF
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import pandas as pd
import io
import os
import gspread.utils 
import json
import time
import requests # จำเป็นสำหรับการส่ง Discord
from openpyxl import load_workbook
from openpyxl.styles import Font, Border, Side, PatternFill, Alignment
from openpyxl.utils import get_column_letter

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'lmt_driver_app_secret_key_2024')
CORS(app)

# ==========================================
# [CONFIG] ตั้งค่า Discord Webhook
# ==========================================
DISCORD_WEBHOOK_URL = 'https://discord.com/api/webhooks/1444236316404482139/UJc-I_NRT33p9UKCas5ATGgjAlqlrtxBuPhvKYKnI-Pz2_AyxAnOs_UFNl203_sqLsI5'

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
        
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload)
            
    except Exception as e:
        print(f"Discord Notify Error: {e}")

# --- Caching System ---
cache_storage = {
    'Jobs': {'data': None, 'timestamp': 0},
    'Drivers': {'data': None, 'timestamp': 0},
    'Users': {'data': None, 'timestamp': 0}
}
CACHE_DURATION = 60 

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
    except gspread.exceptions.APIError as e:
        if "429" in str(e) and cache_entry and cache_entry['data'] is not None:
            return cache_entry['data']
        raise e

def invalidate_cache(worksheet_name):
    if worksheet_name in cache_storage:
        cache_storage[worksheet_name] = {'data': None, 'timestamp': 0}

# [UPDATED] ฟังก์ชันแจ้งเตือนฉบับปรับปรุง (นับตาม PO จริง + เพิ่มแจ้งเตือนออกโรงงาน)
def check_and_notify_shift_completion(sheet, target_po_date, target_round_time, step_trigger):
    """
    ตรวจสอบว่ารถ เข้า/ออก ครบทุกคันหรือยัง 
    target_po_date: วันที่ของ PO ที่กำลังอัปเดต (ป้องกันการนับผิดวัน)
    target_round_time: เวลารอบ (เพื่อหากะ เช้า/ดึก)
    step_trigger: '1' (เข้าโรงงาน) หรือ '6' (ออกโรงงาน)
    """
    try:
        # 1. ระบุกะของผู้กด (Day หรือ Night)
        target_is_day = True
        try:
            h_check = int(str(target_round_time).split(':')[0])
            if h_check < 6 or h_check >= 19: target_is_day = False
        except: return

        shift_name = "รอบเช้า" if target_is_day else "รอบดึก"
        shift_emoji = "☀️" if target_is_day else "🌙"

        # 2. ดึงข้อมูลมานับยอด (ดึงสด ไม่ใช้ Cache เพื่อความแม่นยำ)
        raw_jobs = sheet.worksheet('Jobs').get_all_records()
        
        # 3. กรองงานเฉพาะ PO Date เดียวกัน (แก้ไขปัญหา Dashboard 6 คัน แต่นับได้ 5 คันเพราะคนละวัน)
        target_jobs = [j for j in raw_jobs if str(j['PO_Date']).strip() == str(target_po_date).strip()]

        # ตัวแปรนับยอด (นับเฉพาะกะที่เกี่ยวข้อง)
        total_cars = 0
        action_count = 0 # เข้า หรือ ออก ตาม step_trigger
        
        unique_cars = {}
        for job in target_jobs:
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
            
            # นับเฉพาะกะที่ตรงกับคนกด
            if is_day == target_is_day:
                total_cars += 1
                
                # เช็ค Step 1 (เข้าโรงงาน)
                if step_trigger == '1':
                    if str(job.get('T1_Enter', '')).strip() != '':
                        action_count += 1
                
                # เช็ค Step 6 (ออกโรงงาน)
                elif step_trigger == '6':
                    if str(job.get('T6_Exit', '')).strip() != '':
                        action_count += 1

        # 4. ส่งแจ้งเตือนถ้าครบ (และต้องมีรถมากกว่า 0 คัน)
        if total_cars > 0 and total_cars == action_count:
            now_str = (datetime.now() + timedelta(hours=7)).strftime('%H:%M')
            
            if step_trigger == '1':
                msg = f"{shift_emoji} **แจ้งเตือน: รถ{shift_name} เข้าโรงงาน ครบแล้ว!**\n✅ PO Date: {target_po_date}\n🚛 จำนวน: `{total_cars}` คัน\n🕒 เวลา: `{now_str} น.`"
                send_discord_msg(msg)
            
            elif step_trigger == '6':
                msg = f"🚀 **แจ้งเตือน: รถ{shift_name} ออกจากโรงงาน ครบแล้ว!**\n✅ PO Date: {target_po_date}\n🚛 จำนวน: `{total_cars}` คัน\n🕒 เวลา: `{now_str} น.`"
                send_discord_msg(msg)

    except Exception as e:
        print(f"Check Notify Error: {e}")

# --- Custom Filter ---
def thai_date_filter(date_str):
    if not date_str: return ""
    try:
        date_obj = datetime.strptime(str(date_str).strip(), "%Y-%m-%d")
        thai_year = date_obj.year + 543
        return date_obj.strftime(f"%d/%m/{thai_year}")
    except ValueError: return date_str

app.jinja_env.filters['thai_date'] = thai_date_filter

# --- DB Connection ---
def get_db():
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds_json = os.environ.get('GSPREAD_CREDENTIALS')
    
    if not creds_json:
        if os.path.exists("credentials.json"): return gspread.service_account(filename="credentials.json").open("DriverLogApp")
        else: return None
        
    creds_dict = json.loads(creds_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    sheet = client.open("DriverLogApp") 
    return sheet

# --- Routes ---

@app.route('/manager_login', methods=['GET', 'POST'])
def manager_login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        try:
            sheet = get_db()
            users = get_cached_records(sheet, 'Users')
            for user in users:
                if str(user['Username']) == username and str(user['Password']) == password:
                    session['user'] = username
                    return redirect(url_for('manager_dashboard'))
            return render_template('login.html', error="ชื่อผู้ใช้หรือรหัสผ่านไม่ถูกต้อง")
        except Exception as e: return render_template('login.html', error=f"Error: {str(e)}")
    return render_template('login.html')

@app.route('/manager')
def manager_dashboard():
    if 'user' not in session: return redirect(url_for('manager_login'))
    
    sheet = get_db()
    raw_jobs = get_cached_records(sheet, 'Jobs')
    drivers = get_cached_records(sheet, 'Drivers')

    date_filter = request.args.get('date_filter')
    now_thai = datetime.now() + timedelta(hours=7)
    today_date = now_thai.strftime("%Y-%m-%d")
    
    if not date_filter:
        date_filter = today_date

    filtered_jobs = [j for j in raw_jobs if str(j['PO_Date']).strip() == str(date_filter).strip()]

    def sort_key_func(job):
        po_date = str(job['PO_Date'])
        car_no_str = str(job['Car_No']).strip()
        round_val = str(job['Round'])
        try: car_no_int = int(car_no_str)
        except ValueError: car_no_int = 99999 
        return (po_date, car_no_int, round_val)

    filtered_jobs = sorted(filtered_jobs, key=sort_key_func)
    
    jobs_by_trip_key = {}
    total_done_jobs = 0
    total_branches = len(filtered_jobs)
    trip_last_end_time = {} 
    
    grouped_jobs_for_stats = []
    current_group = []
    prev_key = None
    late_arrivals_by_po = {}
    total_late_cars = 0

    for job in filtered_jobs:
        curr_key = (str(job['PO_Date']), str(job['Car_No']), str(job['Round']), str(job['Driver']))
        if curr_key != prev_key and prev_key is not None:
            grouped_jobs_for_stats.append(current_group)
            current_group = []
        current_group.append(job)
        prev_key = curr_key
        
        trip_key = (str(job['PO_Date']), str(job['Car_No']), str(job['Round']))
        if trip_key not in jobs_by_trip_key:
            jobs_by_trip_key[trip_key] = []
        jobs_by_trip_key[trip_key].append(job)
        
        if job['Status'] == 'Done':
            total_done_jobs += 1
            
        if not job.get('T1_Enter') and job['Status'] != 'Done':
            try:
                load_date_str = job.get('Load_Date', job['PO_Date'])
                round_str = str(job['Round']).strip()
                plan_dt_str = f"{load_date_str} {round_str}"
                try: plan_dt = datetime.strptime(plan_dt_str, "%Y-%m-%d %H:%M")
                except: plan_dt = datetime.strptime(f"{job['PO_Date']} {round_str}", "%Y-%m-%d %H:%M")
                
                if now_thai > plan_dt:
                    po_key = str(job['PO_Date'])
                    if po_key not in late_arrivals_by_po: late_arrivals_by_po[po_key] = []
                    diff = now_thai - plan_dt
                    hours = int(diff.total_seconds() // 3600)
                    mins = int((diff.total_seconds() % 3600) // 60)
                    job['late_duration'] = f"{hours} ชม. {mins} น."
                    
                    if not any(x['Car_No'] == job['Car_No'] for x in late_arrivals_by_po[po_key]):
                        late_arrivals_by_po[po_key].append(job)
                        total_late_cars += 1
            except: pass
            
    if current_group: grouped_jobs_for_stats.append(current_group)

    driver_history = {} 
    for j in raw_jobs:
        d_name = j.get('Driver')
        r_time = str(j.get('Round', '')).strip()
        if d_name and r_time:
            if d_name not in driver_history:
                driver_history[d_name] = {'day_count': 0, 'night_count': 0}
            try:
                h = int(r_time.split(':')[0])
                if 6 <= h <= 18: driver_history[d_name]['day_count'] += 1
                else: driver_history[d_name]['night_count'] += 1
            except: pass

    driver_stats = {}
    for group in grouped_jobs_for_stats:
        first = group[0]
        d_name = first['Driver']
        if not d_name: continue
        if d_name not in driver_stats: driver_stats[d_name] = {'total_trips': 0, 'rounds': []}
        driver_stats[d_name]['total_trips'] += 1
        driver_stats[d_name]['rounds'].append({
            'round': first['Round'], 'car_no': first['Car_No'], 'plate': first['Plate'],
            'branches': [j['Branch_Name'] for j in group],
            'status': 'Done' if all(j['Status'] == 'Done' for j in group) else 'Pending'
        })
    for d in driver_stats:
        driver_stats[d]['rounds'].sort(key=lambda x: int(str(x['car_no']).strip()) if str(x['car_no']).strip().isdigit() else 9999)

    working_drivers_set = set(driver_stats.keys())
    idle_drivers_day = []
    idle_drivers_night = []
    idle_drivers_hybrid = []
    idle_drivers_new = []

    for d in drivers:
        d_name = d.get('Name')
        if d_name and d_name not in working_drivers_set:
            history = driver_history.get(d_name)
            if history:
                has_day = history['day_count'] > 0
                has_night = history['night_count'] > 0
                if has_day and has_night: idle_drivers_hybrid.append(d)
                elif has_day: idle_drivers_day.append(d)
                else: idle_drivers_night.append(d)
            else: idle_drivers_new.append(d)

    shift_status = {'day': {'total': 0, 'entered': 0, 'is_complete': False}, 'night': {'total': 0, 'entered': 0, 'is_complete': False}}
    unique_trips_check = {}
    for job in filtered_jobs:
        if str(job.get('Status', '')).lower() == 'cancel': continue
        t_key = (str(job['PO_Date']), str(job['Round']), str(job['Car_No']))
        if t_key not in unique_trips_check: unique_trips_check[t_key] = job

    for t_key, job in unique_trips_check.items():
        r_time = str(job.get('Round', '')).strip()
        is_day_shift = True
        try:
            h = int(r_time.split(':')[0])
            if h < 6 or h >= 19: is_day_shift = False
        except: pass
        target = shift_status['day'] if is_day_shift else shift_status['night']
        target['total'] += 1
        if str(job.get('T1_Enter', '')).strip() != '': target['entered'] += 1

    if shift_status['day']['total'] > 0 and shift_status['day']['total'] == shift_status['day']['entered']:
        shift_status['day']['is_complete'] = True
    if shift_status['night']['total'] > 0 and shift_status['night']['total'] == shift_status['night']['entered']:
        shift_status['night']['is_complete'] = True

    completed_trips = 0
    for trip_key, job_list in jobs_by_trip_key.items():
        if all(job['Status'] == 'Done' for job in job_list):
            completed_trips += 1
            last_end_time = max([j['T8_EndJob'] for j in job_list if j['T8_EndJob']], default="")
            trip_last_end_time[trip_key] = last_end_time
        else:
            trip_last_end_time[trip_key] = ""
            
    total_trips = len(jobs_by_trip_key)
    total_running_jobs = total_branches - total_done_jobs

    line_data_day = []
    line_data_night = []

    for group in grouped_jobs_for_stats:
        first = group[0]
        round_str = str(first['Round']).strip()
        load_date_raw = str(first.get('Load_Date', first['PO_Date'])).strip()
        show_date_str = load_date_raw
        try:
            ld_obj = datetime.strptime(load_date_raw, "%Y-%m-%d")
            thai_year = ld_obj.year + 543
            show_date_str = ld_obj.strftime(f"%d/%m/{str(thai_year)[2:]}")
        except: pass

        is_day = True
        try:
            h = int(round_str.split(':')[0])
            if h < 6 or h >= 19: is_day = False
        except: pass

        status_txt = "รอเข้า"
        status_time = ""
        found_branch_activity = False

        if all(j['Status'] == 'Done' for j in group):
            status_txt = "จบงานทุกสาขา"
            status_time = group[-1].get('T8_EndJob', '')
            found_branch_activity = True
        else:
            latest_branch_txt = ""
            latest_branch_time = ""
            for idx, j in enumerate(group, 1):
                t8 = j.get('T8_EndJob')
                t7 = j.get('T7_ArriveBranch')
                if t8:
                    latest_branch_txt = f"จบงานสาขา {idx}"
                    latest_branch_time = t8
                elif t7:
                    latest_branch_txt = f"ถึงสาขา {idx}"
                    latest_branch_time = t7
            if latest_branch_txt:
                status_txt = latest_branch_txt
                status_time = latest_branch_time
                found_branch_activity = True

        if not found_branch_activity:
            if first.get('T6_Exit'): status_txt, status_time = "ออกโรงงาน", first.get('T6_Exit')
            elif first.get('T5_RecvDoc'): status_txt, status_time = "รับเอกสาร", first.get('T5_RecvDoc')
            elif first.get('T4_SubmitDoc'): status_txt, status_time = "ยื่นเอกสาร", first.get('T4_SubmitDoc')
            elif first.get('T3_EndLoad'): status_txt, status_time = "โหลดเสร็จ", first.get('T3_EndLoad')
            elif first.get('T2_StartLoad'): status_txt, status_time = "กำลังโหลด", first.get('T2_StartLoad')
            elif first.get('T1_Enter'): status_txt, status_time = "เข้าโรงงาน", first.get('T1_Enter')
        
        full_status = f"{status_txt}"
        if status_time: full_status = f"{status_txt} ({status_time})"
        
        trip_data = {
            'round': round_str, 'car_no': first['Car_No'], 'plate': first['Plate'],
            'driver': first['Driver'], 'branches': [j['Branch_Name'] for j in group],
            'load_date': show_date_str, 'latest_status': full_status 
        }
        if is_day: line_data_day.append(trip_data)
        else: line_data_night.append(trip_data)
    
    line_data_day.sort(key=lambda x: x['round'])
    def night_sort(item):
        try:
            h, m = map(int, item['round'].split(':'))
            return (h + 24 if h < 6 else h) * 60 + m
        except: return 99999
    line_data_night.sort(key=night_sort)

    for job in filtered_jobs:
        job['is_start_late'] = False
        t_plan_str = str(job.get('Round', '')).strip()
        t_act_str = str(job.get('T2_StartLoad', '')).strip()
        if t_plan_str and t_act_str:
            try:
                fmt_plan = "%H:%M" if len(t_plan_str) <= 5 else "%H:%M:%S"
                fmt_act = "%H:%M" if len(t_act_str) <= 5 else "%H:%M:%S"
                t_plan = datetime.strptime(t_plan_str, fmt_plan)
                t_act = datetime.strptime(t_act_str, fmt_act)
                if (t_plan - t_act).total_seconds() > 12 * 3600: t_act += timedelta(days=1)
                if t_act > t_plan: job['is_start_late'] = True
            except: pass

    try:
        current_date_obj = datetime.strptime(date_filter, "%Y-%m-%d")
        prev_date = (current_date_obj - timedelta(days=1)).strftime("%Y-%m-%d")
        next_date = (current_date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
    except ValueError:
        prev_date, next_date = date_filter, date_filter
    
    all_dates = sorted(list(set([str(j['PO_Date']).strip() for j in raw_jobs])), reverse=True)

    return render_template('manager.html', 
                           jobs=filtered_jobs, drivers=drivers, all_dates=all_dates, 
                           total_trips=total_trips, completed_trips=completed_trips,
                           total_branches=total_branches, total_done_jobs=total_done_jobs,
                           total_running_jobs=total_running_jobs, now_time=now_thai.strftime("%H:%M"),
                           today_date=today_date, current_filter_date=date_filter,
                           prev_date=prev_date, next_date=next_date,
                           trip_last_end_time=trip_last_end_time,
                           line_data_day=line_data_day, line_data_night=line_data_night,
                           late_arrivals_by_po=late_arrivals_by_po, total_late_cars=total_late_cars,
                           driver_stats=driver_stats,
                           idle_drivers_day=idle_drivers_day, idle_drivers_night=idle_drivers_night,
                           idle_drivers_hybrid=idle_drivers_hybrid, idle_drivers_new=idle_drivers_new,
                           shift_status=shift_status
                           )

@app.route('/create_job', methods=['POST'])
def create_job():
    if 'user' not in session: return redirect(url_for('manager_login'))
    sheet = get_db()
    ws = sheet.worksheet('Jobs')
    
    po_date = request.form['po_date']
    load_date = request.form['load_date']
    round_time = request.form['round_time']
    car_no = request.form['car_no']
    driver_name = request.form['driver_name']
    branches = request.form.getlist('branches') 
    
    drivers_ws = sheet.worksheet('Drivers')
    driver_list = drivers_ws.get_all_records()
    plate = ""
    for d in driver_list:
        if d['Name'] == driver_name:
            plate = d['Plate_License']
            break
            
    new_rows = []
    for branch in branches:
        if branch.strip(): 
            row = [po_date, load_date, round_time, car_no, driver_name, plate, branch, "", "", "", "", "", "", "", "", "New", "", "", "", "", "", "", "", "", ""]
            new_rows.append(row)
    
    if new_rows: 
        ws.append_rows(new_rows)
        invalidate_cache('Jobs')
    
    return redirect(url_for('manager_dashboard'))

@app.route('/delete_job', methods=['POST'])
def delete_job():
    if 'user' not in session: return redirect(url_for('manager_login'))
    sheet = get_db()
    ws = sheet.worksheet('Jobs')
    
    po_date = request.form['po_date']
    round_time = request.form['round_time']
    car_no = request.form['car_no']
    
    try:
        all_values = ws.get_all_values()
        rows_to_delete = []
        for i, row in enumerate(all_values):
            if i > 0: 
                if (row[0] == po_date and str(row[2]) == str(round_time) and str(row[3]) == str(car_no)):        
                    rows_to_delete.append(i + 1)
        for row_idx in sorted(rows_to_delete, reverse=True):
            ws.delete_rows(row_idx)
        invalidate_cache('Jobs')
        return redirect(url_for('manager_dashboard'))
    except Exception as e: return f"Error: {e}"

@app.route('/export_excel')
def export_excel():
    sheet = get_db()
    raw_jobs = get_cached_records(sheet, 'Jobs')
    date_filter = request.args.get('date_filter')
    if date_filter:
        jobs = [j for j in raw_jobs if str(j['PO_Date']).strip() == str(date_filter).strip()]
    else:
        jobs = raw_jobs
        
    def sort_key_func(job):
        po_date = str(job['PO_Date'])
        car_no_str = str(job['Car_No']).strip()
        round_val = str(job['Round'])
        try: car_no_int = int(car_no_str)
        except ValueError: car_no_int = 99999 
        return (po_date, car_no_int, round_val)

    jobs = sorted(jobs, key=sort_key_func)
    
    export_data = []
    prev_trip_key = None
    grouped_jobs_for_summary = []
    current_group = []
    
    for job_index, job in enumerate(jobs):
        current_trip_key = (str(job['PO_Date']), str(job['Car_No']), str(job['Round']), str(job['Driver']))
        is_same = (current_trip_key == prev_trip_key)
        if current_trip_key != prev_trip_key and prev_trip_key is not None:
            grouped_jobs_for_summary.append(current_group)
            current_group = []
        current_group.append(job)
        
        t2_display = job['T2_StartLoad']
        if not is_same and job['T2_StartLoad']: 
            try:
                plan_time_str = str(job['Round']).strip()
                actual_time_str = str(job['T2_StartLoad']).strip()
                if plan_time_str and actual_time_str:
                    fmt = "%H:%M" if len(plan_time_str) <= 5 else "%H:%M:%S"
                    fmt_act = "%H:%M" if len(actual_time_str) <= 5 else "%H:%M:%S"
                    t_plan = datetime.strptime(plan_time_str, fmt)
                    t_act = datetime.strptime(actual_time_str, fmt_act)
                    if (t_plan - t_act).total_seconds() > 12 * 3600: t_act = t_act + timedelta(days=1)
                    if t_act > t_plan:
                        diff = t_act - t_plan
                        total_seconds = diff.total_seconds()
                        hours = int(total_seconds // 3600)
                        minutes = int((total_seconds % 3600) // 60)
                        delay_msg = f" (ล่าช้า {hours} ชม. {minutes} น.)"
                        t2_display = f"{actual_time_str}{delay_msg}"
            except: pass 

        formatted_date = job['PO_Date']
        try:
            date_obj = datetime.strptime(str(job['PO_Date']).strip(), "%Y-%m-%d")
            formatted_date = date_obj.strftime("%d/%m/%Y")
        except: pass
            
        row = {
            'ลำดับรถ': "" if is_same else job['Car_No'],
            'PO Date': "" if is_same else formatted_date,
            'เวลาโหลด': "" if is_same else job['Round'], 
            'คนขับ': "" if is_same else job['Driver'],
            'ปลายทาง (สาขา)': job['Branch_Name'],
            'ทะเบียนรถ': "" if is_same else job['Plate'],
            'เข้าโรงงาน': "" if is_same else job['T1_Enter'],
            'เริ่มโหลด': "" if is_same else t2_display, 
            'โหลดเสร็จ': "" if is_same else job['T3_EndLoad'],
            'ยื่นเอกสาร': "" if is_same else job['T4_SubmitDoc'],
            'รับเอกสาร': "" if is_same else job['T5_RecvDoc'],
            'ออกโรงงาน': "" if is_same else job['T6_Exit'],
            'ถึงสาขา': job['T7_ArriveBranch'],
            'จบงาน': job['T8_EndJob']
        }
        export_data.append(row)
        prev_trip_key = current_trip_key

    if current_group: grouped_jobs_for_summary.append(current_group)

    df = pd.DataFrame(export_data)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Report')
    
    output.seek(0)
    wb = load_workbook(output)
    ws = wb.active
    
    font_header = Font(name='Cordia New', size=14, bold=True, color='FFFFFF') 
    font_body = Font(name='Cordia New', size=14)
    font_summary_head = Font(name='Cordia New', size=14, bold=True)
    font_summary_body = Font(name='Cordia New', size=14)
    side_thin = Side(border_style="thin", color="000000")
    side_none = Side(border_style=None) 
    border_all = Border(top=side_thin, bottom=side_thin, left=side_thin, right=side_thin)
    border_header = Border(top=side_thin, bottom=side_thin, left=side_thin, right=side_thin)
    align_center = Alignment(horizontal='center', vertical='center', wrap_text=True)
    align_left = Alignment(horizontal='left', vertical='center', wrap_text=True)
    fill_header = PatternFill(start_color='2E4053', end_color='2E4053', fill_type='solid')
    fill_white = PatternFill(start_color='FFFFFF', end_color='FFFFFF', fill_type='solid')
    fill_blue_light = PatternFill(start_color='EBF5FB', end_color='EBF5FB', fill_type='solid')
    fill_green_branch = PatternFill(start_color='D5F5E3', end_color='D5F5E3', fill_type='solid')
    fill_red_end = PatternFill(start_color='FADBD8', end_color='FADBD8', fill_type='solid')
    fill_sum_head = PatternFill(start_color='D6EAF8', end_color='D6EAF8', fill_type='solid') 
    fill_sum_total = PatternFill(start_color='FFFF00', end_color='FFFF00', fill_type='solid') 

    ws.freeze_panes = 'A2'
    current_trip_id = None
    is_zebra_active = False

    for row in ws.iter_rows(min_row=1, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
        ws.row_dimensions[row[0].row].height = 21
        if row[0].row == 1:
            for cell in row:
                cell.font = font_header
                cell.fill = fill_header
                cell.alignment = align_center
                cell.border = border_header
            continue

        job_index = row[0].row - 2
        is_group_end = False
        if 0 <= job_index < len(jobs):
            job_data = jobs[job_index]
            if job_index == len(jobs) - 1: is_group_end = True
            else:
                next_job = jobs[job_index + 1]
                if (str(job_data['PO_Date']) != str(next_job['PO_Date'])) or (str(job_data['Car_No']) != str(next_job['Car_No'])) or (str(job_data['Round']) != str(next_job['Round'])):
                    is_group_end = True

            this_trip_key = (str(job_data['PO_Date']), str(job_data['Car_No']), str(job_data['Round']))
            if this_trip_key != current_trip_id:
                is_zebra_active = not is_zebra_active
                current_trip_id = this_trip_key

        row_fill = fill_blue_light if is_zebra_active else fill_white
        current_border = Border(left=side_thin, right=side_thin, top=side_none, bottom=side_thin if is_group_end else side_none)

        for cell in row:
            col_name = ws.cell(row=1, column=cell.column).value
            f_bold = False
            f_color = '000000'
            if col_name in ['ถึงสาขา', 'จบงาน', 'เริ่มโหลด']: f_bold = True
            if col_name == 'เริ่มโหลด':
                cell_val_str = str(cell.value) if cell.value else ""
                if "(ล่าช้า" in cell_val_str: f_color = 'C0392B'
                elif cell_val_str.strip() != "": f_color = '196F3D'

            cell.font = Font(name='Cordia New', size=14, bold=f_bold, color=f_color)
            cell.border = current_border 
            cell.fill = row_fill
            if col_name == 'ถึงสาขา': cell.fill = fill_green_branch
            elif col_name == 'จบงาน': cell.fill = fill_red_end
            if col_name in ['คนขับ', 'ปลายทาง (สาขา)', 'ทะเบียนรถ']: cell.alignment = align_left
            else: cell.alignment = align_center

    def create_counter(): return {'count':0, 't1':0, 't2':0, 't3':0, 't4':0, 't5':0, 't6':0, 't7':0, 't8':0}
    sum_day = create_counter()
    sum_night = create_counter()
    
    for group in grouped_jobs_for_summary:
        if not group: continue
        first_job = group[0]
        round_time = str(first_job.get('Round', '')).strip()
        is_day_shift = True
        try:
            hour = int(round_time.split(':')[0])
            if not (6 <= hour <= 18): is_day_shift = False
        except: pass

        target = sum_day if is_day_shift else sum_night
        target['count'] += 1 
        if first_job.get('T1_Enter'): target['t1'] += 1
        if first_job.get('T2_StartLoad'): target['t2'] += 1
        if first_job.get('T3_EndLoad'): target['t3'] += 1
        if first_job.get('T4_SubmitDoc'): target['t4'] += 1
        if first_job.get('T5_RecvDoc'): target['t5'] += 1
        if first_job.get('T6_Exit'): target['t6'] += 1
        
        if any(str(j.get('T7_ArriveBranch', '')).strip() != '' for j in group): target['t7'] += 1
        if all(str(j.get('T8_EndJob', '')).strip() != '' for j in group): target['t8'] += 1

    sum_total = create_counter()
    for k in sum_total: sum_total[k] = sum_day[k] + sum_night[k]

    start_row = ws.max_row + 2 
    summary_headers = ['รอบโหลด', 'จำนวนรถ', 'เข้าโรงงาน', 'เริ่มโหลด', 'โหลดเสร็จ', 'ยื่นเอกสาร', 'รับเอกสาร', 'ออกโรงงาน', 'ถึงสาขา', 'จบงาน']
    col_map_idx = [5, 6, 7, 8, 9, 10, 11, 12, 13, 14] 
    
    ws.cell(row=start_row, column=5, value="รอบโหลด")
    for i, label in enumerate(summary_headers[1:]):
        ws.cell(row=start_row, column=col_map_idx[i+1], value=label)

    rows_to_write = [('กลางวัน', sum_day), ('กลางคืน', sum_night), ('รวม', sum_total)]
    
    for idx, (label, data) in enumerate(rows_to_write):
        curr_r = start_row + 1 + idx
        ws.row_dimensions[curr_r].height = 21
        ws.cell(row=curr_r, column=5, value=label)
        vals = [data['count'], data['t1'], data['t2'], data['t3'], data['t4'], data['t5'], data['t6'], data['t7'], data['t8']]
        for i, val in enumerate(vals):
            ws.cell(row=curr_r, column=col_map_idx[i+1], value=val)

    for r in range(start_row, start_row + 4):
        for c in range(5, 15): 
            cell = ws.cell(row=r, column=c)
            cell.border = border_all
            cell.alignment = align_center
            cell.font = font_summary_body
            
            if r == start_row:
                cell.fill = fill_sum_head
                cell.font = font_summary_head
            elif r == start_row + 3:
                cell.fill = fill_sum_total
                cell.font = font_summary_head

    # Column Width Adjustment
    for column_cells in ws.columns:
        col_letter = get_column_letter(column_cells[0].column)
        col_header = column_cells[0].value
        if col_header == 'เริ่มโหลด': ws.column_dimensions[col_letter].width = 22.00 
        else:
            length = 0
            for cell in column_cells:
                if cell.row < start_row: 
                    val = str(cell.value) if cell.value else ""
                    lines = val.split('\n')
                    longest = max(len(line) for line in lines) if lines else 0
                    if longest > length: length = longest
            ws.column_dimensions[col_letter].width = min(length + 5, 50)

    final_output = io.BytesIO()
    wb.save(final_output)
    final_output.seek(0)
    filename = f"Report_{date_filter if date_filter else 'All'}.xlsx"
    return send_file(final_output, download_name=filename, as_attachment=True)

@app.route('/update_status', methods=['POST'])
def update_status():
    row_id_target = int(request.form['row_id'])
    step = request.form['step']
    driver_name = request.form['driver_name']
    lat = request.form.get('lat', '')
    long = request.form.get('long', '')
    mode = request.form.get('mode', 'update')
    
    location_str = f"{lat},{long}" if lat and long else ""
    current_time = (datetime.now() + timedelta(hours=7)).strftime("%H:%M")
    
    sheet = get_db()
    ws = sheet.worksheet('Jobs')
    
    time_col_map = {'1': 8, '2': 9, '3': 10, '4': 11, '5': 12, '6': 13, '7': 14, '8': 15}
    loc_col_map = {'1': 17, '2': 18, '3': 19, '4': 20, '5': 21, '6': 22, '7': 23, '8': 24}
    time_col = time_col_map.get(step)
    loc_col = loc_col_map.get(step)
    updates = []

    val_to_save = current_time if mode == 'update' else ""
    loc_to_save = location_str if mode == 'update' else ""

    if step in ['1', '2', '3', '4', '5', '6']:
        target_row_data = ws.row_values(row_id_target)
        if len(target_row_data) < 4: return redirect(url_for('driver_tasks', name=driver_name))
        target_po = target_row_data[0] 
        target_round = target_row_data[2]
        target_car = target_row_data[3]
        
        all_values = ws.get_all_values()
        for i, row in enumerate(all_values[1:]): 
            current_row_id = i + 2 
            if (len(row) > 3 and row[0] == target_po and row[2] == target_round and row[3] == target_car):      
                cell_coord_time = gspread.utils.rowcol_to_a1(current_row_id, time_col)
                updates.append({'range': cell_coord_time, 'values': [[val_to_save]]})
                if location_str or mode == 'cancel':
                    cell_coord_loc = gspread.utils.rowcol_to_a1(current_row_id, loc_col)
                    updates.append({'range': cell_coord_loc, 'values': [[loc_to_save]]})
        if updates: ws.batch_update(updates)

    elif step in ['7', '8']:
        cell_coord_time = gspread.utils.rowcol_to_a1(row_id_target, time_col)
        updates.append({'range': cell_coord_time, 'values': [[val_to_save]]})
        if location_str or mode == 'cancel':
            cell_coord_loc = gspread.utils.rowcol_to_a1(row_id_target, loc_col)
            updates.append({'range': cell_coord_loc, 'values': [[loc_to_save]]})
        if updates: ws.batch_update(updates)

    if step == '8': 
        status_val = "Done" if mode == 'update' else ""
        ws.update_cell(row_id_target, 16, status_val)
    
    invalidate_cache('Jobs')
    
    # [NEW] Trigger notification checks
    if step == '1' and mode == 'update':
        # Fetch the updated row to determine round time
        try:
            # [IMPORTANT] We need to pass the PO date to the check function
            # Re-fetch the target row to be sure
            updated_row_data = ws.row_values(row_id_target)
            if len(updated_row_data) > 2:
                target_po_date = updated_row_data[0] # Col A
                target_round_time = updated_row_data[2] # Col C
                check_and_notify_shift_completion(sheet, target_po_date, target_round_time, step)
        except Exception as e:
            print(f"Error fetching row for notification: {e}")
    
    # [NEW] Trigger notification for Step 6 (Exit Factory)
    if step == '6' and mode == 'update':
        try:
            updated_row_data = ws.row_values(row_id_target)
            if len(updated_row_data) > 2:
                target_po_date = updated_row_data[0] # Col A
                target_round_time = updated_row_data[2] # Col C
                check_and_notify_shift_completion(sheet, target_po_date, target_round_time, step)
        except Exception as e:
            print(f"Error fetching row for notification: {e}")
        
    return redirect(url_for('driver_tasks', name=driver_name))

@app.route('/update_driver', methods=['POST'])
def update_driver():
    if 'user' not in session: return json.dumps({'status': 'error', 'message': 'Unauthorized'}), 401
    try:
        data = request.json
        target_po = data.get('po_date')
        target_round = data.get('round_time')
        target_car = str(data.get('car_no'))
        new_driver = data.get('new_driver')
        new_plate = data.get('new_plate')

        sheet = get_db()
        ws = sheet.worksheet('Jobs')
        all_values = ws.get_all_values()
        updates = []
        
        for i, row in enumerate(all_values):
            if i == 0: continue 
            if len(row) < 6: continue
            if (row[0] == target_po and str(row[2]) == str(target_round) and str(row[3]) == target_car):
                row_num = i + 1
                updates.append({'range': f'E{row_num}', 'values': [[new_driver]]})
                updates.append({'range': f'F{row_num}', 'values': [[new_plate]]})

        if updates:
            ws.batch_update(updates)
            invalidate_cache('Jobs')
            return json.dumps({'status': 'success', 'count': len(updates)/2})
        else:
            return json.dumps({'status': 'error', 'message': 'ไม่พบรายการงานที่ตรงกัน'})
    except Exception as e:
        print(f"Error updating driver: {e}")
        return json.dumps({'status': 'error', 'message': str(e)})

@app.route('/')
def index(): return render_template('index.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True)