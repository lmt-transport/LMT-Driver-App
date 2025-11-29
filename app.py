from flask import Flask, render_template, request, redirect, url_for, session, send_file
from flask_cors import CORS
from fpdf import FPDF
from datetime import datetime, timedelta
import pandas as pd
import io
import os
import json
from openpyxl import load_workbook
from openpyxl.styles import Font, Border, Side, PatternFill, Alignment
from openpyxl.utils import get_column_letter
import gspread.utils 

# Import จากไฟล์ที่เราแยกไว้
from utils import get_db, get_cached_records, invalidate_cache, check_and_notify_shift_completion, thai_date_filter

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'lmt_driver_app_secret_key_2024')
CORS(app)

# Register Filter
app.jinja_env.filters['thai_date'] = thai_date_filter

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
    if not date_filter: date_filter = today_date

    filtered_jobs = [j for j in raw_jobs if str(j['PO_Date']).strip() == str(date_filter).strip()]

    def sort_key_func(job):
        try: car_no = int(str(job['Car_No']).strip())
        except: car_no = 99999
        return (str(job['PO_Date']), car_no, str(job['Round']))

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
        if trip_key not in jobs_by_trip_key: jobs_by_trip_key[trip_key] = []
        jobs_by_trip_key[trip_key].append(job)
        
        if job['Status'] == 'Done': total_done_jobs += 1
            
        if not job.get('T1_Enter') and job['Status'] != 'Done':
            try:
                load_date_str = job.get('Load_Date', job['PO_Date'])
                round_str = str(job['Round']).strip()
                plan_dt = datetime.strptime(f"{load_date_str} {round_str}", "%Y-%m-%d %H:%M")
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

    # --- Driver Stats & Idle Drivers Logic ---
    driver_history = {} 
    for j in raw_jobs:
        d_name = j.get('Driver')
        r_time = str(j.get('Round', '')).strip()
        if d_name and r_time:
            if d_name not in driver_history: driver_history[d_name] = {'day': 0, 'night': 0}
            try:
                h = int(r_time.split(':')[0])
                if 6 <= h <= 18: driver_history[d_name]['day'] += 1
                else: driver_history[d_name]['night'] += 1
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

    working_set = set(driver_stats.keys())
    idle_day, idle_night, idle_hybrid, idle_new = [], [], [], []
    for d in drivers:
        name = d.get('Name')
        if name and name not in working_set:
            hist = driver_history.get(name)
            if hist:
                if hist['day'] > 0 and hist['night'] > 0: idle_hybrid.append(d)
                elif hist['day'] > 0: idle_day.append(d)
                else: idle_night.append(d)
            else: idle_new.append(d)

    # --- Shift Status Logic ---
    shift_status = {'day': {'total': 0, 'entered': 0, 'is_complete': False}, 'night': {'total': 0, 'entered': 0, 'is_complete': False}}
    unique_trips = {}
    for job in filtered_jobs:
        if str(job.get('Status', '')).lower() == 'cancel': continue
        key = (str(job['PO_Date']), str(job['Round']), str(job['Car_No']))
        if key not in unique_trips: unique_trips[key] = job

    for key, job in unique_trips.items():
        is_day = True
        try:
            h = int(str(job['Round']).split(':')[0])
            if h < 6 or h >= 19: is_day = False
        except: pass
        target = shift_status['day'] if is_day else shift_status['night']
        target['total'] += 1
        if str(job.get('T1_Enter', '')).strip() != '': target['entered'] += 1

    if shift_status['day']['total'] > 0 and shift_status['day']['total'] == shift_status['day']['entered']: shift_status['day']['is_complete'] = True
    if shift_status['night']['total'] > 0 and shift_status['night']['total'] == shift_status['night']['entered']: shift_status['night']['is_complete'] = True

    completed_trips = 0
    for k, v in jobs_by_trip_key.items():
        if all(j['Status'] == 'Done' for j in v):
            completed_trips += 1
            trip_last_end_time[k] = max([j['T8_EndJob'] for j in v if j['T8_EndJob']], default="")
        else: trip_last_end_time[k] = ""
            
    line_data_day, line_data_night = [], []
    for group in grouped_jobs_for_stats:
        first = group[0]
        status_txt = "รอเข้า"
        status_time = ""
        if all(j['Status'] == 'Done' for j in group):
            status_txt, status_time = "จบงานทุกสาขา", group[-1].get('T8_EndJob', '')
        else:
            for idx, j in enumerate(group, 1):
                if j.get('T8_EndJob'): status_txt, status_time = f"จบงานสาขา {idx}", j.get('T8_EndJob')
                elif j.get('T7_ArriveBranch'): status_txt, status_time = f"ถึงสาขา {idx}", j.get('T7_ArriveBranch')
        
        if status_txt == "รอเข้า":
             if first.get('T6_Exit'): status_txt, status_time = "ออกโรงงาน", first.get('T6_Exit')
             elif first.get('T1_Enter'): status_txt, status_time = "เข้าโรงงาน", first.get('T1_Enter')
        
        is_day = True
        try:
            if int(str(first['Round']).split(':')[0]) < 6 or int(str(first['Round']).split(':')[0]) >= 19: is_day = False
        except: pass
        
        data = {
            'round': first['Round'], 'car_no': first['Car_No'], 'plate': first['Plate'], 'driver': first['Driver'],
            'branches': [j['Branch_Name'] for j in group], 'load_date': first.get('Load_Date', ''),
            'latest_status': f"{status_txt} ({status_time})" if status_time else status_txt
        }
        if is_day: line_data_day.append(data)
        else: line_data_night.append(data)
    
    line_data_day.sort(key=lambda x: x['round'])
    line_data_night.sort(key=lambda x: (int(x['round'].split(':')[0]) + 24 if int(x['round'].split(':')[0]) < 6 else int(x['round'].split(':')[0])))

    # Pagination
    try:
        curr = datetime.strptime(date_filter, "%Y-%m-%d")
        prev_date, next_date = (curr - timedelta(days=1)).strftime("%Y-%m-%d"), (curr + timedelta(days=1)).strftime("%Y-%m-%d")
    except: prev_date, next_date = date_filter, date_filter
    all_dates = sorted(list(set([str(j['PO_Date']).strip() for j in raw_jobs])), reverse=True)

    return render_template('manager.html', 
                           jobs=filtered_jobs, drivers=drivers, all_dates=all_dates, 
                           total_trips=len(jobs_by_trip_key), completed_trips=completed_trips,
                           total_branches=len(filtered_jobs), total_done_jobs=total_done_jobs,
                           total_running_jobs=len(filtered_jobs)-total_done_jobs, now_time=now_thai.strftime("%H:%M"),
                           today_date=today_date, current_filter_date=date_filter, prev_date=prev_date, next_date=next_date,
                           trip_last_end_time=trip_last_end_time, line_data_day=line_data_day, line_data_night=line_data_night,
                           late_arrivals_by_po=late_arrivals_by_po, total_late_cars=total_late_cars,
                           driver_stats=driver_stats, idle_drivers_day=idle_day, idle_drivers_night=idle_night,
                           idle_drivers_hybrid=idle_hybrid, idle_drivers_new=idle_new, shift_status=shift_status)

@app.route('/create_job', methods=['POST'])
def create_job():
    if 'user' not in session: return redirect(url_for('manager_login'))
    sheet = get_db()
    ws = sheet.worksheet('Jobs')
    # ... (Logic การสร้างงาน เหมือนเดิม) ...
    # เพื่อความกระชับ ผมละส่วนนี้ไว้ (ใช้โค้ดเดิมได้เลย) 
    # แค่อย่าลืม invalidate_cache('Jobs') ตอนท้าย
    # ถ้าต้องการโค้ดเต็มส่วนนี้บอกได้ครับ แต่หลักการคือเหมือนเดิม
    return redirect(url_for('manager_dashboard'))

@app.route('/delete_job', methods=['POST'])
def delete_job():
    if 'user' not in session: return redirect(url_for('manager_login'))
    sheet = get_db()
    ws = sheet.worksheet('Jobs')
    try:
        all_values = ws.get_all_values()
        rows_to_delete = []
        po_date, round_time, car_no = request.form['po_date'], request.form['round_time'], request.form['car_no']
        for i, row in enumerate(all_values):
            if i > 0 and row[0] == po_date and str(row[2]) == str(round_time) and str(row[3]) == str(car_no):
                rows_to_delete.append(i + 1)
        for row_idx in sorted(rows_to_delete, reverse=True): ws.delete_rows(row_idx)
        invalidate_cache('Jobs')
        return redirect(url_for('manager_dashboard'))
    except Exception as e: return f"Error: {e}"

@app.route('/update_driver', methods=['POST'])
def update_driver():
    if 'user' not in session: return json.dumps({'status': 'error'}), 401
    try:
        data = request.json
        sheet = get_db()
        ws = sheet.worksheet('Jobs')
        all_values = ws.get_all_values()
        updates = []
        for i, row in enumerate(all_values):
            if i > 0 and len(row) > 6 and row[0] == data['po_date'] and str(row[2]) == str(data['round_time']) and str(row[3]) == str(data['car_no']):
                updates.append({'range': f'E{i+1}', 'values': [[data['new_driver']]]})
                updates.append({'range': f'F{i+1}', 'values': [[data['new_plate']]]})
        if updates:
            ws.batch_update(updates)
            invalidate_cache('Jobs')
            return json.dumps({'status': 'success', 'count': len(updates)/2})
        return json.dumps({'status': 'error', 'message': 'Not found'})
    except Exception as e: return json.dumps({'status': 'error', 'message': str(e)})

# --- ส่วน Export Excel / PDF (ยาวมาก) ---
# แนะนำให้คงไว้เหมือนเดิม หรือถ้าอยากให้ผมรวมให้บอกได้ครับ 
# แต่หลักการคือใช้ get_cached_records('Jobs') แทนการดึงใหม่

@app.route('/')
def index(): return render_template('index.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

# ... (Driver Routes - driver_select, driver_tasks, update_status) ...
# ใช้โค้ดเดิมจากคำตอบก่อนหน้าได้เลยครับ เพราะแยกส่วนกันชัดเจน

if __name__ == '__main__':
    app.run(debug=True)