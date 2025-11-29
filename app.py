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

# Import ฟังก์ชันจาก utils.py
from utils import get_db, get_cached_records, invalidate_cache, check_and_notify_shift_completion, thai_date_filter

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'lmt_driver_app_secret_key_2024')
CORS(app)

# Register Filter
app.jinja_env.filters['thai_date'] = thai_date_filter

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

    # Driver History Logic
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

    # Driver Stats
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

    # Idle Drivers
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

    # Shift Status Logic
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

@app.route('/driver')
def driver_select():
    sheet = get_db()
    drivers = sheet.worksheet('Drivers').col_values(1)[1:]
    all_jobs = get_cached_records(sheet, 'Jobs')
    now_thai = datetime.now() + timedelta(hours=7)
    
    driver_info = {} 
    for name in drivers:
        driver_info[name] = {
            'pending_set': set(), 
            'pending_count': 0, 
            'urgent_msg': '', 'urgent_color': '', 'urgent_time': '', 'sort_weight': 999
        }

    for job in all_jobs:
        d_name = job['Driver']
        if d_name not in driver_info: continue
        
        if job['Status'] != 'Done':
            trip_key = f"{job['PO_Date']}_{job['Round']}_{job['Car_No']}"
            driver_info[d_name]['pending_set'].add(trip_key)
            try:
                load_date_str = job.get('Load_Date', job['PO_Date'])
                round_str = str(job['Round']).strip()
                job_dt_str = f"{load_date_str} {round_str}"
                try: job_dt = datetime.strptime(job_dt_str, "%Y-%m-%d %H:%M")
                except ValueError: job_dt = datetime.strptime(f"{job['PO_Date']} {round_str}", "%Y-%m-%d %H:%M")
                
                diff = job_dt - now_thai
                hours_diff = diff.total_seconds() / 3600
                h = job_dt.hour
                m = job_dt.minute
                
                msg, color, weight = "", "", 999
                if hours_diff <= 0:
                    if hours_diff > -12: 
                        msg, color, weight = "❗ โหลดตอนนี้", "bg-red-500 text-white border-red-600 animate-pulse shadow-red-200", 1
                elif 0 < hours_diff <= 16:
                    if 6 <= h <= 12:   msg, color = "☀️ โหลดเช้านี้", "bg-yellow-100 text-yellow-700 border-yellow-200"
                    elif 13 <= h <= 18: msg, color = "⛅ โหลดบ่ายนี้", "bg-orange-100 text-orange-700 border-orange-200"
                    else:               msg, color = "🌙 โหลดคืนนี้", "bg-indigo-100 text-indigo-700 border-indigo-200"
                    weight = 2
                elif 16 < hours_diff <= 40:
                    period = "คืนพรุ่งนี้" if (h >= 19 or h <= 5) else "วันพรุ่งนี้"
                    if driver_info[d_name]['sort_weight'] > 3:
                        msg, color, weight = f"⏩ เตรียมโหลด{period}", "bg-gray-100 text-gray-500 border-gray-200", 3

                if weight < driver_info[d_name]['sort_weight']:
                    driver_info[d_name]['urgent_msg'] = msg
                    driver_info[d_name]['urgent_color'] = color
                    driver_info[d_name]['urgent_time'] = f"{h:02}:{m:02} น."
                    driver_info[d_name]['sort_weight'] = weight
            except Exception as e: pass
    
    for name in drivers:
        driver_info[name]['pending_count'] = len(driver_info[name]['pending_set'])

    return render_template('driver_select.html', drivers=drivers, driver_info=driver_info)

@app.route('/driver/tasks', methods=['GET'])
def driver_tasks():
    driver_name = request.args.get('name')
    if not driver_name: return redirect(url_for('driver_select'))
        
    sheet = get_db()
    raw_data = get_cached_records(sheet, 'Jobs')
    
    driver_jobs_with_id = []
    for idx, job in enumerate(raw_data):
        if job['Driver'] == driver_name:
            job_copy = job.copy()
            job_copy['row_id'] = idx + 2
            driver_jobs_with_id.append(job_copy)

    trips = {}
    for job in driver_jobs_with_id:
        trip_key = (str(job['PO_Date']), str(job['Round']), str(job['Car_No']))
        if trip_key not in trips: trips[trip_key] = []
        trips[trip_key].append(job)

    final_jobs_list = []
    now_thai = datetime.now() + timedelta(hours=7)
    today_date = now_thai.date()
    
    for key, job_list in trips.items():
        is_trip_fully_done = all(j['Status'] == 'Done' for j in job_list)
        show_this_trip = False
        
        if not is_trip_fully_done:
            show_this_trip = True
        else:
            try:
                job_date_str = job_list[0].get('Load_Date', job_list[0]['PO_Date'])
                pd = datetime.strptime(job_date_str, "%Y-%m-%d").date()
                if pd >= today_date:
                    show_this_trip = True
            except: 
                show_this_trip = True
            
        if show_this_trip:
            final_jobs_list.extend(job_list)

    def sort_key_func(job):
        return (str(job['PO_Date']), str(job.get('Load_Date', '')), str(job['Round']))
    
    my_jobs = sorted(final_jobs_list, key=sort_key_func)
    today_date_str = now_thai.strftime("%Y-%m-%d")

    for job in my_jobs:
        try:
            load_date_str = job.get('Load_Date', job['PO_Date'])
            round_str = str(job['Round']).strip()
            job_dt_str = f"{load_date_str} {round_str}"
            try: job_dt = datetime.strptime(job_dt_str, "%Y-%m-%d %H:%M")
            except: job_dt = datetime.strptime(f"{job['PO_Date']} {round_str}", "%Y-%m-%d %H:%M")
            
            diff = job_dt - now_thai
            hours_diff = diff.total_seconds() / 3600
            th_year = job_dt.year + 543
            real_date_str = f"{job_dt.day}/{job_dt.month}/{str(th_year)[2:]}"
            h = job_dt.hour

            job['smart_title'] = f"เวลา {round_str}"
            job['smart_detail'] = f"วันที่ {real_date_str}"
            job['ui_class'] = {'bg': 'bg-gray-50', 'text': 'text-gray-500', 'icon': 'fa-clock'}

            if hours_diff <= 0:
                if hours_diff > -12:
                    job['smart_title'] = f"❗ เข้าโหลดงานตอนนี้"
                    job['ui_class'] = {'bg': 'bg-red-50 border-red-100 ring-2 ring-red-200 animate-pulse', 'text': 'text-red-600', 'icon': 'fa-truck-ramp-box'}
                else:
                    job['smart_title'] = f"🔥 งานค้างส่ง"
                    job['ui_class'] = {'bg': 'bg-red-50 border-red-100', 'text': 'text-red-500', 'icon': 'fa-triangle-exclamation'}
                job['smart_detail'] = f"กำหนด: {round_str} น. ({real_date_str})"
            elif 0 < hours_diff <= 16:
                if 6 <= h <= 12:   p, i, t = "เช้านี้", "fa-sun", "yellow"
                elif 13 <= h <= 18: p, i, t = "บ่ายนี้", "fa-cloud-sun", "orange"
                else:               p, i, t = "คืนนี้", "fa-moon", "indigo"
                job['smart_title'] = f"โหลดสินค้า{p}"
                job['smart_detail'] = f"เวลา {round_str} น. ของวันที่ {real_date_str}"
                job['ui_class'] = {'bg': f'bg-{t}-50 border-{t}-100 ring-1 ring-{t}-50', 'text': f'text-{t}-600', 'icon': i}
            elif 16 < hours_diff <= 40:
                period = "คืนพรุ่งนี้" if (h >= 19 or h <= 5) else "วันพรุ่งนี้"
                job['smart_title'] = f"⏩ เตรียมโหลด{period}"
                job['smart_detail'] = f"เวลา {round_str} น. ของวันที่ {real_date_str}"
                job['ui_class'] = {'bg': 'bg-blue-50 border-blue-100', 'text': 'text-blue-600', 'icon': 'fa-calendar-day'}
            else:
                job['smart_title'] = f"📅 งานล่วงหน้า"
                job['smart_detail'] = f"วันที่ {real_date_str} เวลา {round_str} น."
                job['ui_class'] = {'bg': 'bg-gray-50 border-gray-100', 'text': 'text-gray-500', 'icon': 'fa-calendar-days'}
            
            po_d = datetime.strptime(job['PO_Date'], "%Y-%m-%d")
            po_th = f"{po_d.day}/{po_d.month}/{str(po_d.year+543)[2:]}"
            job['po_label'] = f"(เอกสาร PO วันที่ {po_th})"
        except Exception as e: pass

    return render_template('driver_tasks.html', name=driver_name, jobs=my_jobs, today_date=today_date_str)

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
    
    if step == '1' and mode == 'update':
        # Fetch row data to get round time for notification
        try:
            updated_row_data = ws.row_values(row_id_target)
            if len(updated_row_data) > 2:
                target_round_time = updated_row_data[2] # Col C
                check_and_notify_shift_completion(sheet, target_round_time)
        except Exception as e:
            print(f"Notify fetch error: {e}")
        
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