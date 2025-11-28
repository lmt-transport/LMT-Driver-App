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
from openpyxl import load_workbook
from openpyxl.styles import Font, Border, Side, PatternFill, Alignment
from openpyxl.utils import get_column_letter

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'lmt_driver_app_secret_key_2024')
CORS(app)

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
            users = sheet.worksheet('Users').get_all_records()
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
    
    # 1. โหลดข้อมูลเพียงครั้งเดียว (Optimization)
    sheet = get_db()
    raw_jobs = sheet.worksheet('Jobs').get_all_records()
    drivers = sheet.worksheet('Drivers').get_all_records()

    # 2. Date Filter
    date_filter = request.args.get('date_filter')
    now_thai = datetime.now() + timedelta(hours=7)
    today_date = now_thai.strftime("%Y-%m-%d")
    
    if not date_filter:
        date_filter = today_date

    # 3. Filter Jobs by PO Date
    filtered_jobs = [j for j in raw_jobs if str(j['PO_Date']).strip() == str(date_filter).strip()]

    # 4. Sorting
    def sort_key_func(job):
        po_date = str(job['PO_Date'])
        car_no_str = str(job['Car_No']).strip()
        round_val = str(job['Round'])
        try: car_no_int = int(car_no_str)
        except ValueError: car_no_int = 99999 
        return (po_date, car_no_int, round_val)

    filtered_jobs = sorted(filtered_jobs, key=sort_key_func)
    
    # --- Process Data (Stats, Grouping, Line Data) in Single Pass ---
    jobs_by_trip_key = {}
    total_done_jobs = 0
    total_branches = len(filtered_jobs)
    trip_last_end_time = {} 
    
    grouped_jobs_for_stats = []
    current_group = []
    prev_key = None
    
    # สำหรับ Late Arrival
    late_arrivals_by_po = {}
    total_late_cars = 0

    for job in filtered_jobs:
        # A. Grouping logic
        curr_key = (str(job['PO_Date']), str(job['Car_No']), str(job['Round']), str(job['Driver']))
        if curr_key != prev_key and prev_key is not None:
            grouped_jobs_for_stats.append(current_group)
            current_group = []
        current_group.append(job)
        prev_key = curr_key
        
        # B. Stats Calculation
        trip_key = (str(job['PO_Date']), str(job['Car_No']), str(job['Round']))
        if trip_key not in jobs_by_trip_key:
            jobs_by_trip_key[trip_key] = []
        jobs_by_trip_key[trip_key].append(job)
        
        if job['Status'] == 'Done':
            total_done_jobs += 1
            
        # C. Late Arrival Alert Logic
        if not job.get('T1_Enter') and job['Status'] != 'Done':
            try:
                load_date_str = job.get('Load_Date', job['PO_Date'])
                round_str = str(job['Round']).strip()
                
                plan_dt_str = f"{load_date_str} {round_str}"
                try: plan_dt = datetime.strptime(plan_dt_str, "%Y-%m-%d %H:%M")
                except: plan_dt = datetime.strptime(f"{job['PO_Date']} {round_str}", "%Y-%m-%d %H:%M")
                
                if now_thai > plan_dt:
                    po_key = str(job['PO_Date'])
                    if po_key not in late_arrivals_by_po:
                        late_arrivals_by_po[po_key] = []
                    
                    diff = now_thai - plan_dt
                    hours = int(diff.total_seconds() // 3600)
                    mins = int((diff.total_seconds() % 3600) // 60)
                    
                    job['late_duration'] = f"{hours} ชม. {mins} น."
                    
                    # Prevent duplicates in alert list
                    if not any(x['Car_No'] == job['Car_No'] for x in late_arrivals_by_po[po_key]):
                        late_arrivals_by_po[po_key].append(job)
                        total_late_cars += 1
            except: 
                pass
            
    if current_group: grouped_jobs_for_stats.append(current_group)

    # D. Calculate Completed Trips
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

    # E. Prepare Line Data
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

        # Status Logic
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
            'round': round_str,
            'car_no': first['Car_No'],
            'plate': first['Plate'],
            'driver': first['Driver'],
            'branches': [j['Branch_Name'] for j in group],
            'load_date': show_date_str,
            'latest_status': full_status 
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

    # Pre-calculate Late Status for Display
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

    # Pagination
    try:
        current_date_obj = datetime.strptime(date_filter, "%Y-%m-%d")
        prev_date = (current_date_obj - timedelta(days=1)).strftime("%Y-%m-%d")
        next_date = (current_date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
    except ValueError:
        prev_date, next_date = date_filter, date_filter
    
    all_dates = sorted(list(set([str(j['PO_Date']).strip() for j in raw_jobs])), reverse=True)

    return render_template('manager.html', 
                           jobs=filtered_jobs, 
                           drivers=drivers, 
                           all_dates=all_dates, 
                           total_trips=total_trips, 
                           completed_trips=completed_trips,
                           total_branches=total_branches,
                           total_done_jobs=total_done_jobs,
                           total_running_jobs=total_running_jobs,
                           now_time=now_thai.strftime("%H:%M"),
                           today_date=today_date,
                           current_filter_date=date_filter,
                           prev_date=prev_date,
                           next_date=next_date,
                           trip_last_end_time=trip_last_end_time,
                           line_data_day=line_data_day,
                           line_data_night=line_data_night,
                           late_arrivals_by_po=late_arrivals_by_po,
                           total_late_cars=total_late_cars
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
    
    if new_rows: ws.append_rows(new_rows)
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
                # Check PO, Round, CarNo
                if (row[0] == po_date and 
                    str(row[2]) == str(round_time) and  
                    str(row[3]) == str(car_no)):        
                    rows_to_delete.append(i + 1)
                    
        for row_idx in sorted(rows_to_delete, reverse=True):
            ws.delete_rows(row_idx)
            
        return redirect(url_for('manager_dashboard'))
    except Exception as e: return f"Error: {e}"

@app.route('/export_excel')
def export_excel():
    # 1. Load Data Once
    sheet = get_db()
    raw_jobs = sheet.worksheet('Jobs').get_all_records()
    
    date_filter = request.args.get('date_filter')
    if date_filter:
        jobs = [j for j in raw_jobs if str(j['PO_Date']).strip() == str(date_filter).strip()]
    else:
        jobs = raw_jobs
        
    # 2. Sort Data
    def sort_key_func(job):
        po_date = str(job['PO_Date'])
        car_no_str = str(job['Car_No']).strip()
        round_val = str(job['Round'])
        try: car_no_int = int(car_no_str)
        except ValueError: car_no_int = 99999 
        return (po_date, car_no_int, round_val)

    jobs = sorted(jobs, key=sort_key_func)
    
    # 3. Process Data for Excel & Summary
    export_data = []
    prev_trip_key = None
    
    grouped_jobs_for_summary = []
    current_group = []
    
    for job_index, job in enumerate(jobs):
        current_trip_key = (str(job['PO_Date']), str(job['Car_No']), str(job['Round']), str(job['Driver']))
        is_same = (current_trip_key == prev_trip_key)
        
        # Grouping for Summary
        if current_trip_key != prev_trip_key and prev_trip_key is not None:
            grouped_jobs_for_summary.append(current_group)
            current_group = []
        current_group.append(job)
        
        # Midnight Delay Logic
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
                    
                    if (t_plan - t_act).total_seconds() > 12 * 3600:
                        t_act = t_act + timedelta(days=1)
                    
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
    
    # Styles
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

    # --- 4. Calculate Summary Stats (Fixed Logic) ---
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
        
        # [FIXED] Use any() to check if ANY branch in the trip has timestamp
        if any(j.get('T7_ArriveBranch') for j in group): target['t7'] += 1
        if any(j.get('T8_EndJob') for j in group): target['t8'] += 1

    sum_total = create_counter()
    for k in sum_total: sum_total[k] = sum_day[k] + sum_night[k]

    # --- Write Summary Table ---
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

    # Styles for Summary
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

    # Column Widths
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
    
@app.route('/export_pdf')
def export_pdf():
    # 1. Load Data
    sheet = get_db()
    raw_jobs = sheet.worksheet('Jobs').get_all_records()
    
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

    # 2. Process Data (Delay Logic + Grouping + Summary)
    def create_counter(): return {'total': 0, 't1': 0, 't2': 0, 't3': 0, 't6': 0, 't7': 0, 't8': 0}
    sum_day = create_counter()
    sum_night = create_counter()
    grouped_jobs = []
    current_group = []
    prev_key = None
    
    for job in jobs:
        # Grouping
        curr_key = (str(job['PO_Date']), str(job['Car_No']), str(job['Round']), str(job['Driver']))
        if curr_key != prev_key and prev_key is not None:
            grouped_jobs.append(current_group)
            current_group = []
        current_group.append(job)
        prev_key = curr_key
        
        # Delay Logic
        job['is_late'] = False
        job['delay_msg'] = ""
        t_plan_str = str(job['Round']).strip()
        t_act_str = str(job['T2_StartLoad']).strip()
        
        if t_plan_str and t_act_str:
            try:
                fmt_plan = "%H:%M" if len(t_plan_str) <= 5 else "%H:%M:%S"
                fmt_act = "%H:%M" if len(t_act_str) <= 5 else "%H:%M:%S"
                t_plan = datetime.strptime(t_plan_str, fmt_plan)
                t_act = datetime.strptime(t_act_str, fmt_act)
                if (t_plan - t_act).total_seconds() > 12 * 3600: t_act = t_act + timedelta(days=1)
                if t_act > t_plan:
                    job['is_late'] = True
                    diff = t_act - t_plan
                    total_seconds = diff.total_seconds()
                    hours = int(total_seconds // 3600)
                    minutes = int((total_seconds % 3600) // 60)
                    job['delay_msg'] = f"(ล่าช้า {hours} ชม. {minutes} น.)"
            except: pass
            
    if current_group: grouped_jobs.append(current_group)

    # Calculate Summary
    for group in grouped_jobs:
        if not group: continue
        first_job = group[0]
        
        round_time = str(first_job.get('Round', '')).strip()
        is_day_shift = True
        try:
            hour = int(round_time.split(':')[0])
            if not (6 <= hour <= 18): is_day_shift = False
        except: pass

        target_sum = sum_day if is_day_shift else sum_night
        target_sum['total'] += 1
        if first_job.get('T1_Enter'): target_sum['t1'] += 1
        if first_job.get('T2_StartLoad'): target_sum['t2'] += 1
        if first_job.get('T3_EndLoad'): target_sum['t3'] += 1
        if first_job.get('T6_Exit'): target_sum['t6'] += 1
        
        # [FIXED] Summary Logic with any()
        if any(j.get('T7_ArriveBranch') for j in group): target_sum['t7'] += 1
        if any(j.get('T8_EndJob') for j in group): target_sum['t8'] += 1

    sum_total = create_counter()
    for key in sum_total:
        sum_total[key] = sum_day[key] + sum_night[key]


    # --- Setup PDF ---
    basedir = os.path.abspath(os.path.dirname(__file__))
    font_path = os.path.join(basedir, 'static', 'fonts', 'Sarabun-Regular.ttf')
    logo_path = os.path.join(basedir, 'static', 'mylogo.png') 
    po_date_thai = thai_date_filter(date_filter) if date_filter else "ทั้งหมด"
    print_date = (datetime.now() + timedelta(hours=7)).strftime("%d/%m/%Y %H:%M")

    class PDF(FPDF):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.is_summary_page = False

        def header(self):
            self.add_font('Sarabun', '', font_path, uni=True)
            if self.is_summary_page:
                self.set_font('Sarabun', '', 18)
                self.set_y(25)
                self.cell(0, 15, f'สรุปภาพรวมการจัดส่งสินค้า ประจำวันที่ {po_date_thai}', align='C', new_x="LMARGIN", new_y="NEXT")
                self.ln(5)
            else:
                if os.path.exists(logo_path):
                    self.image(logo_path, x=7, y=8, w=18)
                
                self.set_font('Sarabun', '', 16) 
                self.set_y(10)
                self.cell(0, 8, 'รายงานสรุปการจัดส่งสินค้า (Daily Jobs Report)', align='C', new_x="LMARGIN", new_y="NEXT")
                self.set_font_size(14)
                self.cell(0, 8, 'บริษัท แอลเอ็มที. ทรานสปอร์ต จำกัด', align='C', new_x="LMARGIN", new_y="NEXT")
                self.set_font_size(10)
                self.cell(0, 6, f'วันที่เอกสาร: {po_date_thai} | พิมพ์เมื่อ: {print_date}', align='C', new_x="LMARGIN", new_y="NEXT")
                self.ln(4)

                cols = [12, 32, 38, 18, 56, 16, 35, 16, 16, 22, 22]
                headers = ['คันที่', 'ทะเบียน', 'คนขับ', 'เวลาโหลด', 'ปลายทาง', 'เข้าโรงงาน', 'เริ่มโหลด', 'โหลดเสร็จ', 'ออกโรงงาน', 'ถึงสาขา', 'จบงาน']
                self.set_fill_color(44, 62, 80)
                self.set_text_color(255, 255, 255)
                self.set_font('Sarabun', '', 9) 
                for i, h in enumerate(headers):
                    self.cell(cols[i], 8, h, border=1, align='C', fill=True)
                self.ln()
                self.set_text_color(0, 0, 0)

        def footer(self):
            self.set_y(-15)
            self.set_font('Sarabun', '', 8)
            self.set_text_color(100, 100, 100)
            self.cell(0, 10, 'ข้อมูลจาก: ระบบ LMT. Transport Driver App V.1.02', align='L')
            self.set_x(-30)
            self.cell(0, 10, f'หน้า {self.page_no()}/{{nb}}', align='R')

    pdf = PDF(orientation='L', unit='mm', format='A4')
    pdf.alias_nb_pages()
    pdf.set_margins(7, 10, 7)
    pdf.add_page()
    
    cols = [12, 32, 38, 18, 56, 16, 35, 16, 16, 22, 22]
    
    for group in grouped_jobs:
        group_total_height = 0
        for job in group:
            h = 9
            if job.get('is_late', False): h = 13
            group_total_height += h

        if group_total_height > (pdf.page_break_trigger - pdf.get_y()):
            pdf.add_page()

        for idx, job in enumerate(group):
            is_first_row = (idx == 0)
            is_last_in_group = (idx == len(group) - 1)
            y_top = pdf.get_y()
            pdf.set_fill_color(255, 255, 255)
            
            c_no = str(job['Car_No']) if is_first_row else ""
            plate = str(job['Plate']) if is_first_row else ""
            driver = str(job['Driver']) if is_first_row else ""
            round_t = str(job['Round']) if is_first_row else ""
            branch = str(job['Branch_Name'])
            t1 = str(job['T1_Enter']) if is_first_row else ""
            
            t2_text = ""
            is_late_row = False
            if is_first_row:
                t2_text = str(job['T2_StartLoad'])
                if job['is_late']:
                    t2_text += f"\n{job['delay_msg']}"
                    is_late_row = True
            
            t3 = str(job['T3_EndLoad']) if is_first_row else ""
            t6 = str(job['T6_Exit']) if is_first_row else ""
            t7 = str(job['T7_ArriveBranch'])
            t8 = str(job['T8_EndJob'])

            row_height = 9
            if is_late_row: row_height = 13

            pdf.set_font('Sarabun', '', 8)
            pdf.set_text_color(0, 0, 0)
            pdf.cell(cols[0], row_height, c_no, border='LR', align='C')
            pdf.cell(cols[1], row_height, plate, border='LR', align='C')
            pdf.cell(cols[2], row_height, driver, border='LR', align='L')
            pdf.cell(cols[3], row_height, round_t, border='LR', align='C')
            
            pdf.set_font_size(7) 
            pdf.cell(cols[4], row_height, branch, border='LR', align='L')
            pdf.set_font_size(8) 
            
            pdf.cell(cols[5], row_height, t1, border='LR', align='C')

            current_x = pdf.get_x()
            current_y = pdf.get_y()
            if is_late_row:
                pdf.set_text_color(192, 57, 43)
                pdf.multi_cell(cols[6], row_height/2 if '\n' in t2_text else row_height, t2_text, border='LR', align='C')
                pdf.set_xy(current_x + cols[6], current_y)
                pdf.set_text_color(0, 0, 0)
            else:
                if is_first_row and t2_text: pdf.set_text_color(25, 111, 61)
                pdf.cell(cols[6], row_height, t2_text.split('\n')[0], border='LR', align='C')
                pdf.set_text_color(0, 0, 0)

            pdf.cell(cols[7], row_height, t3, border='LR', align='C')
            pdf.cell(cols[8], row_height, t6, border='LR', align='C')
            
            pdf.set_fill_color(213, 245, 227)
            pdf.cell(cols[9], row_height, t7, border='LR', align='C', fill=True)
            pdf.set_fill_color(250, 219, 216)
            pdf.cell(cols[10], row_height, t8, border='LR', align='C', fill=True)

            pdf.ln()
            
            if is_first_row:
                pdf.set_draw_color(0, 0, 0)
                pdf.set_line_width(0.3)
            else:
                pdf.set_draw_color(200, 200, 200)
                pdf.set_line_width(0.1)
                
            pdf.line(7, y_top, 290, y_top)

            if is_last_in_group:
                y_bottom = pdf.get_y()
                pdf.set_draw_color(0, 0, 0)
                pdf.set_line_width(0.3)
                pdf.line(7, y_bottom, 290, y_bottom)

            pdf.set_draw_color(0, 0, 0)
            pdf.set_line_width(0.2)

    # --- Summary Page ---
    pdf.is_summary_page = True
    pdf.add_page()
    
    sum_headers = ['รอบงาน', 'จำนวนเที่ยว', 'เข้าโรงงาน', 'เข้าโหลด', 'โหลดเสร็จ', 'ออกโรงงาน', 'ถึงสาขา', 'จบงาน']
    sum_cols = [45, 25, 25, 25, 25, 25, 25, 25] 
    total_table_width = sum(sum_cols)
    start_x = (297 - total_table_width) / 2 
    
    COLOR_HEADER_BG = (44, 62, 80)
    COLOR_HEADER_TXT = (255, 255, 255)
    COLOR_ROW_DAY_BG = (255, 255, 255)
    COLOR_ROW_NIGHT_BG = (242, 243, 244)
    COLOR_TOTAL_BG = (213, 245, 227)
    COLOR_TOTAL_TXT = (25, 111, 61)
    COLOR_BORDER = (189, 195, 199)

    def draw_sum_row(label, data, row_type='normal'):
        pdf.set_x(start_x)
        if row_type == 'header':
            pdf.set_fill_color(*COLOR_HEADER_BG)
            pdf.set_text_color(*COLOR_HEADER_TXT)
            pdf.set_font('Sarabun', '', 12)
            pdf.set_draw_color(*COLOR_HEADER_BG)
        elif row_type == 'total':
            pdf.set_fill_color(*COLOR_TOTAL_BG)
            pdf.set_text_color(*COLOR_TOTAL_TXT)
            pdf.set_font('Sarabun', '', 12)
            pdf.set_draw_color(*COLOR_BORDER)
        elif row_type == 'night':
            pdf.set_fill_color(*COLOR_ROW_NIGHT_BG)
            pdf.set_text_color(0, 0, 0)
            pdf.set_font('Sarabun', '', 11)
            pdf.set_draw_color(*COLOR_BORDER)
        else:
            pdf.set_fill_color(*COLOR_ROW_DAY_BG)
            pdf.set_text_color(0, 0, 0)
            pdf.set_font('Sarabun', '', 11)
            pdf.set_draw_color(*COLOR_BORDER)

        row_h = 12
        pdf.cell(sum_cols[0], row_h, label, border=1, align='C', fill=True)
        
        vals = []
        if row_type == 'header': vals = data
        else: vals = [str(data['total']), str(data['t1']), str(data['t2']), str(data['t3']), str(data['t6']), str(data['t7']), str(data['t8'])]

        for i, val in enumerate(vals):
            pdf.cell(sum_cols[i+1], row_h, val, border=1, align='C', fill=True)
        pdf.ln()

    draw_sum_row("รอบงาน", sum_headers[1:], row_type='header')
    draw_sum_row("รอบกลางวัน", sum_day, row_type='day')
    draw_sum_row("รอบกลางคืน", sum_night, row_type='night')
    draw_sum_row("รวมทั้งหมด", sum_total, row_type='total')
    
    pdf.ln(5)
    pdf.set_x(start_x)
    pdf.set_text_color(127, 140, 141)
    pdf.set_font('Sarabun', '', 9)
    pdf.cell(0, 5, "* ข้อมูลนับจากจำนวนเที่ยวรถที่มีการบันทึกเวลาในแต่ละขั้นตอนจริง", align='L')

    pdf_bytes = pdf.output()
    filename = f"Report_{date_filter if date_filter else 'All'}.pdf"
    return send_file(io.BytesIO(pdf_bytes), mimetype='application/pdf', as_attachment=True, download_name=filename)
    
@app.route('/export_pdf_summary')
def export_pdf_summary():
    # 1. Load Data Once
    sheet = get_db()
    raw_jobs = sheet.worksheet('Jobs').get_all_records()
    
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

    # 2. Process Data
    def create_counter(): return {'count':0, 't1':0, 't2':0, 't3':0, 't4':0, 't5':0, 't6':0, 't7':0, 't8':0}
    sum_day = create_counter()
    sum_night = create_counter()
    grouped_jobs = []
    current_group = []
    prev_key = None

    for job in jobs:
        curr_key = (str(job['PO_Date']), str(job['Car_No']), str(job['Round']), str(job['Driver']))
        if curr_key != prev_key and prev_key is not None:
            grouped_jobs.append(current_group)
            current_group = []
        current_group.append(job)
        prev_key = curr_key
        
        # Delay Logic
        job['is_late'] = False
        t_plan_str = str(job['Round']).strip()
        t_act_str = str(job['T2_StartLoad']).strip()
        if t_plan_str and t_act_str:
            try:
                fmt_plan = "%H:%M" if len(t_plan_str) <= 5 else "%H:%M:%S"
                fmt_act = "%H:%M" if len(t_act_str) <= 5 else "%H:%M:%S"
                t_plan = datetime.strptime(t_plan_str, fmt_plan)
                t_act = datetime.strptime(t_act_str, fmt_act)
                if (t_plan - t_act).total_seconds() > 12 * 3600: t_act += timedelta(days=1)
                if t_act > t_plan: job['is_late'] = True
            except: pass
            
    if current_group: grouped_jobs.append(current_group)

    # Calculate Summary
    for group in grouped_jobs:
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
        
        # [FIXED] Summary Logic
        if any(j.get('T7_ArriveBranch') for j in group): target['t7'] += 1 
        if any(j.get('T8_EndJob') for j in group): target['t8'] += 1       

    sum_total = create_counter()
    for k in sum_total: sum_total[k] = sum_day[k] + sum_night[k]


    # --- 3. Setup PDF ---
    basedir = os.path.abspath(os.path.dirname(__file__))
    font_path = os.path.join(basedir, 'static', 'fonts', 'Sarabun-Regular.ttf')
    logo_path = os.path.join(basedir, 'static', 'mylogo.png') 
    po_date_thai = thai_date_filter(date_filter) if date_filter else "ทั้งหมด"
    print_date = (datetime.now() + timedelta(hours=7)).strftime("%d/%m/%Y %H:%M")

    COLS = [10, 18, 27, 13, 50, 13, 13, 13, 13, 13, 13]
    HEADERS = ['คันที่', 'ทะเบียน', 'คนขับ', 'เวลาโหลด', 'ปลายทาง', 'เข้าโรงงาน', 'เริ่มโหลด', 'โหลดเสร็จ', 'ออกโรงงาน', 'ถึงสาขา', 'จบงาน']

    class PDFSummary(FPDF):
        def header(self):
            self.add_font('Sarabun', '', font_path, uni=True)
            if os.path.exists(logo_path):
                self.image(logo_path, x=7, y=6, w=10)
            self.set_font('Sarabun', '', 12) 
            self.set_y(6)
            self.cell(0, 8, 'สรุปรายงานการจัดส่งสินค้า (Compact View)', align='C', new_x="LMARGIN", new_y="NEXT")
            self.set_font_size(10)
            self.cell(0, 7, 'บริษัท แอลเอ็มที. ทรานสปอร์ต จำกัด', align='C', new_x="LMARGIN", new_y="NEXT")
            self.set_font_size(8)
            self.cell(0, 6, f'วันที่เอกสาร: {po_date_thai} | พิมพ์เมื่อ: {print_date}', align='C', new_x="LMARGIN", new_y="NEXT")
            self.ln(3)
            self.set_fill_color(44, 62, 80)
            self.set_text_color(255, 255, 255)
            self.set_draw_color(100, 100, 100)
            self.set_font('Sarabun', '', 7)
            for i, h in enumerate(HEADERS):
                self.cell(COLS[i], 7, h, border=1, align='C', fill=True)
            self.ln()
            self.set_text_color(0, 0, 0)
            self.set_draw_color(200, 200, 200)

        def footer(self):
            self.set_y(-10)
            self.set_font('Sarabun', '', 6)
            self.set_text_color(150)
            self.cell(0, 10, f'หน้า {self.page_no()}/{{nb}}', align='R')

    pdf = PDFSummary(orientation='P', unit='mm', format='A4')
    pdf.alias_nb_pages()
    pdf.set_margins(7, 7, 7)
    pdf.add_page()
    
    group_count = 0
    for group in grouped_jobs:
        if group_count % 2 == 0: pdf.set_fill_color(255, 255, 255) 
        else: pdf.set_fill_color(245, 247, 249) 
        group_count += 1
        
        for idx, job in enumerate(group):
            is_first_row = (idx == 0)
            is_last_in_group = (idx == len(group) - 1)
            
            c_no = str(job['Car_No']) if is_first_row else ""
            plate = str(job['Plate']) if is_first_row else ""
            driver = str(job['Driver']) if is_first_row else ""
            round_t = str(job['Round']) if is_first_row else ""
            branch = str(job['Branch_Name'])
            t1 = str(job['T1_Enter']) if is_first_row else ""
            
            t2 = ""
            is_late_row = False
            if is_first_row:
                t2 = str(job['T2_StartLoad'])
                if job['is_late']: is_late_row = True

            t3 = str(job['T3_EndLoad']) if is_first_row else ""
            t6 = str(job['T6_Exit']) if is_first_row else ""
            t7 = str(job['T7_ArriveBranch'])
            t8 = str(job['T8_EndJob'])

            row_height = 5.8
            if pdf.get_y() + row_height > pdf.page_break_trigger:
                pdf.add_page()
                if (group_count - 1) % 2 == 0: pdf.set_fill_color(255, 255, 255)
                else: pdf.set_fill_color(245, 247, 249)

            pdf.set_font('Sarabun', '', 7)
            pdf.set_draw_color(180, 180, 180) 
            
            pdf.cell(COLS[0], row_height, c_no, border=1, align='C', fill=True)
            pdf.cell(COLS[1], row_height, plate, border=1, align='C', fill=True)
            if pdf.get_string_width(driver) > COLS[2] - 2:
                 while pdf.get_string_width(driver + "..") > COLS[2] - 2 and len(driver) > 0:
                     driver = driver[:-1]
                 driver += ".."
            pdf.cell(COLS[2], row_height, driver, border=1, align='L', fill=True)
            pdf.cell(COLS[3], row_height, round_t, border=1, align='C', fill=True)
            
            if pdf.get_string_width(branch) > COLS[4] - 2:
                 while pdf.get_string_width(branch + "..") > COLS[4] - 2 and len(branch) > 0:
                     branch = branch[:-1]
                 branch += ".."
            pdf.cell(COLS[4], row_height, branch, border=1, align='L', fill=True)
            
            pdf.cell(COLS[5], row_height, t1, border=1, align='C', fill=True)

            if is_late_row: pdf.set_text_color(192, 57, 43) 
            elif t2: pdf.set_text_color(39, 174, 96)
            pdf.cell(COLS[6], row_height, t2, border=1, align='C', fill=True)
            pdf.set_text_color(0, 0, 0)

            pdf.cell(COLS[7], row_height, t3, border=1, align='C', fill=True)
            pdf.cell(COLS[8], row_height, t6, border=1, align='C', fill=True)
            
            base_r, base_g, base_b = (255, 255, 255) if (group_count - 1) % 2 == 0 else (245, 247, 249)
            pdf.set_fill_color(240, 253, 244)
            pdf.cell(COLS[9], row_height, t7, border=1, align='C', fill=True)
            pdf.set_fill_color(254, 242, 242)
            pdf.cell(COLS[10], row_height, t8, border=1, align='C', fill=True)
            
            pdf.set_fill_color(base_r, base_g, base_b)
            pdf.ln()
            
            if is_last_in_group:
                y_curr = pdf.get_y()
                pdf.set_draw_color(0, 0, 0)
                pdf.set_line_width(0.3)
                pdf.line(7, y_curr, 203, y_curr) 
                pdf.set_line_width(0.2)
                pdf.set_draw_color(180, 180, 180)

    # --- Summary Table ---
    pdf.ln(5)
    if pdf.get_y() + 30 > pdf.page_break_trigger:
        pdf.add_page()
    
    SUM_HEADERS = ['รอบงาน', 'จำนวน', 'เข้าโรงงาน', 'เริ่มโหลด', 'โหลดเสร็จ', 'ยื่นเอกสาร', 'รับเอกสาร', 'ออกโรงงาน', 'ถึงสาขา', 'จบงาน']
    SUM_COLS = [20, 16, 20, 20, 20, 20, 20, 20, 20, 20]
    
    pdf.set_fill_color(44, 62, 80)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font('Sarabun', '', 6)
    for i, h in enumerate(SUM_HEADERS):
        pdf.cell(SUM_COLS[i], 8, h, border=1, align='C', fill=True)
    pdf.ln()
    
    def draw_sum_row(label, data, is_total=False):
        if is_total: pdf.set_fill_color(255, 255, 0)
        else: pdf.set_fill_color(255, 255, 255)
        pdf.set_text_color(0, 0, 0)
        pdf.set_font('Sarabun', '', 7)
        pdf.cell(SUM_COLS[0], 8, label, border=1, align='C', fill=True)
        vals = [data['count'], data['t1'], data['t2'], data['t3'], data['t4'], data['t5'], data['t6'], data['t7'], data['t8']]
        for i, v in enumerate(vals):
            pdf.cell(SUM_COLS[i+1], 8, str(v), border=1, align='C', fill=True)
        pdf.ln()

    draw_sum_row('กลางวัน', sum_day)
    draw_sum_row('กลางคืน', sum_night)
    draw_sum_row('รวม', sum_total, is_total=True)

    pdf_bytes = pdf.output()
    filename = f"Summary_{date_filter if date_filter else 'All'}.pdf"
    return send_file(io.BytesIO(pdf_bytes), mimetype='application/pdf', as_attachment=True, download_name=filename) 

@app.route('/tracking')
def customer_view():
    sheet = get_db()
    raw_jobs = sheet.worksheet('Jobs').get_all_records()
    all_dates = sorted(list(set([str(j['PO_Date']).strip() for j in raw_jobs])), reverse=True)
    
    date_filter = request.args.get('date_filter')
    now_thai = datetime.now() + timedelta(hours=7)
    if not date_filter: date_filter = now_thai.strftime("%Y-%m-%d")

    jobs = [j for j in raw_jobs if str(j['PO_Date']).strip() == str(date_filter).strip()]
    
    # Pagination Logic
    try:
        current_date_obj = datetime.strptime(date_filter, "%Y-%m-%d")
        prev_date = (current_date_obj - timedelta(days=1)).strftime("%Y-%m-%d")
        next_date = (current_date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
    except ValueError:
        prev_date = date_filter
        next_date = date_filter

    # Stats Logic
    jobs_by_trip_key = {}
    total_done_jobs = 0
    total_branches = len(jobs)
    
    # Merged Loop for Stats & Delay
    for job in jobs:
        # A. Stats
        trip_key = (str(job['PO_Date']), str(job['Car_No']), str(job['Round']))
        if trip_key not in jobs_by_trip_key: jobs_by_trip_key[trip_key] = []
        jobs_by_trip_key[trip_key].append(job)
        if job['Status'] == 'Done': total_done_jobs += 1
            
        # B. Delay Logic
        job['is_late'] = False
        job['delay_tooltip'] = ""
        t_plan_str = str(job.get('Round', '')).strip()
        t_act_str = str(job.get('T2_StartLoad', '')).strip()
        
        if t_plan_str and t_act_str:
            try:
                fmt_plan = "%H:%M" if len(t_plan_str) <= 5 else "%H:%M:%S"
                fmt_act = "%H:%M" if len(t_act_str) <= 5 else "%H:%M:%S"
                t_plan = datetime.strptime(t_plan_str, fmt_plan)
                t_act = datetime.strptime(t_act_str, fmt_act)
                if (t_plan - t_act).total_seconds() > 12 * 3600: t_act = t_act + timedelta(days=1)
                
                if t_act > t_plan:
                    job['is_late'] = True
                    diff = t_act - t_plan
                    total_seconds = diff.total_seconds()
                    hours = int(total_seconds // 3600)
                    minutes = int((total_seconds % 3600) // 60)
                    job['delay_tooltip'] = f"ล่าช้า {hours} ชม. {minutes} น."
                else:
                    job['delay_tooltip'] = "เข้าโหลดตรงตามเวลา"
            except (ValueError, TypeError): pass
            
    completed_trips = 0
    for trip_key, job_list in jobs_by_trip_key.items():
        if all(job['Status'] == 'Done' for job in job_list): completed_trips += 1
            
    total_trips = len(jobs_by_trip_key)
    total_running_jobs = total_branches - total_done_jobs

    def sort_key_func(job):
        car_no_str = str(job['Car_No']).strip()
        try: car_no_int = int(car_no_str)
        except ValueError: car_no_int = 99999 
        return (car_no_int)

    jobs = sorted(jobs, key=sort_key_func)
    
    return render_template('customer_view.html', 
                           jobs=jobs, all_dates=all_dates, current_date=date_filter,
                           total_trips=total_trips, completed_trips=completed_trips,
                           total_branches=total_branches, total_done_jobs=total_done_jobs,
                           total_running_jobs=total_running_jobs,
                           prev_date=prev_date, next_date=next_date)

@app.route('/driver')
def driver_select():
    sheet = get_db()
    drivers = sheet.worksheet('Drivers').col_values(1)[1:]
    all_jobs = sheet.worksheet('Jobs').get_all_records() # Load once
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
    raw_data = sheet.worksheet('Jobs').get_all_records()
    my_jobs = []
    
    # Filter in Python
    for i, job in enumerate(raw_data): 
        if job['Driver'] == driver_name and job['Status'] != 'Done':
            job['row_id'] = i + 2
            my_jobs.append(job)
            
    def sort_key_func(job):
        return (str(job['PO_Date']), str(job.get('Load_Date', '')), str(job['Round']))
    my_jobs = sorted(my_jobs, key=sort_key_func)
    
    now_thai = datetime.now() + timedelta(hours=7)
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
        except Exception as e:
            job['smart_title'] = f"เวลา {job['Round']}"
            job['smart_detail'] = job['PO_Date']
            job['ui_class'] = {'bg': 'bg-gray-50', 'text': 'text-gray-500', 'icon': 'fa-clock'}
            job['po_label'] = ""

    return render_template('driver_tasks.html', name=driver_name, jobs=my_jobs, today_date=today_date_str)

@app.route('/update_status', methods=['POST'])
def update_status():
    row_id_target = int(request.form['row_id'])
    step = request.form['step']
    driver_name = request.form['driver_name']
    lat = request.form.get('lat', '')
    long = request.form.get('long', '')
    location_str = f"{lat},{long}" if lat and long else ""
    current_time = (datetime.now() + timedelta(hours=7)).strftime("%H:%M")
    
    sheet = get_db()
    ws = sheet.worksheet('Jobs')
    
    time_col_map = {'1': 8, '2': 9, '3': 10, '4': 11, '5': 12, '6': 13, '7': 14, '8': 15}
    loc_col_map = {'1': 17, '2': 18, '3': 19, '4': 20, '5': 21, '6': 22, '7': 23, '8': 24}
    time_col = time_col_map.get(step)
    loc_col = loc_col_map.get(step)
    updates = []

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
                updates.append({'range': cell_coord_time, 'values': [[current_time]]})
                if location_str:
                    cell_coord_loc = gspread.utils.rowcol_to_a1(current_row_id, loc_col)
                    updates.append({'range': cell_coord_loc, 'values': [[location_str]]})
        if updates: ws.batch_update(updates)

    elif step in ['7', '8']:
        cell_coord_time = gspread.utils.rowcol_to_a1(row_id_target, time_col)
        updates.append({'range': cell_coord_time, 'values': [[current_time]]})
        if location_str:
            cell_coord_loc = gspread.utils.rowcol_to_a1(row_id_target, loc_col)
            updates.append({'range': cell_coord_loc, 'values': [[location_str]]})
        if updates: ws.batch_update(updates)

    if step == '8': 
        ws.update_cell(row_id_target, 16, "Done")
        
    return redirect(url_for('driver_tasks', name=driver_name))

@app.route('/')
def index(): return render_template('index.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True)