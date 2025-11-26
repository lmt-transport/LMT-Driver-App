from flask import Flask, render_template, request, redirect, url_for, session, send_file
from flask_cors import CORS
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
from datetime import datetime
import pandas as pd
import io

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
    
    sheet = get_db()
    raw_jobs = sheet.worksheet('Jobs').get_all_records()
    drivers = sheet.worksheet('Drivers').get_all_records()

    # 1. Determine Date Filter
    date_filter = request.args.get('date_filter')
    now_thai = datetime.now() + timedelta(hours=7)
    today_date = now_thai.strftime("%Y-%m-%d")
    
    if not date_filter:
        date_filter = today_date

    # 2. Filter Jobs by Date
    filtered_jobs = [j for j in raw_jobs if str(j['PO_Date']).strip() == str(date_filter).strip()]

    # 3. Calculate Stats
    jobs_by_trip_key = {}
    total_done_jobs = 0
    total_branches = len(filtered_jobs)
    
    trip_last_end_time = {} 

    for job in filtered_jobs:
        trip_key = (str(job['PO_Date']), str(job['Car_No']), str(job['Round']))
        if trip_key not in jobs_by_trip_key:
            jobs_by_trip_key[trip_key] = []
        jobs_by_trip_key[trip_key].append(job)
        
        if job['Status'] == 'Done':
            total_done_jobs += 1

    completed_trips = 0
    for trip_key, job_list in jobs_by_trip_key.items():
        is_trip_done = all(job['Status'] == 'Done' for job in job_list)
        if is_trip_done:
            completed_trips += 1
            last_end_time = max([j['T8_EndJob'] for j in job_list if j['T8_EndJob']], default="")
            trip_last_end_time[trip_key] = last_end_time
        else:
            trip_last_end_time[trip_key] = ""

    total_trips = len(jobs_by_trip_key)
    total_running_jobs = total_branches - total_done_jobs

    # 4. Sorting
    def sort_key_func(job):
        po_date = str(job['PO_Date'])
        car_no_str = str(job['Car_No']).strip()
        round_val = str(job['Round'])
        try: car_no_int = int(car_no_str)
        except ValueError: car_no_int = 99999 
        return (po_date, car_no_int, round_val)

    filtered_jobs = sorted(filtered_jobs, key=sort_key_func)
    
    # 5. Pagination Dates
    try:
        current_date_obj = datetime.strptime(date_filter, "%Y-%m-%d")
        prev_date = (current_date_obj - timedelta(days=1)).strftime("%Y-%m-%d")
        next_date = (current_date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
    except ValueError:
        prev_date = date_filter
        next_date = date_filter
    
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
                           trip_last_end_time=trip_last_end_time
                           )

@app.route('/create_job', methods=['POST'])
def create_job():
    if 'user' not in session: return redirect(url_for('manager_login'))
    sheet = get_db()
    ws = sheet.worksheet('Jobs')
    
    po_date = request.form['po_date']
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
            row = [po_date, round_time, car_no, driver_name, plate, branch, "", "", "", "", "", "", "", "", "New", "", "", "", "", "", "", "", ""]
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
                if (row[0] == po_date and str(row[1]) == str(round_time) and str(row[2]) == str(car_no)):
                    rows_to_delete.append(i + 1)
        for row_idx in sorted(rows_to_delete, reverse=True):
            ws.delete_rows(row_idx)
        return redirect(url_for('manager_dashboard'))
    except Exception as e: return f"Error: {e}"

@app.route('/export_excel')
def export_excel():
    # ... (ส่วนเตรียมข้อมูลจาก DB) ...
    sheet = get_db()
    raw_jobs = sheet.worksheet('Jobs').get_all_records()
    
    date_filter = request.args.get('date_filter')
    if date_filter:
        jobs = [j for j in raw_jobs if str(j['PO_Date']).strip() == str(date_filter).strip()]
    else:
        jobs = raw_jobs
        
    # Sort Data
    def sort_key_func(job):
        po_date = str(job['PO_Date'])
        car_no_str = str(job['Car_No']).strip()
        try: car_no_int = int(car_no_str)
        except ValueError: car_no_int = 99999 
        return (po_date, car_no_int)

    jobs = sorted(jobs, key=sort_key_func)
    
    export_data = []
    prev_trip_key = None
    
    for job in jobs:
        current_trip_key = (str(job['PO_Date']), str(job['Car_No']), str(job['Round']), str(job['Driver']))
        is_same = (current_trip_key == prev_trip_key)
        
        # --- Logic คำนวณความล่าช้า (คงเดิม) ---
        t2_display = job['T2_StartLoad']
        if not is_same: 
            try:
                plan_time_str = str(job['Round']).strip()
                actual_time_str = str(job['T2_StartLoad']).strip()
                
                if plan_time_str and actual_time_str:
                    fmt = "%H:%M" if len(plan_time_str) <= 5 else "%H:%M:%S"
                    fmt_act = "%H:%M" if len(actual_time_str) <= 5 else "%H:%M:%S"
                    
                    t_plan = datetime.strptime(plan_time_str, fmt)
                    t_act = datetime.strptime(actual_time_str, fmt_act)
                    
                    if t_act > t_plan:
                        diff = t_act - t_plan
                        total_seconds = diff.total_seconds()
                        hours = int(total_seconds // 3600)
                        minutes = int((total_seconds % 3600) // 60)
                        
                        # เพิ่มข้อความล่าช้า (ใช้ตรวจสอบเงื่อนไขสีแดงทีหลัง)
                        delay_msg = f" (ล่าช้า {hours} ชม./{minutes} น.)"
                        t2_display = f"{actual_time_str}{delay_msg}"
            except (ValueError, TypeError):
                pass 
        # ----------------------------------------

        formatted_date = job['PO_Date']
        try:
            date_obj = datetime.strptime(str(job['PO_Date']).strip(), "%Y-%m-%d")
            formatted_date = date_obj.strftime("%d/%m/%Y")
        except ValueError:
            pass
            
        row = {
            'ลำดับรถ': "" if is_same else job['Car_No'],
            'PO Date': "" if is_same else formatted_date,
            'เวลาโหลด': "" if is_same else job['Round'], 
            'คนขับ': "" if is_same else job['Driver'],
            'ปลายทาง (สาขา)': job['Branch_Name'],
            'ทะเบียนรถ': "" if is_same else job['Plate'],
            '1.เข้าโรงงาน': "" if is_same else job['T1_Enter'],
            '2.เริ่มโหลด': "" if is_same else t2_display, 
            '3.โหลดเสร็จ': "" if is_same else job['T3_EndLoad'],
            '4.ยื่นเอกสาร': "" if is_same else job['T4_SubmitDoc'],
            '5.รับเอกสาร': "" if is_same else job['T5_RecvDoc'],
            '6.ออกโรงงาน': "" if is_same else job['T6_Exit'],
            '7.ถึงสาขา': job['T7_ArriveBranch'],
            '8.จบงาน': job['T8_EndJob']
        }
        export_data.append(row)
        prev_trip_key = current_trip_key

    df = pd.DataFrame(export_data)
    
    # --- ส่วนการจัดรูปแบบ Excel ---
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Report')
    
    output.seek(0)
    wb = load_workbook(output)
    ws = wb.active
    
    # Styles
    font_header = Font(name='Cordia New', size=14, bold=True, color='FFFFFF') 
    
    side_thin = Side(border_style="thin", color="000000")
    side_none = Side(border_style=None) 
    border_header = Border(top=side_thin, bottom=side_thin, left=side_thin, right=side_thin)
    
    align_center = Alignment(horizontal='center', vertical='top', wrap_text=True)
    align_left = Alignment(horizontal='left', vertical='top', wrap_text=True)
    
    fill_header = PatternFill(start_color='2E4053', end_color='2E4053', fill_type='solid')
    fill_white = PatternFill(start_color='FFFFFF', end_color='FFFFFF', fill_type='solid')
    fill_blue_light = PatternFill(start_color='EBF5FB', end_color='EBF5FB', fill_type='solid')
    fill_green_branch = PatternFill(start_color='D5F5E3', end_color='D5F5E3', fill_type='solid')
    fill_red_end = PatternFill(start_color='FADBD8', end_color='FADBD8', fill_type='solid')

    current_trip_id = None
    is_zebra_active = False

    for row in ws.iter_rows(min_row=1, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
        
        # --- ส่วนหัวตาราง ---
        if row[0].row == 1:
            for cell in row:
                cell.font = font_header
                cell.fill = fill_header
                cell.alignment = align_center
                cell.border = border_header
            continue

        # --- ส่วนเนื้อหา ---
        job_index = row[0].row - 2
        
        is_group_end = False
        if 0 <= job_index < len(jobs):
            job_data = jobs[job_index]
            if job_index == len(jobs) - 1:
                is_group_end = True
            else:
                next_job = jobs[job_index + 1]
                if (str(job_data['PO_Date']) != str(next_job['PO_Date'])) or \
                   (str(job_data['Car_No']) != str(next_job['Car_No'])):
                    is_group_end = True

            this_trip_key = (str(job_data['PO_Date']), str(job_data['Car_No']), str(job_data['Round']))
            if this_trip_key != current_trip_id:
                is_zebra_active = not is_zebra_active
                current_trip_id = this_trip_key

        row_fill = fill_blue_light if is_zebra_active else fill_white

        current_border = Border(
            left=side_thin, 
            right=side_thin, 
            top=side_none, 
            bottom=side_thin if is_group_end else side_none
        )

        for cell in row:
            col_name = ws.cell(row=1, column=cell.column).value
            
            # --- กำหนด Font, Bold, และ Color แบบ Dynamic ---
            f_bold = False
            f_color = '000000' # สีดำ (Default)

            # 1. เงื่อนไข Bold
            if col_name in ['7.ถึงสาขา', '8.จบงาน']:
                f_bold = True

            # 2. เงื่อนไขสีตัวอักษร คอลัมน์ '2.เริ่มโหลด'
            if col_name == '2.เริ่มโหลด':
                cell_val_str = str(cell.value) if cell.value else ""
                if "(ล่าช้า" in cell_val_str:
                    f_color = 'C0392B' # สีแดงเข้ม (Dark Red)
                elif cell_val_str.strip() != "":
                    f_color = '196F3D' # สีเขียวเข้ม (Dark Green) สำหรับคันที่ทันเวลา

            # สร้าง Font Object ใหม่สำหรับเซลล์นี้
            cell.font = Font(name='Cordia New', size=14, bold=f_bold, color=f_color)
            
            cell.border = current_border 
            cell.fill = row_fill
            
            # Override สีพื้นหลัง
            if col_name == '7.ถึงสาขา':
                cell.fill = fill_green_branch
            elif col_name == '8.จบงาน':
                cell.fill = fill_red_end
            
            # Alignment
            if col_name in ['คนขับ', 'ปลายทาง (สาขา)', 'ทะเบียนรถ']:
                cell.alignment = align_left
            else:
                cell.alignment = align_center

    # --- ปรับความกว้างคอลัมน์ ---
    for column_cells in ws.columns:
        col_letter = get_column_letter(column_cells[0].column)
        col_header = column_cells[0].value
        
        # เงื่อนไขความกว้าง
        if col_header == '2.เริ่มโหลด':
            ws.column_dimensions[col_letter].width = 19.00
        else:
            # Auto fit สำหรับคอลัมน์อื่นๆ
            length = 0
            for cell in column_cells:
                val = str(cell.value) if cell.value else ""
                lines = val.split('\n')
                longest_line = max(len(line) for line in lines) if lines else 0
                if longest_line > length:
                    length = longest_line
            ws.column_dimensions[col_letter].width = min(length + 4, 50)

    ws.auto_filter.ref = ws.dimensions

    final_output = io.BytesIO()
    wb.save(final_output)
    final_output.seek(0)
    
    filename = f"Report_{date_filter if date_filter else 'All'}.xlsx"
    return send_file(final_output, download_name=filename, as_attachment=True)
    
@app.route('/print_report')
def print_report():
    # อนุญาตให้เข้าถึงได้ทั่วไป (เหมือน export_excel)
    
    sheet = get_db()
    raw_jobs = sheet.worksheet('Jobs').get_all_records()
    
    date_filter = request.args.get('date_filter')
    if date_filter:
        # กรองข้อมูลตามวันที่ที่ส่งมา
        jobs = [j for j in raw_jobs if str(j['PO_Date']).strip() == str(date_filter).strip()]
    else:
        jobs = raw_jobs
        
    # เรียงลำดับข้อมูล (เหมือนหน้าอื่นๆ)
    def sort_key_func(job):
        po_date = str(job['PO_Date'])
        car_no_str = str(job['Car_No']).strip()
        round_val = str(job['Round'])
        try: car_no_int = int(car_no_str)
        except ValueError: car_no_int = 99999 
        return (po_date, car_no_int, round_val)

    jobs = sorted(jobs, key=sort_key_func)
    
    # เตรียมวันที่สำหรับแสดงหัวกระดาษ
    print_date = datetime.now().strftime("%d/%m/%Y %H:%M")
    
    # แปลงวันที่ PO เป็นไทย
    po_date_thai = ""
    if date_filter:
        po_date_thai = thai_date_filter(date_filter)

    return render_template('print_report.html', 
                           jobs=jobs, 
                           po_date=po_date_thai,
                           print_date=print_date)

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
    for job in jobs:
        trip_key = (str(job['PO_Date']), str(job['Car_No']), str(job['Round']))
        if trip_key not in jobs_by_trip_key: jobs_by_trip_key[trip_key] = []
        jobs_by_trip_key[trip_key].append(job)
        if job['Status'] == 'Done': total_done_jobs += 1
            
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
    all_jobs = sheet.worksheet('Jobs').get_all_records()
    driver_pending_trips = {name: set() for name in drivers}
    for job in all_jobs:
        if job['Status'] != 'Done' and job['Driver'] in driver_pending_trips:
            trip_key = (str(job['PO_Date']), str(job['Round']), str(job['Car_No']))
            driver_pending_trips[job['Driver']].add(trip_key)
    driver_counts = {name: len(trips) for name, trips in driver_pending_trips.items()}
    return render_template('driver_select.html', drivers=drivers, driver_counts=driver_counts)

@app.route('/driver/tasks', methods=['GET'])
def driver_tasks():
    driver_name = request.args.get('name')
    if not driver_name: return redirect(url_for('driver_select'))
        
    sheet = get_db()
    raw_data = sheet.worksheet('Jobs').get_all_records()
    
    my_jobs = []
    for i, job in enumerate(raw_data): 
        if job['Driver'] == driver_name and job['Status'] != 'Done':
            job['row_id'] = i + 2
            my_jobs.append(job)
            
    # --- แก้ไข Logic การเรียงลำดับตรงนี้ ---
    def sort_key_func(job):
        po_date = str(job['PO_Date'])
        car_no_str = str(job['Car_No']).strip()
        round_val = str(job['Round'])
        
        try: car_no_int = int(car_no_str)
        except ValueError: car_no_int = 99999 
        
        return (po_date, car_no_int, round_val) 
            
    my_jobs = sorted(my_jobs, key=sort_key_func)
    # -------------------------------------
    
    now_thai = datetime.now() + timedelta(hours=7)
    today_date = now_thai.strftime("%Y-%m-%d")
    
    return render_template('driver_tasks.html', name=driver_name, jobs=my_jobs, today_date=today_date)

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
    time_col_map = {'1': 7, '2': 8, '3': 9, '4': 10, '5': 11, '6': 12, '7': 13, '8': 14}
    loc_col_map = {'1': 16, '2': 17, '3': 18, '4': 19, '5': 20, '6': 21, '7': 22, '8': 23}
    time_col = time_col_map.get(step)
    loc_col = loc_col_map.get(step)
    updates = []

    if step in ['1', '2', '3', '4', '5', '6']:
        target_row_data = ws.row_values(row_id_target)
        if len(target_row_data) < 3: return redirect(url_for('driver_tasks', name=driver_name))
        target_po = target_row_data[0] 
        target_round = target_row_data[1] 
        target_car = target_row_data[2] 
        all_values = ws.get_all_values()
        for i, row in enumerate(all_values[1:]): 
            current_row_id = i + 2 
            if (len(row) > 2 and row[0] == target_po and row[1] == target_round and row[2] == target_car):
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

    if step == '8': ws.update_cell(row_id_target, 15, "Done")
    return redirect(url_for('driver_tasks', name=driver_name))

@app.route('/')
def index(): return render_template('index.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True)