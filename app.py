from flask import Flask, render_template, request, redirect, url_for, session, send_file
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import pandas as pd
import io
import os
import gspread.utils 
import json # <--- สำคัญ: ต้อง import json

app = Flask(__name__)
app.secret_key = 'lmt_driver_app_secret_key_2024' 

# --- Google Sheet Connection ---
def get_db():
    scope = ["https://spreadsheets.google.com/feeds", 
             'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", 
             "https://www.googleapis.com/auth/drive"]
    
    # 1. อ่าน JSON String จาก Environment Variable GSPREAD_CREDENTIALS
    creds_json = os.environ.get('GSPREAD_CREDENTIALS')
    
    if not creds_json:
        # Fallback สำหรับรันในเครื่องตัวเอง (ถ้าไฟล์ credentials.json มีอยู่)
        return gspread.service_account(filename="credentials.json").open("DriverLogApp")
        
    # 2. แปลง JSON String เป็น Dict และสร้าง Credential object
    creds_dict = json.loads(creds_json)
    
    # gspread เวอร์ชันใหม่สามารถใช้ from_dict ได้
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    sheet = client.open("DriverLogApp") 
    return sheet

# --- Routes: Manager ---

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
        except Exception as e:
            return render_template('login.html', error=f"เกิดข้อผิดพลาดในการเชื่อมต่อ: {str(e)}")
            
    return render_template('login.html')

# ในไฟล์ app.py ค้นหาฟังก์ชัน @app.route('/manager')
@app.route('/manager')
def manager_dashboard():
    if 'user' not in session:
        return redirect(url_for('manager_login'))
    
    sheet = get_db()
    raw_jobs = sheet.worksheet('Jobs').get_all_records()
    drivers = sheet.worksheet('Drivers').get_all_records()

    # 1. กรองข้อมูลตามวันที่
    date_filter = request.args.get('date_filter')
    if date_filter:
        jobs = [j for j in raw_jobs if str(j['PO_Date']).strip() == str(date_filter).strip()]
    else:
        jobs = raw_jobs

    # 2. คำนวณสถิติ
    jobs_by_trip_key = {}
    total_done_jobs = 0
    total_branches = len(jobs)  # <-- FIX 1: คำนวณสาขาทั้งหมด (แถวทั้งหมด)
    
    for job in jobs:
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
            
    total_trips = len(jobs_by_trip_key)
    total_running_jobs = total_branches - total_done_jobs

    # 3. เรียงลำดับข้อมูล
    def sort_key_func(job):
        po_date = str(job['PO_Date'])
        car_no_str = str(job['Car_No']).strip()
        round_val = str(job['Round'])
        try:
            car_no_int = int(car_no_str)
        except ValueError:
            car_no_int = 99999 
            
        return (po_date, car_no_int, round_val)

    jobs = sorted(jobs, key=sort_key_func)
    
    # 4. หาวันที่ทั้งหมด
    all_dates = sorted(list(set([str(j['PO_Date']).strip() for j in raw_jobs])), reverse=True)

    return render_template('manager.html', 
                           jobs=jobs, 
                           drivers=drivers, 
                           all_dates=all_dates, 
                           total_trips=total_trips, 
                           completed_trips=completed_trips,
                           total_branches=total_branches, # <-- FIX 2: ส่งค่าไปยัง Template
                           total_done_jobs=total_done_jobs,
                           total_running_jobs=total_running_jobs
                           )

@app.route('/create_job', methods=['POST'])
def create_job():
    if 'user' not in session:
        return redirect(url_for('manager_login'))
    
    sheet = get_db()
    ws = sheet.worksheet('Jobs')
    
    po_date = request.form['po_date']
    round_time = request.form['round_time']
    car_no = request.form['car_no']
    driver_name = request.form['driver_name']
    
    branches = request.form.getlist('branches') 
    
    # Vlookup ทะเบียนรถ
    drivers_ws = sheet.worksheet('Drivers')
    driver_list = drivers_ws.get_all_records()
    plate = ""
    for d in driver_list:
        if d['Name'] == driver_name:
            plate = d['Plate_License']
            break
            
    # สร้างแถวข้อมูลใหม่ (1 สาขา = 1 แถว)
    new_rows = []
    for branch in branches:
        if branch.strip(): 
            # Row structure: [PO, Round, Car, Driver, Plate, Branch, T1..T8, Status, L1..L8]
            row = [
                po_date, round_time, car_no, driver_name, plate, branch, 
                "", "", "", "", "", "", "", "", "New",  # Time Columns (G-N) & Status (O)
                "", "", "", "", "", "", "", ""          # Location Columns (P-W)
            ]
            new_rows.append(row)
    
    if new_rows:
        ws.append_rows(new_rows)
    
    return redirect(url_for('manager_dashboard'))

@app.route('/export_excel')
def export_excel():
    if 'user' not in session and request.path != '/tracking': # ตรวจสอบสิทธิ์สำหรับ Manager หรือถ้าเรียกจาก Customer View ก็ผ่าน
        return redirect(url_for('manager_login'))
    
    sheet = get_db()
    raw_jobs = sheet.worksheet('Jobs').get_all_records()
    
    date_filter = request.args.get('date_filter')
    if date_filter:
        jobs = [j for j in raw_jobs if str(j['PO_Date']).strip() == str(date_filter).strip()]
    else:
        jobs = raw_jobs
        
    # --- แก้ไขการเรียงลำดับ: แปลง Car_No เป็น int ก่อนเรียง ---
    def sort_key_func(job):
        po_date = str(job['PO_Date'])
        car_no_str = str(job['Car_No']).strip()
        round_val = str(job['Round'])
        
        try:
            car_no_int = int(car_no_str)
        except ValueError:
            car_no_int = 99999 
            
        return (po_date, car_no_int, round_val)

    jobs = sorted(jobs, key=sort_key_func)
    
    # Grouping Logic for Excel
    export_data = []
    prev_trip_key = None
    
    for job in jobs:
        current_trip_key = (str(job['PO_Date']), str(job['Car_No']), str(job['Round']), str(job['Driver']))
        is_same = (current_trip_key == prev_trip_key)
            
        row = {
            'ลำดับรถ': "" if is_same else job['Car_No'],
            'PO Date': "" if is_same else job['PO_Date'],
            'เวลาโหลด': "" if is_same else job['Round'],
            'คนขับ': "" if is_same else job['Driver'], # เพิ่มคอลัมน์คนขับสำหรับ export
            'ปลายทาง (สาขา)': job['Branch_Name'],
            'ทะเบียนรถ': "" if is_same else job['Plate'],
            '1.เข้าโรงงาน': "" if is_same else job['T1_Enter'],
            '2.เริ่มโหลด': "" if is_same else job['T2_StartLoad'],
            '3.โหลดเสร็จ': "" if is_same else job['T3_EndLoad'],
            '4.ยื่นเอกสาร': "" if is_same else job['T4_SubmitDoc'],
            '5.รับเอกสาร': "" if is_same else job['T5_RecvDoc'],
            '6.ออกโรงงาน': "" if is_same else job['T6_Exit'],
            '7.ถึงสาขา': job['T7_ArriveBranch'],
            '8.จบงาน': job['T8_EndJob']
        }
        export_data.append(row)
        
        prev_trip_key = current_trip_key

    # Convert to Excel using Pandas in-memory
    df = pd.DataFrame(export_data)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Report')
        
        # Auto adjust column width (optional)
        ws = writer.sheets['Report']
        for column in ws.columns:
            max_length = 0
            column = [cell for cell in column]
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = (max_length + 2)
            ws.column_dimensions[column[0].column_letter].width = adjusted_width

    output.seek(0)
    filename = f"Report_{date_filter if date_filter else 'All'}.xlsx"
    return send_file(output, download_name=filename, as_attachment=True)

# --- Routes: Customer View ---

# ในไฟล์ app.py ค้นหา @app.route('/tracking') แล้วแทนที่ทั้งหมด
@app.route('/tracking')
def customer_view():
    sheet = get_db()
    raw_jobs = sheet.worksheet('Jobs').get_all_records()
    
    # 1. หาวันที่ทั้งหมดที่มีงานใน Google Sheet
    all_dates = sorted(list(set([str(j['PO_Date']).strip() for j in raw_jobs])), reverse=True)
    
    date_filter = request.args.get('date_filter')
    
    # 2. กำหนดวันที่ Default: ใช้ วันที่ล่าสุดที่มีงาน (สำคัญต่อการแสดงผลครั้งแรก)
    if not date_filter and all_dates:
        date_filter = all_dates[0] 
    elif not date_filter:
        date_filter = datetime.now().strftime("%Y-%m-%d")

    # 3. กรองข้อมูล (Fix สำหรับการแสดงผล)
    jobs = [j for j in raw_jobs if str(j['PO_Date']).strip() == str(date_filter).strip()]
    
    # 4. คำนวณสถิติทั้งหมด (เหมือน Manager Dashboard)
    jobs_by_trip_key = {}
    total_done_jobs = 0
    total_branches = len(jobs)  # สาขาทั้งหมด
    
    for job in jobs:
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
            
    total_trips = len(jobs_by_trip_key)
    total_running_jobs = total_branches - total_done_jobs

    # 5. Sort Jobs (เรียงตามตัวเลขคันที่)
    def sort_key_func(job):
        car_no_str = str(job['Car_No']).strip()
        round_val = str(job['Round'])
        try:
            car_no_int = int(car_no_str)
        except ValueError:
            car_no_int = 99999 
        return (car_no_int, round_val)

    jobs = sorted(jobs, key=sort_key_func)
    
    return render_template('customer_view.html', 
                           jobs=jobs, 
                           all_dates=all_dates, 
                           current_date=date_filter,
                           total_trips=total_trips, 
                           completed_trips=completed_trips,
                           total_branches=total_branches,
                           total_done_jobs=total_done_jobs,
                           total_running_jobs=total_running_jobs)
# --- Routes: Driver ---

@app.route('/driver')
def driver_select():
    sheet = get_db()
    drivers = sheet.worksheet('Drivers').col_values(1)[1:]
    return render_template('driver_select.html', drivers=drivers)

@app.route('/driver/tasks', methods=['GET'])
def driver_tasks():
    driver_name = request.args.get('name')
    if not driver_name:
        return redirect(url_for('driver_select'))
        
    sheet = get_db()
    raw_data = sheet.worksheet('Jobs').get_all_records()
    
    my_jobs = []
    # ดึงค่าพร้อม index เพื่อคำนวณ row_id ที่ถูกต้อง (i+2)
    for i, job in enumerate(raw_data): 
        # T1-T6 ควรจะถูกบันทึกแค่ครั้งเดียว แต่ T7-T8 ต้องบันทึกแยกสาขา
        if job['Driver'] == driver_name and job['Status'] != 'Done':
            job['row_id'] = i + 2
            my_jobs.append(job)
            
    # --- แก้ไขการเรียงลำดับ: แปลง Car_No เป็น int ก่อนเรียง ---
    def sort_key_func(job):
        po_date = str(job['PO_Date'])
        car_no_str = str(job['Car_No']).strip()
        round_val = str(job['Round'])
        
        try:
            car_no_int = int(car_no_str)
        except ValueError:
            car_no_int = 99999 
            
        return (po_date, car_no_int, round_val)
            
    my_jobs = sorted(my_jobs, key=sort_key_func)
            
    return render_template('driver_tasks.html', name=driver_name, jobs=my_jobs)

@app.route('/update_status', methods=['POST'])
def update_status():
    row_id_target = int(request.form['row_id'])
    step = request.form['step']
    
    lat = request.form.get('lat', '')
    long = request.form.get('long', '')
    location_str = f"{lat},{long}" if lat and long else ""
    current_time = datetime.now().strftime("%H:%M")
    
    sheet = get_db()
    ws = sheet.worksheet('Jobs')
    
    time_col_map = {'1': 7, '2': 8, '3': 9, '4': 10, '5': 11, '6': 12, '7': 13, '8': 14}
    loc_col_map = {'1': 16, '2': 17, '3': 18, '4': 19, '5': 20, '6': 21, '7': 22, '8': 23}
    
    time_col = time_col_map.get(step)
    loc_col = loc_col_map.get(step)
    updates = []

    # 1. Factory Steps (T1-T6): Sync ข้อมูลให้ทุกแถวใน Trip
    if step in ['1', '2', '3', '4', '5', '6']:
        
        # ดึงข้อมูลเพื่อระบุ Trip Key: [PO Date, Round, Car No]
        target_row_data = ws.row_values(row_id_target)
        if len(target_row_data) < 3: return redirect(url_for('driver_tasks', name=request.form['driver_name']))

        target_po = target_row_data[0] 
        target_round = target_row_data[1] 
        target_car = target_row_data[2] 
        
        # ดึงค่าทั้งหมดมาหาแถวที่ตรงกัน
        all_values = ws.get_all_values()
        
        for i, row in enumerate(all_values[1:]): 
            current_row_id = i + 2 
            
            # Match Trip Key
            if (len(row) > 2 and 
                row[0] == target_po and 
                row[1] == target_round and 
                row[2] == target_car):
                
                # Update Time
                cell_coord_time = gspread.utils.rowcol_to_a1(current_row_id, time_col)
                updates.append({'range': cell_coord_time, 'values': [[current_time]]})
                
                # Update Location
                if location_str:
                    cell_coord_loc = gspread.utils.rowcol_to_a1(current_row_id, loc_col)
                    updates.append({'range': cell_coord_loc, 'values': [[location_str]]})
        
        if updates:
            ws.batch_update(updates)
            
    # 2. Destination Steps (T7-T8): อัปเดตแค่แถวที่กด
    elif step in ['7', '8']:
        cell_coord_time = gspread.utils.rowcol_to_a1(row_id_target, time_col)
        updates.append({'range': cell_coord_time, 'values': [[current_time]]})
        
        if location_str:
            cell_coord_loc = gspread.utils.rowcol_to_a1(row_id_target, loc_col)
            updates.append({'range': cell_coord_loc, 'values': [[location_str]]})
        
        if updates:
             ws.batch_update(updates)

    # 3. Final Status Update
    if step == '8':
        ws.update_cell(row_id_target, 15, "Done")

    return redirect(url_for('driver_tasks', name=request.form['driver_name']))

if __name__ == '__main__':
    app.run(debug=True)