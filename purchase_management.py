import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime
import time

# --- Database Connection ---
def get_db_connection():
    max_retries = 3
    retry_delay = 1
    
    for attempt in range(max_retries):
        try:
            db_url = st.secrets["db_url"]
            if '?' not in db_url:
                db_url += '?sslmode=require'
            elif 'sslmode' not in db_url:
                db_url += '&sslmode=require'
            conn = psycopg2.connect(db_url)
            return conn
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            raise
        except KeyError:
            st.error("Database URL not found in secrets.")
            st.stop()

# --- Schema Creation ---
def create_purchase_order_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Purchase_Order (
                po_id SERIAL PRIMARY KEY,
                po_number VARCHAR(50) UNIQUE,
                pkid VARCHAR(50) NOT NULL,
                supplier VARCHAR(100),
                order_date DATE,
                order_qty INTEGER,
                eta DATE,
                status VARCHAR(50) DEFAULT 'PO Issued',
                remarks TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_by VARCHAR(50)
            );
        """)
        conn.commit()
    except Exception as e:
        conn.rollback()
        st.error(f"Error creating table: {e}")
    finally:
        conn.close()

# --- Helper Functions ---
def generate_po_number():
    """Generate a new PO Number: PO-YYYYMMDD-XXX"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        today_str = datetime.now().strftime('%Y%m%d')
        prefix = f"PO-{today_str}-"
        
        # Find the max sequence for today
        query = "SELECT po_number FROM Purchase_Order WHERE po_number LIKE %s ORDER BY po_number DESC LIMIT 1"
        cursor.execute(query, (prefix + '%',))
        result = cursor.fetchone()
        
        if result:
            last_po = result[0]
            last_seq = int(last_po.split('-')[-1])
            new_seq = last_seq + 1
        else:
            new_seq = 1
            
        return f"{prefix}{new_seq:03d}"
    except Exception as e:
        st.error(f"Error generating PO number: {e}")
        return f"PO-{datetime.now().strftime('%Y%m%d')}-ERR"
    finally:
        conn.close()

# --- Data Operations ---
def get_purchase_orders():
    conn = get_db_connection()
    try:
        query = """
            SELECT po_id, po_number, pkid, supplier, order_date, order_qty, eta, status, remarks, updated_at 
            FROM Purchase_Order 
            ORDER BY 
                CASE status 
                    WHEN 'PO Issued' THEN 1 
                    WHEN 'In-Transit' THEN 2 
                    WHEN 'Arrived' THEN 3 
                    WHEN 'Obsoleted' THEN 4 
                    ELSE 5 
                END,
                eta ASC
        """
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        st.error(f"Error fetching POs: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def process_bulk_upload(df):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    success_count = 0
    errors = []
    
    try:
        # Generate base PO number logic handled per row to ensure uniqueness? 
        # Or generate one by one. To be safe, generate one by one inside loop.
        
        for index, row in df.iterrows():
            try:
                pkid = str(row['PKID']).strip()
                supplier = str(row['Supplier']).strip() if 'Supplier' in row else None
                order_qty = int(row['Order Qty']) if 'Order Qty' in row else 0
                
                # Handle ETA
                eta = None
                if 'ETA' in row and pd.notna(row['ETA']):
                    eta = pd.to_datetime(row['ETA']).date()
                
                status = row['Status'] if 'Status' in row and pd.notna(row['Status']) else 'PO Issued'
                remarks = row['Remarks'] if 'Remarks' in row and pd.notna(row['Remarks']) else ''
                
                # Generate PO Number
                # Note: Calling DB for every row is slow but safe for sequence. 
                # For bulk, we can optimize but let's stick to safety for now.
                po_number = generate_po_number()
                
                cursor.execute("""
                    INSERT INTO Purchase_Order (po_number, pkid, supplier, order_date, order_qty, eta, status, remarks, updated_at)
                    VALUES (%s, %s, %s, CURRENT_DATE, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """, (po_number, pkid, supplier, order_qty, eta, status, remarks))
                
                conn.commit() # Commit each to ensure PO number sequence is visible for next generation if using DB check
                success_count += 1
                
            except Exception as row_e:
                conn.rollback()
                errors.append(f"Row {index+1} ({pkid}): {row_e}")
                
        return success_count, errors
    except Exception as e:
        return 0, [str(e)]
    finally:
        conn.close()

def update_purchase_order(po_id, col, value):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        query = f"UPDATE Purchase_Order SET {col} = %s, updated_at = CURRENT_TIMESTAMP WHERE po_id = %s"
        cursor.execute(query, (value, po_id))
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        return False
    finally:
        conn.close()

# --- UI Functions ---
def show_purchase_management():
    st.title("💰 자재 구매 관리 (Purchase Management)")
    
    # Ensure table exists
    create_purchase_order_table()
    
    tab1, tab2 = st.tabs(["📊 구매 현황 대시보드", "📂 PO 일괄 등록 (Bulk Upload)"])
    
    # --- Tab 1: Dashboard ---
    with tab1:
        st.subheader("구매 진행 현황")
        
        df = get_purchase_orders()
        
        if not df.empty:
            # Highlight logic
            today = datetime.now().date()
            
            def highlight_row(row):
                status = row['status']
                eta = row['eta']
                
                if status in ['Arrived', 'Obsoleted']:
                    return ['background-color: #e0e0e0; color: #9e9e9e'] * len(row) # Grey out
                
                if pd.isna(eta):
                    return [''] * len(row)
                    
                if eta < today:
                    return ['background-color: #ffcdd2; color: #b71c1c'] * len(row) # Red for delayed
                elif (eta - today).days <= 3:
                    return ['background-color: #fff9c4; color: #f57f17'] * len(row) # Yellow for imminent
                
                return [''] * len(row)

            # Display with Column Config
            st.data_editor(
                df,
                key="po_dashboard_editor",
                use_container_width=True,
                hide_index=True,
                column_config={
                    "po_id": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                    "po_number": st.column_config.TextColumn("발주 번호", disabled=True, width="medium"),
                    "pkid": st.column_config.TextColumn("자재 코드 (PKID)", disabled=True),
                    "supplier": st.column_config.TextColumn("공급사", disabled=True),
                    "order_date": st.column_config.DateColumn("발주 일자", disabled=True),
                    "order_qty": st.column_config.NumberColumn("발주 수량", disabled=True),
                    "eta": st.column_config.DateColumn("입고 예정일 (ETA)"),
                    "status": st.column_config.SelectboxColumn(
                        "진행 상태",
                        options=["PO Issued", "In-Transit", "Arrived", "Obsoleted", "ETC"],
                        required=True,
                        width="medium"
                    ),
                    "remarks": st.column_config.TextColumn("비고", width="large"),
                    "updated_at": st.column_config.DatetimeColumn("최종 수정", format="YYYY-MM-DD HH:mm", disabled=True)
                },
                disabled=["po_id", "po_number", "pkid", "supplier", "order_date", "order_qty", "updated_at"]
            )
            
            # Note: st.data_editor supports editing but handling updates back to DB requires session state management or a save button.
            # For simplicity in this iteration, we assume the user views here. 
            # To make it truly editable, we need to capture `edited_rows` from session state.
            
            # --- Handle Edits ---
            if st.session_state.get("po_dashboard_editor"):
                changes = st.session_state["po_dashboard_editor"].get("edited_rows", {})
                if changes:
                    if st.button("변경 사항 저장 (Save Changes)"):
                        updated_count = 0
                        for idx, change in changes.items():
                            # Get actual PO ID from the original dataframe using the index
                            po_id = df.iloc[idx]['po_id']
                            for col, value in change.items():
                                update_purchase_order(po_id, col, value)
                            updated_count += 1
                        
                        st.success(f"{updated_count}건의 변경 사항이 저장되었습니다.")
                        time.sleep(1)
                        st.rerun()
            
        else:
            st.info("등록된 구매 발주 내역이 없습니다.")

    # --- Tab 2: Bulk Upload ---
    with tab2:
        st.subheader("PO 일괄 등록 (Bulk Upload)")
        
        st.markdown("""
        **업로드 가이드:**
        1. CSV 파일은 다음 컬럼을 포함해야 합니다: `PKID`, `Supplier`, `Order Qty`, `ETA`
        2. `Status`와 `Remarks`는 선택 사항입니다. (기본값: `PO Issued`)
        3. 날짜 형식은 `YYYY-MM-DD`를 권장합니다.
        """)
        
        # Template Download
        template_data = pd.DataFrame({
            'PKID': ['PKID001', 'PKID002'],
            'Supplier': ['Supplier A', 'Supplier B'],
            'Order Qty': [100, 200],
            'ETA': [datetime.now().strftime('%Y-%m-%d'), datetime.now().strftime('%Y-%m-%d')],
            'Status': ['PO Issued', 'In-Transit'],
            'Remarks': ['Urgent', '']
        })
        csv_template = template_data.to_csv(index=False).encode('utf-8-sig')
        st.download_button("📥 템플릿 다운로드", csv_template, "po_upload_template.csv", "text/csv")
        
        uploaded_file = st.file_uploader("CSV 파일 업로드", type=['csv'])
        
        if uploaded_file:
            try:
                df_upload = pd.read_csv(uploaded_file)
                st.dataframe(df_upload.head())
                
                if st.button("업로드 실행 (Upload)"):
                    required_cols = {'PKID', 'Supplier', 'Order Qty', 'ETA'}
                    if not required_cols.issubset(df_upload.columns):
                        st.error(f"필수 컬럼이 누락되었습니다: {required_cols - set(df_upload.columns)}")
                    else:
                        with st.spinner("PO 생성 중..."):
                            success, errors = process_bulk_upload(df_upload)
                            
                            if success > 0:
                                st.success(f"✅ {success}건의 PO가 성공적으로 생성되었습니다.")
                            
                            if errors:
                                st.error(f"❌ {len(errors)}건의 오류가 발생했습니다.")
                                for err in errors:
                                    st.write(err)
            except Exception as e:
                st.error(f"파일 처리 중 오류 발생: {e}")
