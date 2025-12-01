import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import io
from datetime import datetime
import time

# --- Database Helper Functions ---

def get_db_connection():
    max_retries = 3
    retry_delay = 1
    
    for attempt in range(max_retries):
        try:
            db_url = st.secrets["db_url"]
            # Add SSL mode if not present
            if '?' not in db_url:
                db_url += '?sslmode=require'
            elif 'sslmode' not in db_url:
                db_url += '&sslmode=require'
            conn = psycopg2.connect(db_url, cursor_factory=RealDictCursor)
            return conn
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            raise
        except KeyError:
            st.error("Database URL not found in secrets.")
            st.stop()

def init_bom_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS BOM_Master (
            PARENT_PN TEXT NOT NULL,
            CHILD_PKID TEXT NOT NULL,
            BOM_QTY REAL NOT NULL,
            CREATED_AT DATE DEFAULT CURRENT_DATE,
            PRIMARY KEY (PARENT_PN, CHILD_PKID)
        )
    ''')
    conn.commit()
    conn.close()

def init_substitute_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Substitute_Master (
            SUB_ID SERIAL PRIMARY KEY,
            CHILD_PKID TEXT NOT NULL,
            CHILD_PKID_NAME TEXT,
            SUBSTITUTE_PKID TEXT NOT NULL,
            SUBSTITUTE_PKID_NAME TEXT,
            DESCRIPTION TEXT,
            REG_DATE DATE DEFAULT CURRENT_DATE
        )
    ''')
    conn.commit()
    conn.close()

def get_all_product_pns():
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT PN FROM Product_Master", conn)
    conn.close()
    # Normalize columns to uppercase
    df.columns = df.columns.str.upper()
    return set(df['PN'].tolist())

# ===== BOM Functions =====
def insert_bom_record(parent_pn, child_pkid, bom_qty):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO BOM_Master (PARENT_PN, CHILD_PKID, BOM_QTY)
            VALUES (%s, %s, %s)
        ''', (parent_pn, child_pkid, bom_qty))
        conn.commit()
        return True, "Success"
    except psycopg2.IntegrityError as e:
        conn.rollback()
        return False, f"Integrity Error: {e}"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()

def update_bom_record(parent_pn, child_pkid, bom_qty):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            UPDATE BOM_Master
            SET BOM_QTY = %s
            WHERE PARENT_PN = %s AND CHILD_PKID = %s
        ''', (bom_qty, parent_pn, child_pkid))
        conn.commit()
        return True, "Success"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()

def delete_bom_record(parent_pn, child_pkid):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('DELETE FROM BOM_Master WHERE PARENT_PN = %s AND CHILD_PKID = %s', (parent_pn, child_pkid))
        conn.commit()
        return True, "Success"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()

# ===== Substitute Functions =====
def insert_substitute_record(child_pkid, child_name, sub_pkid, sub_name, description):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO Substitute_Master (CHILD_PKID, CHILD_PKID_NAME, SUBSTITUTE_PKID, SUBSTITUTE_PKID_NAME, DESCRIPTION)
            VALUES (%s, %s, %s, %s, %s)
        ''', (child_pkid, child_name, sub_pkid, sub_name, description))
        conn.commit()
        return True, "Success"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()

def update_substitute_record(sub_id, child_pkid, child_name, sub_pkid, sub_name, description):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            UPDATE Substitute_Master
            SET CHILD_PKID = %s, CHILD_PKID_NAME = %s, SUBSTITUTE_PKID = %s, SUBSTITUTE_PKID_NAME = %s, DESCRIPTION = %s
            WHERE SUB_ID = %s
        ''', (child_pkid, child_name, sub_pkid, sub_name, description, sub_id))
        conn.commit()
        return True, "Success"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()

def delete_substitute_record(sub_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('DELETE FROM Substitute_Master WHERE SUB_ID = %s', (sub_id,))
        conn.commit()
        return True, "Success"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()

def show_bom_management():
    st.title("🔩 BOM 관리 및 대체자재")
    init_bom_db()
    init_substitute_db()

    # Main tabs for BOM and Substitute
    main_tab1, main_tab2 = st.tabs(["📋 자재 명세서 (BOM Master)", "🔄 대체 자재 (Substitute Master)"])

    # ========== BOM MASTER TAB ==========
    with main_tab1:
        tab1, tab2, tab3 = st.tabs(["📂 Bulk Upload", "📝 Registration/Modification", "🔍 View BOM"])

        # --- Tab 1: BOM Bulk Upload ---
        with tab1:
            st.header("BOM Bulk Upload via CSV")
            st.info("Required Columns: PARENT_PN, CHILD_PKID, BOM_QTY")
            
            uploaded_file = st.file_uploader("Upload BOM CSV", type=['csv'], key="bom_upload")
            
            if uploaded_file is not None:
                try:
                    # Robust CSV Loading
                    try:
                        df = pd.read_csv(uploaded_file, encoding='utf-8-sig')
                    except UnicodeDecodeError:
                        uploaded_file.seek(0)
                        df = pd.read_csv(uploaded_file, encoding='cp949')
                    
                    # Normalize columns
                    df.columns = df.columns.str.strip().str.upper()
                    
                    required_cols = {'PARENT_PN', 'CHILD_PKID', 'BOM_QTY'}
                    if not required_cols.issubset(df.columns):
                        st.error(f"Missing required columns: {', '.join(required_cols - set(df.columns))}")
                    else:
                        error_rows = []
                        valid_product_pns = get_all_product_pns()
                        
                        conn = get_db_connection()
                        existing_bom_df = pd.read_sql_query("SELECT PARENT_PN, CHILD_PKID FROM BOM_Master", conn)
                        conn.close()
                        
                        # Normalize columns
                        existing_bom_df.columns = existing_bom_df.columns.str.upper()
                        
                        existing_bom_set = set(zip(existing_bom_df['PARENT_PN'], existing_bom_df['CHILD_PKID']))

                        # 1. Check for Nulls
                        if df[list(required_cols)].isnull().any().any():
                            null_rows = df[df[list(required_cols)].isnull().any(axis=1)].copy()
                            null_rows['Error'] = "Null values in required columns"
                            error_rows.append(null_rows)
                            df = df.dropna(subset=list(required_cols))

                        # 1.5. Filter out header rows
                        if not df.empty:
                            header_mask = df['PARENT_PN'].astype(str).str.upper() == 'PARENT_PN'
                            if header_mask.any():
                                st.warning(f"Filtering out {header_mask.sum()} header rows from CSV.")
                                df = df[~header_mask]

                        if not df.empty:
                            # 2. Validate BOM_QTY
                            df['BOM_QTY_NUM'] = pd.to_numeric(df['BOM_QTY'], errors='coerce')
                            invalid_qty_mask = (df['BOM_QTY_NUM'].isna()) | (df['BOM_QTY_NUM'] <= 0)
                            if invalid_qty_mask.any():
                                invalid_qty_rows = df[invalid_qty_mask].copy()
                                invalid_qty_rows['Error'] = "Invalid BOM_QTY (Must be numeric > 0)"
                                error_rows.append(invalid_qty_rows)
                                df = df[~invalid_qty_mask]
                            
                            df['BOM_QTY'] = df['BOM_QTY_NUM']
                            df = df.drop(columns=['BOM_QTY_NUM'])

                        if not df.empty:
                            # 3. Validate PARENT_PN
                            unknown_pn_mask = ~df['PARENT_PN'].isin(valid_product_pns)
                            if unknown_pn_mask.any():
                                unknown_pn_rows = df[unknown_pn_mask].copy()
                                unknown_pn_rows['Error'] = "PARENT_PN not found in Product Master"
                                error_rows.append(unknown_pn_rows)
                                df = df[~unknown_pn_mask]

                        if not df.empty:
                            # 4. Check for Duplicates
                            df['key'] = list(zip(df['PARENT_PN'], df['CHILD_PKID']))
                            duplicate_mask = df['key'].isin(existing_bom_set)
                            
                            if duplicate_mask.any():
                                duplicate_rows = df[duplicate_mask].copy()
                                duplicate_rows['Error'] = "BOM relationship already exists"
                                error_rows.append(duplicate_rows.drop(columns=['key']))
                                df = df[~duplicate_mask]
                            
                            df = df.drop(columns=['key'])

                        # Show errors
                        if error_rows:
                            all_errors = pd.concat(error_rows)
                            st.error(f"Validation failed for {len(all_errors)} rows.")
                            st.dataframe(all_errors)
                            
                            csv = all_errors.to_csv(index=False).encode('utf-8')
                            st.download_button(
                                label="Download Error Report",
                                data=csv,
                                file_name='bom_upload_errors.csv',
                                mime='text/csv',
                            )

                        # Insert Valid Rows
                        if not df.empty:
                            conn = get_db_connection()
                            try:
                                cols_to_insert = ['PARENT_PN', 'CHILD_PKID', 'BOM_QTY']
                                
                                # Use executemany for batch insert
                                cursor = conn.cursor()
                                data_tuples = [tuple(x) for x in df[cols_to_insert].to_numpy()]
                                query = "INSERT INTO BOM_Master (PARENT_PN, CHILD_PKID, BOM_QTY) VALUES (%s, %s, %s)"
                                cursor.executemany(query, data_tuples)
                                conn.commit()
                                st.success(f"Successfully uploaded {len(df)} BOM records.")
                            except Exception as e:
                                conn.rollback()
                                st.error(f"Database Insertion Error: {e}")
                            finally:
                                conn.close()
                        else:
                            if not error_rows:
                                st.info("No valid data to upload.")

                except Exception as e:
                    st.error(f"Failed to process CSV: {e}")

        # --- Tab 2: BOM CRUD ---
        with tab2:
            st.header("Individual BOM Management")
            
            crud_option = st.radio("Action", ["Add BOM Item", "Update BOM Item", "Delete BOM Item"], key="bom_crud")
            
            if crud_option == "Add BOM Item":
                with st.form("add_bom_form"):
                    parent_pn = st.text_input("Parent PN (Product)")
                    child_pkid = st.text_input("Child PKID (Part)")
                    bom_qty = st.number_input("Quantity", min_value=0.0001, format="%.4f")
                    
                    submitted = st.form_submit_button("Add")
                    if submitted:
                        valid_pns = get_all_product_pns()
                        if parent_pn not in valid_pns:
                            st.error("Parent PN does not exist in Product Master.")
                        elif not child_pkid:
                            st.error("Child PKID is required.")
                        elif bom_qty <= 0:
                            st.error("Quantity must be greater than 0.")
                        else:
                            success, msg = insert_bom_record(parent_pn, child_pkid, bom_qty)
                            if success:
                                st.success("BOM Item added successfully.")
                            else:
                                st.error(f"Failed: {msg}")

            elif crud_option == "Update BOM Item":
                col1, col2 = st.columns(2)
                with col1:
                    u_parent = st.text_input("Target Parent PN")
                with col2:
                    u_child = st.text_input("Target Child PKID")
                
                if st.button("Search for Update", key="bom_search"):
                    conn = get_db_connection()
                    record = pd.read_sql_query("SELECT * FROM BOM_Master WHERE PARENT_PN = %s AND CHILD_PKID = %s", conn, params=(u_parent, u_child))
                    conn.close()
                    
                    # Normalize columns
                    record.columns = record.columns.str.upper()
                    
                    if not record.empty:
                        st.session_state['update_bom_record'] = record.iloc[0].to_dict()
                    else:
                        st.error("Record not found.")
                
                if 'update_bom_record' in st.session_state:
                    rec = st.session_state['update_bom_record']
                    if rec['PARENT_PN'] == u_parent and rec['CHILD_PKID'] == u_child:
                        with st.form("update_bom_form"):
                            st.write(f"Updating: {rec['PARENT_PN']} -> {rec['CHILD_PKID']}")
                            new_qty = st.number_input("New Quantity", min_value=0.0001, value=float(rec['BOM_QTY']), format="%.4f")
                            
                            if st.form_submit_button("Update"):
                                success, msg = update_bom_record(u_parent, u_child, new_qty)
                                if success:
                                    st.success("Updated successfully.")
                                    del st.session_state['update_bom_record']
                                else:
                                    st.error(f"Failed: {msg}")

            elif crud_option == "Delete BOM Item":
                d_parent = st.text_input("Parent PN to Delete")
                d_child = st.text_input("Child PKID to Delete")
                
                if st.button("Delete", key="bom_delete"):
                    if d_parent and d_child:
                        success, msg = delete_bom_record(d_parent, d_child)
                        if success:
                            st.success("Deleted successfully.")
                        else:
                            st.error(f"Failed: {msg}")
                    else:
                        st.error("Please enter both Parent PN and Child PKID.")

        # --- Tab 3: View BOM ---
        with tab3:
            st.header("View BOM Structure")
            
            search_pn = st.text_input("Search by Parent PN", key="bom_search_view")
            
            if search_pn:
                conn = get_db_connection()
                query = "SELECT * FROM BOM_Master WHERE PARENT_PN LIKE %s"
                df = pd.read_sql_query(query, conn, params=(f"%{search_pn}%",))
                conn.close()
                
                # Normalize columns
                df.columns = df.columns.str.upper()
                
                if not df.empty:
                    st.dataframe(df, use_container_width=True)
                    st.write(f"Total Components: {len(df)}")
                else:
                    st.info("No BOM records found for this PN.")
            else:
                conn = get_db_connection()
                df = pd.read_sql_query("SELECT * FROM BOM_Master LIMIT 100", conn)
                conn.close()
                # Normalize columns
                df.columns = df.columns.str.upper()
                st.dataframe(df, use_container_width=True)
                st.caption("Showing top 100 records.")

    # ========== SUBSTITUTE MASTER TAB ==========
    with main_tab2:
        tab1, tab2, tab3 = st.tabs(["📂 Bulk Upload", "📝 Registration/Modification", "🔍 View Substitutes"])

        # --- Tab 1: Substitute Bulk Upload ---
        with tab1:
            st.header("Substitute Master Bulk Upload via CSV")
            st.info("Required Columns: CHILD_PKID, SUBSTITUTE_PKID (Others are optional)")
            st.warning("Duplicate Check: Only exact row matches are considered duplicates.")
            
            uploaded_file = st.file_uploader("Upload Substitute CSV", type=['csv'], key="sub_upload")
            
            if uploaded_file is not None:
                try:
                    # Robust CSV Loading
                    try:
                        df = pd.read_csv(uploaded_file, encoding='utf-8-sig')
                    except UnicodeDecodeError:
                        uploaded_file.seek(0)
                        df = pd.read_csv(uploaded_file, encoding='cp949')
                    
                    # Normalize columns
                    df.columns = df.columns.str.strip().str.upper()
                    
                    required_cols = {'CHILD_PKID', 'SUBSTITUTE_PKID'}
                    if not required_cols.issubset(df.columns):
                        st.error(f"Missing required columns: {', '.join(required_cols - set(df.columns))}")
                    else:
                        error_rows = []
                        
                        # 1. Check for Nulls in required columns
                        if df[list(required_cols)].isnull().any().any():
                            null_rows = df[df[list(required_cols)].isnull().any(axis=1)].copy()
                            null_rows['Error'] = "Null values in required columns"
                            error_rows.append(null_rows)
                            df = df.dropna(subset=list(required_cols))

                        # 1.5. Filter out header rows
                        if not df.empty:
                            header_mask = df['CHILD_PKID'].astype(str).str.upper() == 'CHILD_PKID'
                            if header_mask.any():
                                st.warning(f"Filtering out {header_mask.sum()} header rows from CSV.")
                                df = df[~header_mask]

                        if not df.empty:
                            # 2. Relaxed Duplicate Check - ONLY exact row matches
                            # Load existing data
                            conn = get_db_connection()
                            existing_df = pd.read_sql_query("SELECT CHILD_PKID, CHILD_PKID_NAME, SUBSTITUTE_PKID, SUBSTITUTE_PKID_NAME, DESCRIPTION FROM Substitute_Master", conn)
                            conn.close()
                            
                            # Normalize columns
                            existing_df.columns = existing_df.columns.str.upper()
                            
                            # Prepare columns for comparison
                            compare_cols = ['CHILD_PKID', 'SUBSTITUTE_PKID']
                            if 'CHILD_PKID_NAME' in df.columns:
                                compare_cols.append('CHILD_PKID_NAME')
                            else:
                                df['CHILD_PKID_NAME'] = None
                            
                            if 'SUBSTITUTE_PKID_NAME' in df.columns:
                                compare_cols.append('SUBSTITUTE_PKID_NAME')
                            else:
                                df['SUBSTITUTE_PKID_NAME'] = None
                            
                            if 'DESCRIPTION' in df.columns:
                                compare_cols.append('DESCRIPTION')
                            else:
                                df['DESCRIPTION'] = None
                            
                            # Fill NaN with empty string for comparison
                            df_compare = df[compare_cols].fillna('')
                            existing_compare = existing_df[compare_cols].fillna('')
                            
                            # Create tuples for exact match comparison
                            df['row_tuple'] = df_compare.apply(tuple, axis=1)
                            existing_set = set(existing_compare.apply(tuple, axis=1))
                            
                            # Check for exact duplicates
                            duplicate_mask = df['row_tuple'].isin(existing_set)
                            
                            if duplicate_mask.any():
                                duplicate_rows = df[duplicate_mask].copy().drop(columns=['row_tuple'])
                                duplicate_rows['Error'] = "Exact duplicate row exists"
                                error_rows.append(duplicate_rows)
                                df = df[~duplicate_mask]
                            
                            df = df.drop(columns=['row_tuple'])

                        # Show errors
                        if error_rows:
                            all_errors = pd.concat(error_rows)
                            st.error(f"Validation failed for {len(all_errors)} rows.")
                            st.dataframe(all_errors)
                            
                            csv = all_errors.to_csv(index=False).encode('utf-8')
                            st.download_button(
                                label="Download Error Report",
                                data=csv,
                                file_name='substitute_upload_errors.csv',
                                mime='text/csv',
                            )

                        # Insert Valid Rows
                        if not df.empty:
                            conn = get_db_connection()
                            try:
                                cols_to_insert = ['CHILD_PKID', 'SUBSTITUTE_PKID']
                                if 'CHILD_PKID_NAME' in df.columns:
                                    cols_to_insert.append('CHILD_PKID_NAME')
                                if 'SUBSTITUTE_PKID_NAME' in df.columns:
                                    cols_to_insert.append('SUBSTITUTE_PKID_NAME')
                                if 'DESCRIPTION' in df.columns:
                                    cols_to_insert.append('DESCRIPTION')
                                
                                # Use executemany
                                cursor = conn.cursor()
                                data_tuples = [tuple(x) for x in df[cols_to_insert].to_numpy()]
                                placeholders = ', '.join(['%s'] * len(cols_to_insert))
                                cols_str = ', '.join(cols_to_insert)
                                query = f"INSERT INTO Substitute_Master ({cols_str}) VALUES ({placeholders})"
                                
                                cursor.executemany(query, data_tuples)
                                conn.commit()
                                st.success(f"Successfully uploaded {len(df)} substitute records.")
                            except Exception as e:
                                conn.rollback()
                                st.error(f"Database Insertion Error: {e}")
                            finally:
                                conn.close()
                        else:
                            if not error_rows:
                                st.info("No valid data to upload.")

                except Exception as e:
                    st.error(f"Failed to process CSV: {e}")

        # --- Tab 2: Substitute CRUD ---
        with tab2:
            st.header("Individual Substitute Management")
            
            crud_option = st.radio("Action", ["Add Substitute", "Update Substitute", "Delete Substitute"], key="sub_crud")
            
            if crud_option == "Add Substitute":
                with st.form("add_sub_form"):
                    child_pkid = st.text_input("Original Part PKID")
                    child_name = st.text_input("Original Part Name (Optional)")
                    sub_pkid = st.text_input("Substitute Part PKID")
                    sub_name = st.text_input("Substitute Part Name (Optional)")
                    description = st.text_area("Description/Reason (Optional)")
                    
                    submitted = st.form_submit_button("Add")
                    if submitted:
                        if not child_pkid or not sub_pkid:
                            st.error("CHILD_PKID and SUBSTITUTE_PKID are required.")
                        else:
                            success, msg = insert_substitute_record(child_pkid, child_name, sub_pkid, sub_name, description)
                            if success:
                                st.success("Substitute record added successfully.")
                            else:
                                st.error(f"Failed: {msg}")

            elif crud_option == "Update Substitute":
                sub_id = st.number_input("Enter SUB_ID to Update", min_value=1, step=1)
                
                if st.button("Search for Update", key="sub_search"):
                    conn = get_db_connection()
                    record = pd.read_sql_query("SELECT * FROM Substitute_Master WHERE SUB_ID = %s", conn, params=(sub_id,))
                    conn.close()
                    
                    # Normalize columns
                    record.columns = record.columns.str.upper()
                    
                    if not record.empty:
                        st.session_state['update_sub_record'] = record.iloc[0].to_dict()
                    else:
                        st.error("Record not found.")
                
                if 'update_sub_record' in st.session_state:
                    rec = st.session_state['update_sub_record']
                    if rec['SUB_ID'] == sub_id:
                        with st.form("update_sub_form"):
                            st.write(f"Updating SUB_ID: {rec['SUB_ID']}")
                            new_child = st.text_input("Original PKID", value=rec['CHILD_PKID'])
                            new_child_name = st.text_input("Original Name", value=rec['CHILD_PKID_NAME'] if rec['CHILD_PKID_NAME'] else "")
                            new_sub = st.text_input("Substitute PKID", value=rec['SUBSTITUTE_PKID'])
                            new_sub_name = st.text_input("Substitute Name", value=rec['SUBSTITUTE_PKID_NAME'] if rec['SUBSTITUTE_PKID_NAME'] else "")
                            new_desc = st.text_area("Description", value=rec['DESCRIPTION'] if rec['DESCRIPTION'] else "")
                            
                            if st.form_submit_button("Update"):
                                success, msg = update_substitute_record(sub_id, new_child, new_child_name, new_sub, new_sub_name, new_desc)
                                if success:
                                    st.success("Updated successfully.")
                                    del st.session_state['update_sub_record']
                                else:
                                    st.error(f"Failed: {msg}")

            elif crud_option == "Delete Substitute":
                sub_id = st.number_input("Enter SUB_ID to Delete", min_value=1, step=1, key="del_sub_id")
                
                if st.button("Delete", key="sub_delete"):
                    if sub_id:
                        success, msg = delete_substitute_record(sub_id)
                        if success:
                            st.success("Deleted successfully.")
                        else:
                            st.error(f"Failed: {msg}")
                    else:
                        st.error("Please enter a SUB_ID.")

        # --- Tab 3: View Substitutes ---
        with tab3:
            st.header("View Substitute Master")
            
            search_term = st.text_input("Search by CHILD_PKID or SUBSTITUTE_PKID", key="sub_search_view")
            
            conn = get_db_connection()
            if search_term:
                query = "SELECT * FROM Substitute_Master WHERE CHILD_PKID LIKE %s OR SUBSTITUTE_PKID LIKE %s"
                df = pd.read_sql_query(query, conn, params=(f"%{search_term}%", f"%{search_term}%"))
            else:
                df = pd.read_sql_query("SELECT * FROM Substitute_Master LIMIT 100", conn)
            conn.close()
            
            # Normalize columns
            df.columns = df.columns.str.upper()
            
            if not df.empty:
                st.dataframe(df, use_container_width=True)
                st.write(f"Total Records: {len(df)}")
            else:
                st.info("No substitute records found.")
                if not search_term:
                    st.caption("Showing top 100 records.")
