import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import io
from datetime import datetime

# --- Database Helper Functions ---
# --- Database Helper Functions ---
import time

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
            -- Foreign key constraint can be added if Product_Master exists
            -- FOREIGN KEY (PARENT_PN) REFERENCES Product_Master(PN)
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
                        if df[list(required_cols)].isnull().any().any():
                            null_rows = df[df[list(required_cols)].isnull().any(axis=1)].copy()
                            null_rows['Error'] = "Null values in required columns"
                            error_rows.append(null_rows)
                            df = df.dropna(subset=list(required_cols))

                        if not df.empty:
                            # 2. Relaxed Duplicate Check - ONLY exact row matches
                            # Load existing data
                            conn = get_db_connection()
                            existing_df = pd.read_sql_query("SELECT CHILD_PKID, CHILD_PKID_NAME, SUBSTITUTE_PKID, SUBSTITUTE_PKID_NAME, DESCRIPTION FROM Substitute_Master", conn)
                            conn.close()
                            
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
            
            if not df.empty:
                st.dataframe(df, use_container_width=True)
                st.write(f"Total Records: {len(df)}")
            else:
                st.info("No substitute records found.")
                if not search_term:
                    st.caption("Showing top 100 records.")
