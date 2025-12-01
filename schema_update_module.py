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

def init_schema_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Plant_Site_Master
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Plant_Site_Master (
            SITE_CODE TEXT PRIMARY KEY NOT NULL,
            SITE_NAME TEXT,
            REGION TEXT,
            CREATED_AT DATE DEFAULT CURRENT_DATE
        )
    ''')
    
    # Inventory_Master (Snapshot based)
    # Composite Primary Key: PKID + PLANT_SITE + SNAPSHOT_DATE
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Inventory_Master (
            PKID TEXT NOT NULL,
            PLANT_SITE TEXT NOT NULL,
            SNAPSHOT_DATE DATE NOT NULL,
            PKID_QTY INTEGER NOT NULL CHECK(PKID_QTY >= 0),
            PRIMARY KEY (PKID, PLANT_SITE, SNAPSHOT_DATE)
        )
    ''')
    
    conn.commit()
    conn.close()

# --- Plant Site Management ---
def upsert_plant_sites(df):
    conn = get_db_connection()
    cursor = conn.cursor()
    inserted_count = 0
    
    try:
        # Postgres ON CONFLICT DO NOTHING
        for _, row in df.iterrows():
            cursor.execute('''
                INSERT INTO Plant_Site_Master (SITE_CODE, SITE_NAME, REGION)
                VALUES (%s, %s, %s)
                ON CONFLICT (SITE_CODE) DO NOTHING
            ''', (row['SITE_CODE'], row.get('SITE_NAME'), row.get('REGION')))
            if cursor.rowcount > 0:
                inserted_count += 1
        conn.commit()
        return True, f"Successfully processed. Inserted {inserted_count} new sites."
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()

def delete_plant_site(site_code):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('DELETE FROM Plant_Site_Master WHERE SITE_CODE = %s', (site_code,))
        conn.commit()
        return True, "Success"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()

# --- Inventory Management (Wide to Long) ---
def process_inventory_upload(df, snapshot_date):
    """
    Convert Wide format (PKID, Site1, Site2...) to Long format (PKID, PLANT_SITE, QTY, DATE)
    and UPSERT into Inventory_Master.
    """
    # 1. Identify Site Columns (Columns that are not PKID/Total)
    # Assuming 'PKID' is the key column.
    if 'PKID' not in df.columns:
        return False, "CSV must have a 'PKID' column."
    
    # Get valid sites from DB to verify columns (Optional but good practice)
    conn = get_db_connection()
    valid_sites_df = pd.read_sql_query("SELECT SITE_CODE FROM Plant_Site_Master", conn)
    conn.close()
    valid_sites = set(valid_sites_df['SITE_CODE'].tolist())
    
    # Filter columns that match valid sites
    site_cols = [col for col in df.columns if col in valid_sites]
    
    if not site_cols:
        return False, f"No valid site columns found in CSV. Registered sites: {valid_sites}"
    
    # 2. Melt (Wide -> Long)
    long_df = df.melt(id_vars=['PKID'], value_vars=site_cols, var_name='PLANT_SITE', value_name='PKID_QTY')
    
    # 3. Clean Data
    long_df['PKID_QTY'] = pd.to_numeric(long_df['PKID_QTY'], errors='coerce').fillna(0).astype(int)
    long_df['SNAPSHOT_DATE'] = snapshot_date
    
    # 4. UPSERT into DB
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Using executemany with ON CONFLICT DO UPDATE
        data_tuples = [tuple(x) for x in long_df[['PKID', 'PLANT_SITE', 'SNAPSHOT_DATE', 'PKID_QTY']].to_numpy()]
        
        query = '''
            INSERT INTO Inventory_Master (PKID, PLANT_SITE, SNAPSHOT_DATE, PKID_QTY)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (PKID, PLANT_SITE, SNAPSHOT_DATE) 
            DO UPDATE SET PKID_QTY = EXCLUDED.PKID_QTY
        '''
        cursor.executemany(query, data_tuples)
        conn.commit()
        return True, f"Successfully uploaded {len(long_df)} inventory records for date {snapshot_date}."
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()

def get_inventory_comparison():
    """
    Get inventory counts for the last 4 snapshots for each PKID/Site
    """
    conn = get_db_connection()
    
    # Get distinct top 4 dates
    dates_df = pd.read_sql_query("SELECT DISTINCT SNAPSHOT_DATE FROM Inventory_Master ORDER BY SNAPSHOT_DATE DESC LIMIT 4", conn)
    
    # PostgreSQL returns lowercase column names
    if not dates_df.empty:
        # Try both cases for compatibility
        date_col = 'snapshot_date' if 'snapshot_date' in dates_df.columns else 'SNAPSHOT_DATE'
        dates = dates_df[date_col].tolist()
    else:
        dates = []
    
    if not dates:
        conn.close()
        return pd.DataFrame()
    
    # Pivot logic in SQL or Pandas. Pandas is easier for dynamic columns.
    # Get all data for these dates
    placeholders = ','.join(['%s'] * len(dates))
    query = f'''
        SELECT PKID, PLANT_SITE, SNAPSHOT_DATE, PKID_QTY 
        FROM Inventory_Master 
        WHERE SNAPSHOT_DATE IN ({placeholders})
    '''
    df = pd.read_sql_query(query, conn, params=tuple(dates))
    conn.close()
    
    if df.empty:
        return pd.DataFrame()
    
    # Handle column name case
    snapshot_col = 'snapshot_date' if 'snapshot_date' in df.columns else 'SNAPSHOT_DATE'
    pkid_col = 'pkid' if 'pkid' in df.columns else 'PKID'
    site_col = 'plant_site' if 'plant_site' in df.columns else 'PLANT_SITE'
    qty_col = 'pkid_qty' if 'pkid_qty' in df.columns else 'PKID_QTY'
    
    # Pivot: Index=[PKID, PLANT_SITE], Columns=SNAPSHOT_DATE, Values=PKID_QTY
    pivot_df = df.pivot_table(index=[pkid_col, site_col], columns=snapshot_col, values=qty_col, fill_value=0)
    
    # Sort columns descending (Newest first)
    pivot_df = pivot_df.sort_index(axis=1, ascending=False)
    
    # Flatten columns
    pivot_df.columns = [str(date) for date in pivot_df.columns]
    pivot_df = pivot_df.reset_index()
    
    return pivot_df

def show_schema_management():
    st.title("🏭 생산처 및 재고 관리 (Master Data)")
    
    tab1, tab2, tab3 = st.tabs(["🏭 Plant Site Management", "📦 Inventory Upload (Wide)", "📈 Inventory History"])
    
    # --- Tab 1: Plant Site ---
    with tab1:
        st.header("Plant Site Master")
        
        # Add New Site Form
        st.subheader("➕ Add New Plant Site")
        with st.form("add_plant_site_form"):
            col1, col2, col3 = st.columns(3)
            with col1:
                site_code = st.text_input("Site Code (Required)", placeholder="e.g., DEY, VINA")
            with col2:
                site_name = st.text_input("Site Name (Optional)", placeholder="e.g., Daeyang")
            with col3:
                region = st.text_input("Region (Optional)", placeholder="e.g., Korea")
            
            submitted = st.form_submit_button("Add Plant Site")
            if submitted:
                if not site_code:
                    st.error("Site Code is required")
                else:
                    # Create a single-row DataFrame for UPSERT
                    new_site_df = pd.DataFrame([{
                        'SITE_CODE': site_code,
                        'SITE_NAME': site_name if site_name else None,
                        'REGION': region if region else None
                    }])
                    success, msg = upsert_plant_sites(new_site_df)
                    if success:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)
        
        st.divider()
        
        # View & Delete
        st.subheader("📋 Current Plant Sites")
        conn = get_db_connection()
        sites_df = pd.read_sql_query("SELECT * FROM Plant_Site_Master", conn)
        conn.close()
        
        # Normalize columns to uppercase
        sites_df.columns = sites_df.columns.str.upper()
        
        if not sites_df.empty:
            st.dataframe(sites_df, use_container_width=True)
            
            # Check for invalid header rows
            header_rows = sites_df[sites_df['SITE_CODE'].astype(str).str.upper() == 'SITE_CODE']
            if not header_rows.empty:
                st.warning(f"Found {len(header_rows)} invalid header rows (SITE_CODE='site_code').")
                if st.button("Delete Invalid Header Rows", key="delete_site_headers"):
                    conn = get_db_connection()
                    cursor = conn.cursor()
                    try:
                        cursor.execute("DELETE FROM Plant_Site_Master WHERE UPPER(SITE_CODE) = 'SITE_CODE'")
                        conn.commit()
                        st.success("Deleted invalid rows. Refreshing...")
                        st.rerun()
                    except Exception as e:
                        conn.rollback()
                        st.error(f"Failed to delete: {e}")
                    finally:
                        conn.close()
            
            with st.expander("🗑️ Delete Site"):
                site_to_delete = st.selectbox("Select Site to Delete", sites_df['SITE_CODE'].tolist())
                if st.button("Delete Selected Site"):
                    success, msg = delete_plant_site(site_to_delete)
                    if success:
                        st.success(f"Deleted {site_to_delete}")
                        st.rerun()
                    else:
                        st.error(f"Failed to delete: {msg}")
        else:
            st.info("No plant sites registered yet. Add your first site above.")


    # --- Tab 2: Inventory Upload ---
    with tab2:
        st.header("Inventory Upload (Wide Format)")
        st.info("""
        **Format**: PKID column + Site Code columns.
        Example:
        | PKID | VINA | HANOI | ... |
        |------|------|-------|-----|
        | P001 | 100  | 50    | ... |
        """)
        
        snapshot_date = st.date_input("Snapshot Date", value=datetime.now())
        inv_file = st.file_uploader("Upload Inventory CSV", type=['csv'], key="inv_upload")
        
        if inv_file:
            try:
                # Robust CSV Loading
                try:
                    df = pd.read_csv(inv_file, encoding='utf-8-sig')
                except UnicodeDecodeError:
                    inv_file.seek(0)
                    df = pd.read_csv(inv_file, encoding='cp949')
                
                # Normalize columns
                df.columns = df.columns.str.strip().str.upper()
                
                st.write("Preview:", df.head())
                
                if st.button("Process Inventory Upload"):
                    success, msg = process_inventory_upload(df, snapshot_date)
                    if success:
                        st.success(msg)
                    else:
                        st.error(msg)
            except Exception as e:
                st.error(f"Error processing CSV: {e}")

    # --- Tab 3: Inventory History ---
    with tab3:
        st.header("Inventory Snapshot Comparison (Last 4)")
        
        comp_df = get_inventory_comparison()
        
        if not comp_df.empty:
            st.dataframe(comp_df, use_container_width=True)
        else:
            st.info("No inventory history found.")
