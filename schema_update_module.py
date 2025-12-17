import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
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

    # --- [NEW] AS_Inventory_Master ---
    # PN Based Inventory for Shortage Analysis
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS AS_Inventory_Master (
            PN TEXT NOT NULL,
            LOCATION TEXT NOT NULL,
            SNAPSHOT_DATE DATE NOT NULL,
            QTY INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (PN, LOCATION, SNAPSHOT_DATE)
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
    Convert Wide format to Long format and UPSERT into Inventory_Master.
    Uses small batches with individual commits to avoid statement timeout.
    """
    if 'PKID' not in df.columns:
        return False, "CSV must have a 'PKID' column."

    conn = get_db_connection()
    valid_sites_df = pd.read_sql_query("SELECT SITE_CODE FROM Plant_Site_Master", conn)
    valid_sites_df.columns = valid_sites_df.columns.str.upper()
    valid_sites = set(valid_sites_df['SITE_CODE'].tolist())
    conn.close()

    site_cols = [col for col in df.columns if col in valid_sites]

    if not site_cols:
        return False, f"No valid site columns found in CSV. Registered sites: {valid_sites}"

    # Melt (Wide -> Long)
    long_df = df.melt(id_vars=['PKID'], value_vars=site_cols, var_name='PLANT_SITE', value_name='PKID_QTY')
    long_df['PKID_QTY'] = pd.to_numeric(long_df['PKID_QTY'], errors='coerce').fillna(0).astype(int)
    long_df['SNAPSHOT_DATE'] = snapshot_date

    # Prepare data
    data_tuples = [tuple(x) for x in long_df[['PKID', 'PLANT_SITE', 'SNAPSHOT_DATE', 'PKID_QTY']].to_numpy()]
    total = len(data_tuples)

    # --- Small Batch with Individual Commits ---
    batch_size = 200  # 작은 배치 사이즈로 타임아웃 방지
    success_count = 0

    for i in range(0, total, batch_size):
        batch = data_tuples[i:i+batch_size]

        # 각 배치마다 새 연결 (커넥션 타임아웃 방지)
        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            execute_values(cursor, '''
                INSERT INTO Inventory_Master (PKID, PLANT_SITE, SNAPSHOT_DATE, PKID_QTY)
                VALUES %s
                ON CONFLICT (PKID, PLANT_SITE, SNAPSHOT_DATE) 
                DO UPDATE SET PKID_QTY = EXCLUDED.PKID_QTY
            ''', batch)
            conn.commit()
            success_count += len(batch)
        except Exception as e:
            conn.rollback()
            conn.close()
            return False, f"Error at row {i}: {str(e)}. Successfully inserted: {success_count}"
        finally:
            conn.close()

    return True, f"Successfully uploaded {success_count} inventory records for date {snapshot_date}."

def get_inventory_comparison():
    """
    Get inventory counts for the last 4 snapshots for each PKID/Site
    """
    conn = get_db_connection()

    # Get distinct top 4 dates
    dates_df = pd.read_sql_query("SELECT DISTINCT SNAPSHOT_DATE FROM Inventory_Master ORDER BY SNAPSHOT_DATE DESC LIMIT 4", conn)

    # Normalize columns
    dates_df.columns = dates_df.columns.str.upper()

    if not dates_df.empty:
        dates = dates_df['SNAPSHOT_DATE'].tolist()
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

    # Normalize columns
    df.columns = df.columns.str.upper()

    # Pivot: Index=[PKID, PLANT_SITE], Columns=SNAPSHOT_DATE, Values=PKID_QTY
    pivot_df = df.pivot_table(index=['PKID', 'PLANT_SITE'], columns='SNAPSHOT_DATE', values='PKID_QTY', fill_value=0)

    # Sort columns descending (Newest first)
    pivot_df = pivot_df.sort_index(axis=1, ascending=False)

    # Flatten columns
    pivot_df.columns = [str(date) for date in pivot_df.columns]
    pivot_df = pivot_df.reset_index()

    return pivot_df

# --- [NEW] AS Inventory Logic ---
def process_as_inventory_upload(df, snapshot_date):
    """
    Upload AS Inventory (PN based) with specific locations.
    Target Columns: PN, 114(A/S창고), 114C(천안 A/S창고), 114R(부산 A/S 창고), 111H(HMC창고), 운송중(927SF), 운송중(111S), 운송중(DEY)
    """
    REQUIRED_LOCATIONS = ['114(A/S창고)', '114C(천안 A/S창고)', '114R(부산 A/S 창고)', '111H(HMC창고)', '운송중(927SF)', '운송중(111S)', '운송중(DEY)']
    
    if 'PN' not in df.columns:
        return False, "CSV must have 'PN' column."

    # Identify which location columns exist in the uploaded file
    present_locations = [col for col in REQUIRED_LOCATIONS if col in df.columns]
    
    if not present_locations:
        return False, f"CSV must contain at least one of these columns: {REQUIRED_LOCATIONS}"

    # Melt Wide -> Long
    long_df = df.melt(id_vars=['PN'], value_vars=present_locations, var_name='LOCATION', value_name='QTY')
    
    # Clean Data
    long_df['QTY'] = pd.to_numeric(long_df['QTY'], errors='coerce').fillna(0).astype(int)
    long_df['SNAPSHOT_DATE'] = snapshot_date
    
    # Prepare for Bulk Insert
    data_tuples = [tuple(x) for x in long_df[['PN', 'LOCATION', 'SNAPSHOT_DATE', 'QTY']].to_numpy()]
    
    # Batch Insert
    batch_size = 500
    success_count = 0
    total = len(data_tuples)
    
    for i in range(0, total, batch_size):
        batch = data_tuples[i:i+batch_size]
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            execute_values(cursor, '''
                INSERT INTO AS_Inventory_Master (PN, LOCATION, SNAPSHOT_DATE, QTY)
                VALUES %s
                ON CONFLICT (PN, LOCATION, SNAPSHOT_DATE)
                DO UPDATE SET QTY = EXCLUDED.QTY
            ''', batch)
            conn.commit()
            success_count += len(batch)
        except Exception as e:
            conn.rollback()
            conn.close()
            return False, f"Error uploading batch {i}: {e}. Processed {success_count} records."
        finally:
            conn.close()
            
    return True, f"Successfully uploaded {success_count} AS inventory records."

def show_schema_management():
    st.title("🏭 생산처 및 재고 관리 (Master Data)")
    
    # Initialize Tables First
    init_schema_tables()

    tab1, tab2, tab3, tab4 = st.tabs([
        "🏭 Plant Site Management", 
        "📦 Inventory Upload (Wide)", 
        "🔧 A/S Inventory Upload", 
        "📈 Inventory History"
    ])

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
                elif site_code.upper() in ['SITE_CODE', 'SITE CODE']:
                    st.error("Invalid Site Code: Cannot use 'site_code' as a site code.")
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
        st.header("Inventory Upload (Standard - PKID Based)")
        st.info("""
        **Format**: PKID column + Site Code columns.
        Example:
        | PKID | VINA | HANOI | ... |
        |------|------|-------|-----|
        | P001 | 100  | 50    | ... |
        """)

        snapshot_date = st.date_input("Snapshot Date", value=datetime.now(), key="date_std")
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

                # Normalize data
                if 'PN' in df.columns and 'PKID' not in df.columns:
                     st.warning("Warning: Found 'PN' but not 'PKID'. This tab requires 'PKID'. Did you mean to use the A/S Upload tab?")

                if 'PKID' in df.columns:
                    df['PKID'] = df['PKID'].astype(str).str.strip().str.upper()

                st.write("Preview:", df.head())

                if st.button("Process Inventory Upload", key="btn_std_upload"):
                    success, msg = process_inventory_upload(df, snapshot_date)
                    if success:
                        st.success(msg)
                    else:
                        st.error(msg)
            except Exception as e:
                st.error(f"Error processing CSV: {e}")

    # --- Tab 3: [NEW] AS Inventory Upload ---
    with tab3:
        st.header("🔧 A/S Inventory Upload (PN Based)")
        st.info("""
        **Format**: PN column + Location columns.
        **Supported Locations**: 114(A/S창고), 114C(천안 A/S창고), 114R(부산 A/S 창고), 111H(HMC창고), 운송중(927SF), 운송중(111S), 운송중(DEY)
        
        Example:
        | PN   | 114(A/S창고) | 114C(천안 A/S창고) | ... |
        |------|--------------|-------------------|-----|
        | A001 | 10           | 5                 | ... |
        """)

        as_snapshot_date = st.date_input("Snapshot Date", value=datetime.now(), key="date_as")
        as_file = st.file_uploader("Upload A/S Inventory CSV", type=['csv'], key="as_upload")

        if as_file:
            try:
                try:
                    # Allow cp949 for Korean headers
                    as_df = pd.read_csv(as_file, encoding='cp949')
                except UnicodeDecodeError:
                     as_file.seek(0)
                     as_df = pd.read_csv(as_file, encoding='utf-8-sig')

                # Clean Headers (strip whitespace, but KEEP CASE/Korean for matching)
                as_df.columns = as_df.columns.str.strip()
                
                # Check for PN
                if 'PN' not in as_df.columns and 'pn' in as_df.columns:
                    as_df.rename(columns={'pn': 'PN'}, inplace=True)

                st.write("Preview:", as_df.head())
                
                if st.button("Process A/S Upload", key="btn_as_upload"):
                    success, msg = process_as_inventory_upload(as_df, as_snapshot_date)
                    if success:
                        st.success(msg)
                    else:
                        st.error(msg)
                        
            except Exception as e:
                st.error(f"Error processing CSV: {e}")

    # --- Tab 4: Inventory History ---
    with tab4:
        st.header("Inventory Snapshot Comparison (Last 4 - PKID Based)")

        comp_df = get_inventory_comparison()

        if not comp_df.empty:
            st.dataframe(comp_df, use_container_width=True)
        else:
            st.info("No inventory history found.")

if __name__ == "__main__":
    show_schema_management()
