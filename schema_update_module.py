import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import io
from datetime import datetime

# --- Database Helper Functions ---
def get_db_connection():
    try:
        db_url = st.secrets["db_url"]
        # Add SSL mode if not present
        if '?' not in db_url:
            db_url += '?sslmode=require'
        elif 'sslmode' not in db_url:
            db_url += '&sslmode=require'
        conn = psycopg2.connect(db_url, cursor_factory=RealDictCursor)
        return conn
    except KeyError:
        st.error("Database URL not found in secrets. Please set 'db_url' in .streamlit/secrets.toml")
        st.stop()
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
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
        
        # Upload
        uploaded_file = st.file_uploader("Upload Plant Site CSV", type=['csv'])
        if uploaded_file:
            df = pd.read_csv(uploaded_file)
            if 'SITE_CODE' not in df.columns:
                st.error("CSV must have 'SITE_CODE' column.")
            else:
                if st.button("Upload Sites"):
                    success, msg = upsert_plant_sites(df)
                    if success:
                        st.success(msg)
                    else:
                        st.error(msg)
        
        # View & Delete
        conn = get_db_connection()
        sites_df = pd.read_sql_query("SELECT * FROM Plant_Site_Master", conn)
        conn.close()
        
        st.dataframe(sites_df, use_container_width=True)
        
        with st.expander("Delete Site"):
            site_to_delete = st.selectbox("Select Site to Delete", sites_df['SITE_CODE'].tolist() if not sites_df.empty else [])
            if st.button("Delete Selected Site"):
                if delete_plant_site(site_to_delete):
                    st.success(f"Deleted {site_to_delete}")
                    st.rerun()
                else:
                    st.error("Failed to delete")

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
            df = pd.read_csv(inv_file)
            st.write("Preview:", df.head())
            
            if st.button("Process Inventory Upload"):
                success, msg = process_inventory_upload(df, snapshot_date)
                if success:
                    st.success(msg)
                else:
                    st.error(msg)

    # --- Tab 3: Inventory History ---
    with tab3:
        st.header("Inventory Snapshot Comparison (Last 4)")
        
        comp_df = get_inventory_comparison()
        
        if not comp_df.empty:
            st.dataframe(comp_df, use_container_width=True)
        else:
            st.info("No inventory history found.")
