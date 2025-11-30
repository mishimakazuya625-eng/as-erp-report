import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from datetime import datetime
import bom_substitute_master
import order_management
import schema_update_module
import shortage_analysis_report

# --- Database Helper Functions ---
def get_db_connection():
    # Use Streamlit secrets for database URL
    # Format: postgresql://user:password@host:port/dbname
    try:
        db_url = st.secrets["db_url"]
        conn = psycopg2.connect(db_url, cursor_factory=RealDictCursor)
        return conn
    except KeyError:
        st.error("Database URL not found in secrets. Please set 'db_url' in .streamlit/secrets.toml")
        st.stop()
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        st.stop()

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Product_Master (
            PN TEXT PRIMARY KEY NOT NULL,
            PART_NAME TEXT NOT NULL,
            CAR_TYPE TEXT,
            CUSTOMER TEXT NOT NULL,
            PLANT_SITE TEXT NOT NULL,
            REG_DATE DATE DEFAULT CURRENT_DATE
        )
    ''')
    conn.commit()
    conn.close()

def check_duplicate_pn(pn_list):
    conn = get_db_connection()
    # Postgres uses %s for placeholders
    # For IN clause with list, we need to format manually or use tuple
    if not pn_list:
        return []
    
    placeholders = ','.join(['%s'] * len(pn_list))
    query = f"SELECT PN FROM Product_Master WHERE PN IN ({placeholders})"
    
    # pd.read_sql_query with psycopg2 connection
    existing_pns = pd.read_sql_query(query, conn, params=tuple(pn_list))
    conn.close()
    return existing_pns['PN'].tolist()

def get_valid_plant_sites():
    """Get all valid plant site codes"""
    conn = get_db_connection()
    try:
        df = pd.read_sql_query("SELECT SITE_CODE FROM Plant_Site_Master", conn)
        return set(df['SITE_CODE'].tolist()) if not df.empty else set()
    except:
        # Plant_Site_Master might not exist yet
        return set()
    finally:
        conn.close()

def insert_product(pn, part_name, car_type, customer, plant_site):
    # Validate PLANT_SITE
    valid_sites = get_valid_plant_sites()
    if valid_sites and plant_site not in valid_sites:
        return False, f"Invalid PLANT_SITE. Must be one of: {', '.join(sorted(valid_sites))}"
    
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO Product_Master (PN, PART_NAME, CAR_TYPE, CUSTOMER, PLANT_SITE)
            VALUES (%s, %s, %s, %s, %s)
        ''', (pn, part_name, car_type, customer, plant_site))
        conn.commit()
        return True, "Success"
    except psycopg2.IntegrityError:
        conn.rollback()
        return False, "Product Number (PN) already exists."
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()

def update_product(original_pn, part_name, car_type, customer, plant_site):
    # Validate PLANT_SITE
    valid_sites = get_valid_plant_sites()
    if valid_sites and plant_site not in valid_sites:
        return False, f"Invalid PLANT_SITE. Must be one of: {', '.join(sorted(valid_sites))}"
    
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            UPDATE Product_Master
            SET PART_NAME = %s, CAR_TYPE = %s, CUSTOMER = %s, PLANT_SITE = %s
            WHERE PN = %s
        ''', (part_name, car_type, customer, plant_site, original_pn))
        conn.commit()
        return True, "Success"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()

def delete_product(pn):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('DELETE FROM Product_Master WHERE PN = %s', (pn,))
        conn.commit()
        return True, "Success"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()

# --- Module Functions ---

def show_product_master():
    st.title("🔧 상품등록/수정 (Product Master)")

    # Tabs for different functionalities
    tab1, tab2, tab3 = st.tabs(["📂 Bulk Upload (CSV)", "📝 Registration/Modification", "🔍 View Master Data"])

    # --- Tab 1: Bulk Upload ---
    with tab1:
        st.header("Bulk Upload via CSV")
        st.info("Required Columns: PN, PART_NAME, CUSTOMER, PLANT_SITE (CAR_TYPE is optional)")
        
        uploaded_file = st.file_uploader("Upload CSV file", type=['csv'])
        
        if uploaded_file is not None:
            try:
                df = pd.read_csv(uploaded_file)
                
                # 1. Integrity Check: Required Columns
                required_columns = {'PN', 'PART_NAME', 'CUSTOMER', 'PLANT_SITE'}
                if not required_columns.issubset(df.columns):
                    missing = required_columns - set(df.columns)
                    st.error(f"Missing required columns: {', '.join(missing)}")
                else:
                    # 2. Integrity Check: Null Values in Critical Columns
                    null_check = df[list(required_columns)].isnull().any(axis=1)
                    if null_check.any():
                        error_rows = df[null_check].index.tolist()
                        st.error("Data Error: Null values found in required columns.")
                        st.write("Error Rows (0-indexed):", error_rows)
                        st.dataframe(df[null_check])
                    else:
                        # 3. Validate PLANT_SITE
                        valid_plant_sites = get_valid_plant_sites()
                        if valid_plant_sites:
                            invalid_site_mask = ~df['PLANT_SITE'].isin(valid_plant_sites)
                            if invalid_site_mask.any():
                                invalid_sites_df = df[invalid_site_mask]
                                st.error(f"Found {len(invalid_sites_df)} rows with invalid PLANT_SITE values.")
                                st.write(f"Valid plant sites: {', '.join(sorted(valid_plant_sites))}")
                                st.dataframe(invalid_sites_df[['PN', 'PLANT_SITE']])
                                df = df[~invalid_site_mask]
                        
                        # 4. Duplicate Handling
                        unique_pns = df['PN'].unique().tolist()
                        duplicates_in_db = check_duplicate_pn(unique_pns)
                        
                        if duplicates_in_db:
                            st.warning(f"Found {len(duplicates_in_db)} duplicate PNs in Database. These will be skipped.")
                            st.write("Duplicate PNs:", duplicates_in_db)
                        
                        # Filter out duplicates
                        df_to_insert = df[~df['PN'].isin(duplicates_in_db)]
                        
                        if not df_to_insert.empty:
                            conn = get_db_connection()
                            try:
                                cols_to_insert = ['PN', 'PART_NAME', 'CUSTOMER', 'PLANT_SITE']
                                if 'CAR_TYPE' in df.columns:
                                    cols_to_insert.append('CAR_TYPE')
                                
                                # Use fast_executemany or simple loop for insertion
                                # For simplicity and compatibility, we use to_sql if using sqlalchemy, 
                                # but here we use psycopg2 directly or pandas to_sql with sqlalchemy engine.
                                # However, pandas to_sql requires sqlalchemy engine.
                                # We should use cursor.executemany for psycopg2.
                                
                                cursor = conn.cursor()
                                data_tuples = [tuple(x) for x in df_to_insert[cols_to_insert].to_numpy()]
                                
                                cols_str = ', '.join(cols_to_insert)
                                placeholders = ', '.join(['%s'] * len(cols_to_insert))
                                query = f"INSERT INTO Product_Master ({cols_str}) VALUES ({placeholders})"
                                
                                cursor.executemany(query, data_tuples)
                                conn.commit()
                                st.success(f"Successfully registered {len(df_to_insert)} products.")
                            except Exception as e:
                                conn.rollback()
                                st.error(f"An error occurred during insertion: {e}")
                            finally:
                                conn.close()
                        else:
                            st.info("No new data to insert.")

            except Exception as e:
                st.error(f"Failed to process CSV: {e}")

    # --- Tab 2: CRUD Operations ---
    with tab2:
        st.header("Individual Product Management")
        
        crud_option = st.radio("Action", ["Register New", "Update Existing", "Delete"])
        
        if crud_option == "Register New":
            with st.form("register_form"):
                pn = st.text_input("Product Number (PN)")
                part_name = st.text_input("Part Name")
                car_type = st.text_input("Car Type (Optional)")
                customer = st.text_input("Customer")
                plant_site = st.text_input("Plant Site")
                
                submitted = st.form_submit_button("Register")
                if submitted:
                    if not pn or not part_name or not customer or not plant_site:
                        st.error("Please fill in all required fields.")
                    else:
                        success, msg = insert_product(pn, part_name, car_type, customer, plant_site)
                        if success:
                            st.success(f"Product {pn} registered successfully!")
                        else:
                            st.error(f"Registration failed: {msg}")

        elif crud_option == "Update Existing":
            pn_to_update = st.text_input("Enter PN to Update")
            if pn_to_update:
                conn = get_db_connection()
                # Use %s for parameter
                product = pd.read_sql_query("SELECT * FROM Product_Master WHERE PN = %s", conn, params=(pn_to_update,))
                conn.close()
                
                if not product.empty:
                    current_data = product.iloc[0]
                    with st.form("update_form"):
                        st.write(f"Updating PN: {current_data['PN']}")
                        new_part_name = st.text_input("Part Name", value=current_data['PART_NAME'])
                        new_car_type = st.text_input("Car Type", value=current_data['CAR_TYPE'] if current_data['CAR_TYPE'] else "")
                        new_customer = st.text_input("Customer", value=current_data['CUSTOMER'])
                        new_plant_site = st.text_input("Plant Site", value=current_data['PLANT_SITE'])
                        
                        submitted = st.form_submit_button("Update")
                        if submitted:
                            if not new_part_name or not new_customer or not new_plant_site:
                                st.error("Please fill in all required fields.")
                            else:
                                success, msg = update_product(pn_to_update, new_part_name, new_car_type, new_customer, new_plant_site)
                                if success:
                                    st.success("Product updated successfully!")
                                else:
                                    st.error(f"Update failed: {msg}")
                else:
                    st.warning("Product not found.")

        elif crud_option == "Delete":
            pn_to_delete = st.text_input("Enter PN to Delete")
            if st.button("Delete Product"):
                if pn_to_delete:
                    success, msg = delete_product(pn_to_delete)
                    if success:
                        st.success(f"Product {pn_to_delete} deleted successfully.")
                    else:
                        st.error(f"Deletion failed: {msg}")
                else:
                    st.error("Please enter a PN.")

    # --- Tab 3: View Data ---
    with tab3:
        st.header("Product Master List")
        
        search_term = st.text_input("Search (PN, Part Name, Customer, etc.)")
        
        conn = get_db_connection()
        query = "SELECT * FROM Product_Master"
        params = []
        
        if search_term:
            query += " WHERE PN LIKE %s OR PART_NAME LIKE %s OR CUSTOMER LIKE %s OR PLANT_SITE LIKE %s"
            like_term = f"%{search_term}%"
            params = [like_term, like_term, like_term, like_term]
            
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        st.dataframe(df, use_container_width=True)
        st.write(f"Total Records: {len(df)}")

def show_po_management():
    order_management.show_order_management()

def show_bom_management():
    bom_substitute_master.show_bom_management()

def show_schema_management():
    schema_update_module.show_schema_management()

def show_shortage_analysis():
    shortage_analysis_report.show_shortage_analysis()

def show_schedule_management():
    st.title("📅 원자재 일정 및 완제품 일정 관리")
    st.info("Coming Soon: Material & Product Schedule Management Module")

def show_report():
    st.title("📊 Report 출력")
    st.info("Coming Soon: Reporting Module")

# --- Custom CSS for Modern UI ---
def load_custom_css():
    st.markdown("""
    <style>
    /* Main container */
    .stApp {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    }
    
    /* Sidebar styling */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1e3a8a 0%, #1e40af 100%);
        border-right: 2px solid #3b82f6;
    }
    
    section[data-testid="stSidebar"] .stRadio > label {
        color: #e0e7ff !important;
        font-weight: 600;
        padding: 0.5rem 0;
        transition: all 0.3s ease;
    }
    
    section[data-testid="stSidebar"] .stRadio > label:hover {
        color: #ffffff !important;
        transform: translateX(5px);
    }
    
    /* Headers */
    h1, h2, h3 {
        color: #ffffff !important;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: rgba(255,255,255,0.1);
        padding: 0.5rem;
        border-radius: 10px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: rgba(255,255,255,0.1);
        border-radius: 8px;
        padding: 0.75rem 1.5rem;
        color: #e0e7ff;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    
    .stTabs [data-baseweb="tab"]:hover {
        background: rgba(255,255,255,0.2);
        transform: translateY(-2px);
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
        color: white !important;
        box-shadow: 0 4px 12px rgba(59, 130, 246, 0.4);
    }
    
    /* Cards & Containers */
    .stMarkdown, .stDataFrame {
        background: rgba(255, 255, 255, 0.95);
        border-radius: 12px;
        padding: 1.5rem;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    /* Buttons */
    .stButton > button {
        background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
        color: white;
        font-weight: 600;
        padding: 0.75rem 2rem;
        border-radius: 8px;
        border: none;
        box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 16px rgba(59, 130, 246, 0.4);
    }
    
    /* Metrics */
    div[data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: 700;
        color: #3b82f6;
    }
    
    /* Info boxes */
    .stAlert {
        border-radius: 12px;
        border-left: 4px solid #3b82f6;
        background: rgba(59, 130, 246, 0.1);
    }
    
    /* Dataframe */
    .dataframe {
        border-radius: 8px;
        overflow: hidden;
    }
    </style>
    """, unsafe_allow_html=True)

# --- Main Application ---
def main():
    st.set_page_config(
        page_title="AS ERP System",
        page_icon="🏭",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Load custom CSS
    load_custom_css()
    
    # Initialize DB
    init_db()
    schema_update_module.init_schema_tables()
    
    # Header with logo/brand
    st.markdown("""
        <div style='text-align: center; padding: 1rem 0 2rem 0;'>
            <h1 style='font-size: 3rem; margin: 0; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                       -webkit-background-clip: text; -webkit-text-fill-color: transparent; font-weight: 800;'>
                🏭 AS ERP System
            </h1>
            <p style='color: #e0e7ff; font-size: 1.1rem; margin-top: 0.5rem;'>
                Advanced Supply Chain & Resource Planning
            </p>
        </div>
    """, unsafe_allow_html=True)

    # Sidebar with modern navigation
    with st.sidebar:
        st.markdown("### 📊 Navigation")
        
        menu_options = [
            "1. 🔧 상품등록/수정",
            "2. 📦 PO upload 및 관리",
            "3. 🔩 BOM 관리 및 대체자재",
            "4. 🏭 생산처 및 재고 관리",
            "5. 📅 원자재 일정 관리",
            "6. 🚨 결품 분석 리포트"
        ]
        
        selection = st.radio("", menu_options, label_visibility="collapsed")
        
        # Footer
        st.markdown("---")
        st.markdown("""
            <div style='text-align: center; color: #94a3b8; font-size: 0.85rem;'>
                <p>v1.0.0 | Powered by Streamlit</p>
                <p>© 2025 AS ERP</p>
            </div>
        """, unsafe_allow_html=True)
    
    if selection == "1. 🔧 상품등록/수정":
        show_product_master()
    elif selection == "2. 📦 PO upload 및 관리":
        show_po_management()
    elif selection == "3. 🔩 BOM 관리 및 대체자재":
        show_bom_management()
    elif selection == "4. 🏭 생산처 및 재고 관리":
        show_schema_management()
    elif selection == "5. 📅 원자재 일정 관리":
        show_schedule_management()
    elif selection == "6. 🚨 결품 분석 리포트":
        show_shortage_analysis()

if __name__ == "__main__":
    main()

