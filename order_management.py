import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import io
from datetime import datetime, timedelta
import random

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

def init_order_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # AS_Order Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS AS_Order (
            ORDER_KEY TEXT PRIMARY KEY NOT NULL,
            PN TEXT NOT NULL,
            ORDER_QTY INTEGER NOT NULL CHECK(ORDER_QTY > 0),
            DELIVERED_QTY INTEGER NOT NULL CHECK(DELIVERED_QTY >= 0),
            ORDER_DATE DATE NOT NULL,
            URGENT_FLAG TEXT,
            ORDER_STATUS TEXT NOT NULL CHECK(ORDER_STATUS IN ('OPEN', 'URGENT', 'CLOSED', 'CANCELLED')),
            COMPLETION_DATE DATE,
            -- Foreign key constraint can be added
            -- FOREIGN KEY (PN) REFERENCES Product_Master(PN)
            CONSTRAINT fk_pn FOREIGN KEY(PN) REFERENCES Product_Master(PN)
        )
    ''')
    
    # Inventory_Master Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Inventory_Master (
            PKID TEXT PRIMARY KEY NOT NULL,
            PKID_QTY INTEGER NOT NULL CHECK(PKID_QTY >= 0)
        )
    ''')
    
    conn.commit()
    conn.close()

def get_all_product_pns():
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT PN FROM Product_Master", conn)
    conn.close()
    return set(df['PN'].tolist()) if not df.empty else set()

def upsert_orders(csv_df):
    """
    UPSERT logic with 3 steps (vectorized operations only):
    1. Validation
    2. UPSERT (Update + Insert)
    3. Superseded detection (Cancel orders not in CSV)
    """
    
    results = {
        'inserted': 0,
        'updated': 0,
        'closed': 0,
        'cancelled': 0,
        'invalid': 0
    }
    
    # Step 1: Validation - Filter out invalid PNs
    valid_pns = get_all_product_pns()
    if not valid_pns:
        st.error("No products in Product_Master. Cannot process orders.")
        return results
    
    invalid_mask = ~csv_df['PN'].isin(valid_pns)
    if invalid_mask.any():
        invalid_df = csv_df[invalid_mask]
        results['invalid'] = len(invalid_df)
        st.warning(f"Found {results['invalid']} orders with invalid PNs. These will be skipped.")
        st.dataframe(invalid_df[['ORDER_KEY', 'PN']])
        csv_df = csv_df[~invalid_mask].copy()
    
    if csv_df.empty:
        st.error("No valid orders to process after validation.")
        return results
    
    conn = get_db_connection()
    
    # Load existing orders
    existing_df = pd.read_sql_query("SELECT ORDER_KEY, PN, ORDER_QTY, DELIVERED_QTY, ORDER_STATUS FROM AS_Order", conn)
    
    # Step 2: UPSERT
    # Identify updates vs inserts
    csv_df['is_update'] = csv_df['ORDER_KEY'].isin(existing_df['ORDER_KEY'])
    
    updates_df = csv_df[csv_df['is_update']].copy()
    inserts_df = csv_df[~csv_df['is_update']].copy()
    
    # Process Updates (vectorized)
    if not updates_df.empty:
        # Merge to get current status
        updates_merged = updates_df.merge(
            existing_df[['ORDER_KEY', 'ORDER_STATUS']], 
            on='ORDER_KEY', 
            how='left'
        )
        
        # Determine new status: CLOSED if delivered_qty == order_qty, else keep current
        updates_merged['NEW_STATUS'] = updates_merged.apply(
            lambda row: 'CLOSED' if row['DELIVERED_QTY'] >= row['ORDER_QTY'] 
            else row['ORDER_STATUS'], 
            axis=1
        )
        
        # Count closures
        results['closed'] = (updates_merged['NEW_STATUS'] == 'CLOSED').sum()
        results['updated'] = len(updates_merged)
        
        # Update via SQL (batch update)
        cursor = conn.cursor()
        for _, row in updates_merged.iterrows():
            completion_date = datetime.now().strftime('%Y-%m-%d') if row['NEW_STATUS'] == 'CLOSED' else None
            cursor.execute('''
                UPDATE AS_Order 
                SET DELIVERED_QTY = %s, ORDER_STATUS = %s, COMPLETION_DATE = %s
                WHERE ORDER_KEY = %s
            ''', (row['DELIVERED_QTY'], row['NEW_STATUS'], completion_date, row['ORDER_KEY']))
        conn.commit()
    
    # Process Inserts (vectorized)
    if not inserts_df.empty:
        # Set initial status based on URGENT_FLAG
        inserts_df['ORDER_STATUS'] = inserts_df['URGENT_FLAG'].apply(
            lambda x: 'URGENT' if x == 'Y' else 'OPEN'
        )
        inserts_df['COMPLETION_DATE'] = None
        
        # Insert to DB
        cols = ['ORDER_KEY', 'PN', 'ORDER_QTY', 'DELIVERED_QTY', 'ORDER_DATE', 'URGENT_FLAG', 'ORDER_STATUS', 'COMPLETION_DATE']
        
        cursor = conn.cursor()
        data_tuples = [tuple(x) for x in inserts_df[cols].to_numpy()]
        cols_str = ', '.join(cols)
        placeholders = ', '.join(['%s'] * len(cols))
        query = f"INSERT INTO AS_Order ({cols_str}) VALUES ({placeholders})"
        
        cursor.executemany(query, data_tuples)
        conn.commit()
        results['inserted'] = len(inserts_df)
    
    # Step 3: Superseded Detection - Cancel orders in DB but not in CSV
    # Get all OPEN/URGENT orders from DB
    active_orders = existing_df[existing_df['ORDER_STATUS'].isin(['OPEN', 'URGENT'])]
    
    # Find orders not in the new CSV
    superseded_mask = ~active_orders['ORDER_KEY'].isin(csv_df['ORDER_KEY'])
    superseded_keys = active_orders[superseded_mask]['ORDER_KEY'].tolist()
    
    if superseded_keys:
        cursor = conn.cursor()
        cancellation_date = datetime.now().strftime('%Y-%m-%d')
        # Use tuple for IN clause
        cursor.execute(f'''
            UPDATE AS_Order 
            SET ORDER_STATUS = 'CANCELLED', COMPLETION_DATE = %s
            WHERE ORDER_KEY IN %s
        ''', (cancellation_date, tuple(superseded_keys)))
        conn.commit()
        results['cancelled'] = len(superseded_keys)
    
    conn.close()
    return results

def show_order_management():
    st.title("📦 주문 관리 (Order Management)")
    init_order_db()
    
    tab1, tab2, tab3 = st.tabs([
        "📂 CSV Upload (UPSERT)", 
        "📝 Individual CRUD", 
        "🔍 View Orders"
    ])
    
    # --- Tab 1: CSV Upload with UPSERT ---
    with tab1:
        st.header("Upload Order CSV (UPSERT Logic)")
        st.info("""
        **Required Columns**: ORDER_KEY, PN, ORDER_QTY, DELIVERED_QTY, ORDER_DATE
        
        **UPSERT Logic**:
        1. Validates PN against Product_Master
        2. Updates existing orders & closes when delivered
        3. Inserts new orders with URGENT/OPEN status
        4. Cancels orders not in CSV (superseded)
        """)
        
        uploaded_file = st.file_uploader("Upload CSV", type=['csv'], key="order_upload")
        
        if uploaded_file is not None:
            try:
                df = pd.read_csv(uploaded_file)
                
                # Check required columns
                required_cols = {'ORDER_KEY', 'PN', 'ORDER_QTY', 'DELIVERED_QTY', 'ORDER_DATE'}
                if not required_cols.issubset(df.columns):
                    st.error(f"Missing columns: {', '.join(required_cols - set(df.columns))}")
                else:
                    st.dataframe(df.head(10))
                    
                    if st.button("Process UPSERT", type="primary"):
                        with st.spinner("Processing UPSERT..."):
                            results = upsert_orders(df)
                        
                        # Show results
                        st.success("UPSERT Complete!")
                        col1, col2, col3, col4, col5 = st.columns(5)
                        col1.metric("Inserted", results['inserted'])
                        col2.metric("Updated", results['updated'])
                        col3.metric("Closed", results['closed'])
                        col4.metric("Cancelled", results['cancelled'])
                        col5.metric("Invalid", results['invalid'])
                        
            except Exception as e:
                st.error(f"Error processing CSV: {e}")
    
    # --- Tab 2: Individual CRUD ---
    with tab2:
        st.header("Individual Order Management")
        
        crud_option = st.radio("Action", ["Add Order", "Update Order", "Delete Order"], key="order_crud")
        
        if crud_option == "Add Order":
            with st.form("add_order_form"):
                order_key = st.text_input("Order Key (Unique)")
                
                valid_pns = list(get_all_product_pns())
                pn = st.selectbox("Product PN", valid_pns if valid_pns else ["No products available"])
                
                order_qty = st.number_input("Order Quantity", min_value=1, value=10)
                delivered_qty = st.number_input("Delivered Quantity", min_value=0, value=0)
                order_date = st.date_input("Order Date")
                urgent = st.checkbox("Urgent Order")
                
                if st.form_submit_button("Add"):
                    if not order_key:
                        st.error("Order Key is required")
                    else:
                        conn = get_db_connection()
                        cursor = conn.cursor()
                        try:
                            status = 'URGENT' if urgent else 'OPEN'
                            cursor.execute('''
                                INSERT INTO AS_Order (ORDER_KEY, PN, ORDER_QTY, DELIVERED_QTY, ORDER_DATE, URGENT_FLAG, ORDER_STATUS)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ''', (order_key, pn, order_qty, delivered_qty, order_date.strftime('%Y-%m-%d'), 'Y' if urgent else 'N', status))
                            conn.commit()
                            st.success(f"Order {order_key} added successfully")
                        except psycopg2.IntegrityError:
                            conn.rollback()
                            st.error("Order Key already exists")
                        except Exception as e:
                            conn.rollback()
                            st.error(f"Error: {e}")
                        finally:
                            conn.close()
        
        elif crud_option == "Update Order":
            order_key = st.text_input("Order Key to Update")
            
            if st.button("Search", key="search_order"):
                conn = get_db_connection()
                order = pd.read_sql_query("SELECT * FROM AS_Order WHERE ORDER_KEY = %s", conn, params=(order_key,))
                conn.close()
                
                if not order.empty:
                    st.session_state['update_order'] = order.iloc[0].to_dict()
                else:
                    st.error("Order not found")
            
            if 'update_order' in st.session_state:
                rec = st.session_state['update_order']
                with st.form("update_order_form"):
                    st.write(f"Updating: {rec['ORDER_KEY']}")
                    new_delivered = st.number_input("Delivered Qty", value=int(rec['DELIVERED_QTY']))
                    new_status = st.selectbox("Status", ['OPEN', 'URGENT', 'CLOSED', 'CANCELLED'], 
                                             index=['OPEN', 'URGENT', 'CLOSED', 'CANCELLED'].index(rec['ORDER_STATUS']))
                    
                    if st.form_submit_button("Update"):
                        conn = get_db_connection()
                        cursor = conn.cursor()
                        completion = datetime.now().strftime('%Y-%m-%d') if new_status in ['CLOSED', 'CANCELLED'] else None
                        cursor.execute('''
                            UPDATE AS_Order 
                            SET DELIVERED_QTY = %s, ORDER_STATUS = %s, COMPLETION_DATE = %s
                            WHERE ORDER_KEY = %s
                        ''', (new_delivered, new_status, completion, order_key))
                        conn.commit()
                        conn.close()
                        st.success("Updated successfully")
                        del st.session_state['update_order']
        
        elif crud_option == "Delete Order":
            order_key = st.text_input("Order Key to Delete")
            if st.button("Delete", type="secondary"):
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute("DELETE FROM AS_Order WHERE ORDER_KEY = %s", (order_key,))
                conn.commit()
                conn.close()
                st.success(f"Order {order_key} deleted")
    
    # --- Tab 3: View Orders ---
    with tab3:
        st.header("View Orders")
        
        # Filters
        col1, col2, col3 = st.columns(3)
        with col1:
            search_key = st.text_input("Search Order Key")
        with col2:
            search_pn = st.text_input("Search PN")
        with col3:
            filter_status = st.multiselect("Filter Status", ['OPEN', 'URGENT', 'CLOSED', 'CANCELLED'])
        
        # Build query
        conn = get_db_connection()
        query = "SELECT * FROM AS_Order WHERE 1=1"
        params = []
        
        if search_key:
            query += " AND ORDER_KEY LIKE %s"
            params.append(f"%{search_key}%")
        if search_pn:
            query += " AND PN LIKE %s"
            params.append(f"%{search_pn}%")
        if filter_status:
            # Postgres IN clause with tuple
            query += " AND ORDER_STATUS IN %s"
            params.append(tuple(filter_status))
        
        query += " ORDER BY ORDER_DATE DESC LIMIT 1000"
        
        df = pd.read_sql_query(query, conn, params=params if params else None)
        conn.close()
        
        if not df.empty:
            st.dataframe(df, use_container_width=True)
            st.write(f"Total: {len(df)} orders")
            
            # Download current view
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download Current View as CSV",
                data=csv,
                file_name='orders_export.csv',
                mime='text/csv',
            )
        else:
            st.info("No orders found")
