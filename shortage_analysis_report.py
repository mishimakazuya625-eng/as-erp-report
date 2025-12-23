import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import time

# --- Database Helper Functions ---

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

def get_filter_options():
    """Get available customers from Product_Master for filtering"""
    conn = get_db_connection()
    try:
        query = "SELECT DISTINCT CUSTOMER FROM Product_Master ORDER BY CUSTOMER"
        df = pd.read_sql_query(query, conn)
        df.columns = df.columns.str.upper()
        
        customers = sorted(df['CUSTOMER'].dropna().unique().tolist())
        
        return customers
    except Exception as e:
        st.error(f"Error loading filter options: {e}")
        return []
    finally:
        conn.close()


def load_data(target_customers, target_statuses):
    """
    Load all required data for shortage analysis with pre-filtering
    (Site filter removed - load all sites)
    """
    conn = get_db_connection()
    
    try:
        # Build filter conditions (Case-Insensitive for Status)
        customer_filter = "(" + ",".join([f"'{c}'" for c in target_customers]) + ")" if target_customers else "('')"
        # Always use UPPER in SQL for robustness
        status_filter = "(" + ",".join([f"'{s.upper()}'" for s in target_statuses]) + ")" if target_statuses else "('')"
        
        # Load Orders (filtered by status - UPPER)
        orders_query = f"""
            SELECT ORDER_KEY, PN, ORDER_QTY, DELIVERED_QTY, ORDER_DATE, ORDER_STATUS, URGENT_FLAG
            FROM AS_Order
            WHERE UPPER(ORDER_STATUS) IN {status_filter}
        """
        orders = pd.read_sql_query(orders_query, conn)
        orders.columns = orders.columns.str.upper()
        
        # [NEW] Normalize PN data in orders
        if not orders.empty:
            orders['PN'] = orders['PN'].astype(str).str.strip().str.upper()

        # Load Products (filtered by customer only, NOT by site)
        products_query = f"""
            SELECT PN, PART_NAME, CAR_TYPE, CUSTOMER, PLANT_SITE
            FROM Product_Master
            WHERE CUSTOMER IN {customer_filter}
        """
        products = pd.read_sql_query(products_query, conn)
        products.columns = products.columns.str.upper()
        
        # [NEW] Normalize PN and Site in products
        if not products.empty:
            products['PN'] = products['PN'].astype(str).str.strip().str.upper()
            products['PLANT_SITE'] = products['PLANT_SITE'].astype(str).str.strip().str.upper()

        # Load BOM
        bom_query = "SELECT PARENT_PN, CHILD_PKID, BOM_QTY FROM BOM_Master"
        bom = pd.read_sql_query(bom_query, conn)
        bom.columns = bom.columns.str.upper()
        
        # [NEW] Normalize PN in BOM
        if not bom.empty:
            bom['PARENT_PN'] = bom['PARENT_PN'].astype(str).str.strip().str.upper()
            bom['CHILD_PKID'] = bom['CHILD_PKID'].astype(str).str.strip().str.upper()
        
        # Load Inventory (ALL sites, not filtered)
        inv_query = """
            SELECT PKID, PLANT_SITE, PKID_QTY, SNAPSHOT_DATE
            FROM Inventory_Master
            WHERE SNAPSHOT_DATE = (SELECT MAX(SNAPSHOT_DATE) FROM Inventory_Master)
        """
        inventory = pd.read_sql_query(inv_query, conn)
        inventory.columns = inventory.columns.str.upper()
        
        # [NEW] Normalize PKID and Site in inventory
        if not inventory.empty:
            inventory['PKID'] = inventory['PKID'].astype(str).str.strip().str.upper()
            inventory['PLANT_SITE'] = inventory['PLANT_SITE'].astype(str).str.strip().str.upper()
            
        snapshot_date = inventory['SNAPSHOT_DATE'].iloc[0] if not inventory.empty else None
        
        # Load Substitutes
        sub_query = "SELECT CHILD_PKID, SUBSTITUTE_PKID, DESCRIPTION FROM Substitute_Master"
        substitutes = pd.read_sql_query(sub_query, conn)
        substitutes.columns = substitutes.columns.str.upper()
        
        # [NEW] Normalize PKID in substitutes
        if not substitutes.empty:
            substitutes['CHILD_PKID'] = substitutes['CHILD_PKID'].astype(str).str.strip().str.upper()
            substitutes['SUBSTITUTE_PKID'] = substitutes['SUBSTITUTE_PKID'].astype(str).str.strip().str.upper()

        # Get ALL plant sites from inventory (not from filtered products)
        all_plant_sites = sorted(inventory['PLANT_SITE'].unique().tolist()) if not inventory.empty else []
        
        # --- [NEW] Load AS Inventory ---
        as_inv_query = "SELECT PN, LOCATION, QTY FROM AS_Inventory_Master"
        as_inventory = pd.read_sql_query(as_inv_query, conn)
        # [FIX] Normalize to uppercase to avoid KeyError with 'QTY' vs 'qty'
        as_inventory.columns = as_inventory.columns.str.upper()
        
        # [NEW] Normalize PN in AS inventory
        if not as_inventory.empty:
            as_inventory['PN'] = as_inventory['PN'].astype(str).str.strip().str.upper()

        # Pivot AS Inventory for Analysis (PN index, Columns=Location, Values=QTY)
        if not as_inventory.empty:
            as_pivot = as_inventory.pivot_table(index='PN', columns='LOCATION', values='QTY', aggfunc='sum', fill_value=0)
            as_pivot['AS_TOTAL'] = as_pivot.sum(axis=1) # Total per PN
            as_pivot = as_pivot.reset_index()
        else:
            as_pivot = pd.DataFrame(columns=['PN', 'AS_TOTAL'])
        
        return orders, products, bom, inventory, substitutes, snapshot_date, all_plant_sites, as_pivot
        
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), None, [], pd.DataFrame()
    finally:
        conn.close()

def allocate_as_inventory(order_details, as_pivot):
    """
    Allocates AS Inventory to Orders (FIFO based on ORDER_DATE) to reduce REMAINING_QTY.
    Returns: Updated order_details
    """
    if as_pivot.empty or 'AS_TOTAL' not in as_pivot.columns:
        return order_details
    
    # Merge AS Total info
    # We use a temp column to track available AS inventory
    merged = order_details.merge(as_pivot[['PN', 'AS_TOTAL']], on='PN', how='left')
    merged['AS_TOTAL'] = merged['AS_TOTAL'].fillna(0)
    
    # Sort by Date to prioritize older orders
    merged = merged.sort_values(['PN', 'ORDER_DATE', 'URGENT_FLAG'], ascending=[True, True, False])
    
    # Allocation Logic
    # Since vectorizing stateful subtraction is hard, we iterate by PN groups or use cumulative sum
    # Cumulative approach:
    # 1. Calc Cumulative Remaining per PN
    # 2. Compare with AS_TOTAL
    
    # Group by PN
    # cumsum of remaining qty
    merged['CUM_REQ'] = merged.groupby('PN')['REMAINING_QTY'].cumsum()
    
    # Determine how much can be covered
    # covered_qty = min(remaining_qty, max(0, as_total - (cum_req - remaining_qty)))
    # logic: avail = as_total - previous_cum_req
    #        deduct = min(current_remaining, avail)
    
    def calc_deduction(row):
        total_as = row['AS_TOTAL']
        if total_as <= 0:
            return 0
        
        prev_cum = row['CUM_REQ'] - row['REMAINING_QTY']
        avail_for_this = max(0, total_as - prev_cum)
        
        deduct = min(row['REMAINING_QTY'], avail_for_this)
        return deduct

    merged['AS_DEDUCTED'] = merged.apply(calc_deduction, axis=1)
    
    # Update Remaining Qty
    merged['REMAINING_QTY'] = merged['REMAINING_QTY'] - merged['AS_DEDUCTED']
    
    return merged, as_pivot

def perform_shortage_analysis(target_customers, target_statuses):
    """
    Core Logic with Pre-Filtering and Corrected Aggregation
    """
    orders, products, bom, inventory, substitutes, snapshot_date, all_plant_sites, as_pivot = load_data(target_customers, target_statuses)
    
    if orders.empty:
        return None, None, None, "No orders found matching the status criteria."
    if products.empty:
        return None, None, None, "No products found matching the customer criteria."
    
    # --- Step 1: Prepare Demand Data ---
    order_details = orders.merge(products, on='PN', how='inner')
    
    if order_details.empty:
        return None, None, None, "No matching orders found for the selected customers."
    
    order_details['REMAINING_QTY'] = order_details['ORDER_QTY'] - order_details['DELIVERED_QTY']
    order_details['REMAINING_QTY'] = order_details['REMAINING_QTY'].clip(lower=0)
    
    # [NEW] Apply AS Inventory Deduction
    # This adds 'AS_TOTAL' and 'AS_DEDUCTED' columns
    order_details, as_pivot_info = allocate_as_inventory(order_details, as_pivot)

    # [FIX] Ensure AS_TOTAL exists even if allocation did nothing (e.g. no AS inventory)
    if 'AS_TOTAL' not in order_details.columns:
        order_details['AS_TOTAL'] = 0
    if 'AS_DEDUCTED' not in order_details.columns:
        order_details['AS_DEDUCTED'] = 0

    # Fill NaN URGENT_FLAG with 'N'
    order_details['URGENT_FLAG'] = order_details['URGENT_FLAG'].fillna('N')
    
    exploded = order_details.merge(bom, left_on='PN', right_on='PARENT_PN', how='inner')
    
    if exploded.empty:
        return None, None, None, "No BOM data found for the selected products."
    
    exploded['REQUIRED_QTY'] = exploded['REMAINING_QTY'] * exploded['BOM_QTY']
    
    # --- Step 2: URGENT Propagation ---
    urgent_pkids = exploded[exploded['URGENT_FLAG'] == 'Y']['CHILD_PKID'].unique()
    
    # --- Step 3: Aggregate Demand by PKID and Site ---
    demand_agg = exploded.groupby(['CHILD_PKID', 'PLANT_SITE'])['REQUIRED_QTY'].sum().reset_index()
    
    # Merge with Inventory
    analysis_df = demand_agg.merge(
        inventory, 
        left_on=['CHILD_PKID', 'PLANT_SITE'], 
        right_on=['PKID', 'PLANT_SITE'], 
        how='left'
    )
    
    analysis_df['PKID_QTY'] = analysis_df['PKID_QTY'].fillna(0)
    analysis_df['SHORTAGE_QTY'] = analysis_df['REQUIRED_QTY'] - analysis_df['PKID_QTY']
    analysis_df['SHORTAGE_QTY'] = analysis_df['SHORTAGE_QTY'].clip(lower=0)
    analysis_df['IS_SHORT'] = analysis_df['SHORTAGE_QTY'] > 0
    analysis_df['IS_URGENT'] = analysis_df['CHILD_PKID'].isin(urgent_pkids)
    
    # --- Step 4: Generate R1 Report ---
    # Group by URGENT_FLAG, CAR_TYPE, PART_NAME, CUSTOMER, PLANT_SITE, ORDER_STATUS, PN
    r1_stats = order_details.groupby(['URGENT_FLAG', 'CUSTOMER', 'PLANT_SITE', 'ORDER_STATUS', 'CAR_TYPE', 'PART_NAME', 'PN']).agg(
        TOTAL_ORDER_QTY=('ORDER_QTY', 'sum'),
        TOTAL_DELIVERED_QTY=('DELIVERED_QTY', 'sum'),  # [NEW] Added
        TOTAL_AS_INV=('AS_TOTAL', 'max'), # Show Global AS Inventory for this PN
        TOTAL_REMAINING_QTY=('REMAINING_QTY', 'sum'), # This is now NET Remaining
        TOTAL_AS_DEDUCTED=('AS_DEDUCTED', 'sum') # New metric
    ).reset_index()
    
    r1_base = exploded.merge(
        analysis_df[['CHILD_PKID', 'PLANT_SITE', 'IS_SHORT']],
        on=['CHILD_PKID', 'PLANT_SITE'],
        how='left'
    )
    
    r1_shortage = r1_base.groupby(['CUSTOMER', 'PLANT_SITE', 'ORDER_STATUS', 'PN']).agg(
        SHORT_PKID_COUNT=('CHILD_PKID', lambda x: x[r1_base.loc[x.index, 'IS_SHORT']].nunique()),
        SHORT_PKID_DETAILS=('CHILD_PKID', lambda x: ', '.join(sorted(x[r1_base.loc[x.index, 'IS_SHORT']].unique())))
    ).reset_index()
    
    r1_report = r1_stats.merge(r1_shortage, on=['CUSTOMER', 'PLANT_SITE', 'ORDER_STATUS', 'PN'], how='left')
    
    # [NEW] Merge AS Inventory Details (PN based)
    if not as_pivot_info.empty:
        r1_report = r1_report.merge(as_pivot_info, on='PN', how='left')
        
    # As_pivot columns excluding PN and AS_TOTAL
    as_cols = [c for c in as_pivot_info.columns if c not in ['PN', 'AS_TOTAL']]
    
    # Fill NaN for AS cols
    for col in as_cols:
        if col in r1_report.columns:
            r1_report[col] = r1_report[col].fillna(0)
            
    r1_report = r1_report.rename(columns={
        'TOTAL_ORDER_QTY': '총 주문 수량',
        'TOTAL_DELIVERED_QTY': '납품완료수량',  # [NEW] Added
        # [REMOVED] 'TOTAL_AS_INV': 'AS 재고 총량',
        'TOTAL_REMAINING_QTY': '순 잔여수량', # Renamed
        # [REMOVED] 'TOTAL_AS_DEDUCTED': 'AS 재고 충당 수량',
        'SHORT_PKID_COUNT': '부족 PKID 개수',
        'SHORT_PKID_DETAILS': '결품 부품 상세'
    })
    
    # Reorder columns
    # [MODIFIED] Removed ORDER_STATUS, Added 납품완료수량
    cols_order = [
        'URGENT_FLAG', 'CUSTOMER', 'PLANT_SITE', 'CAR_TYPE', 'PART_NAME', 'PN',
        '총 주문 수량', '납품완료수량'
    ] + as_cols + [
        '순 잔여수량', '부족 PKID 개수', '결품 부품 상세'
    ]
    
    # Ensure all columns exist (in case some are missing)
    cols_order = [c for c in cols_order if c in r1_report.columns]
    r1_report = r1_report[cols_order]
    
    # --- Step 5: Generate R2 Report (Fixed: Get ALL site inventory for each PKID) ---
    
    # 5-1. 수요 기준 pivot (기존 로직)
    pivot_req = analysis_df.pivot_table(index='CHILD_PKID', columns='PLANT_SITE', values='REQUIRED_QTY', aggfunc='sum')
    pivot_req = pivot_req.reindex(columns=all_plant_sites, fill_value=0).add_suffix(' 소요량')
    
    # 5-2. 재고 pivot (수정됨: 수요와 무관하게 inventory 테이블에서 직접 조회)
    # 분석 대상 PKID 목록
    target_pkids = analysis_df['CHILD_PKID'].unique()
    
    # 해당 PKID들의 모든 Site 재고를 inventory에서 직접 가져옴
    inv_for_target = inventory[inventory['PKID'].isin(target_pkids)]
    pivot_inv = inv_for_target.pivot_table(index='PKID', columns='PLANT_SITE', values='PKID_QTY', aggfunc='sum')
    pivot_inv = pivot_inv.reindex(columns=all_plant_sites, fill_value=0).add_suffix(' 재고')
    
    # 5-3. 인덱스 통일 후 병합
    pivot_req.index.name = 'PKID'
    pivot_inv.index.name = 'PKID'
    
    r2_wide = pivot_req.join(pivot_inv, how='left')
    
    # Summary Columns
    summary_cols = analysis_df.groupby('CHILD_PKID').agg(
        TOTAL_REQ=('REQUIRED_QTY', 'sum'),
        TOTAL_SHORTAGE=('SHORTAGE_QTY', 'sum'),
        IS_URGENT=('IS_URGENT', 'max')
    )
    summary_cols.index.name = 'PKID'
    
    # 총 재고는 inventory에서 직접 계산 (수요와 무관하게)
    total_inv = inv_for_target.groupby('PKID')['PKID_QTY'].sum()
    summary_cols['TOTAL_INV'] = total_inv
    summary_cols['TOTAL_INV'] = summary_cols['TOTAL_INV'].fillna(0)
    
    shortage_sites = analysis_df[analysis_df['SHORTAGE_QTY'] > 0].groupby('CHILD_PKID')['PLANT_SITE'].apply(lambda x: ', '.join(x))
    shortage_sites.index.name = 'PKID'
    summary_cols['결품 발생처'] = shortage_sites
    
    r2_report = summary_cols.join(r2_wide, how='left')
    
    # Substitutes logic
    sub_pkids = substitutes['SUBSTITUTE_PKID'].unique()
    if len(sub_pkids) > 0:
        sub_inv = inventory[inventory['PKID'].isin(sub_pkids)]
        sub_inv = sub_inv.copy()
        sub_inv['INV_STR'] = sub_inv['PLANT_SITE'] + ': ' + sub_inv['PKID_QTY'].astype(str)
        sub_inv_agg = sub_inv.groupby('PKID')['INV_STR'].apply(lambda x: ', '.join(x)).reset_index()
        sub_inv_agg.columns = ['SUBSTITUTE_PKID', 'SUB_INV_DETAILS']
        
        substitutes_with_inv = substitutes.merge(sub_inv_agg, on='SUBSTITUTE_PKID', how='left')
        substitutes_with_inv['SUB_INV_DETAILS'] = substitutes_with_inv['SUB_INV_DETAILS'].fillna('재고 없음')
        
        subs_agg = substitutes_with_inv.groupby('CHILD_PKID').agg({
            'SUBSTITUTE_PKID': lambda x: ', '.join(x.dropna().astype(str)),
            'DESCRIPTION': lambda x: ', '.join(x.dropna().astype(str)),
            'SUB_INV_DETAILS': lambda x: ' | '.join(x.dropna().astype(str))
        }).rename(columns={
            'SUBSTITUTE_PKID': '추천 대체품', 
            'DESCRIPTION': '추천대체품 DESCRIPTION',
            'SUB_INV_DETAILS': '대체품 재고 현황 (SITE별)'
        })
        subs_agg.index.name = 'PKID'
        
        r2_report = r2_report.join(subs_agg, how='left')
    else:
        r2_report['추천 대체품'] = ''
        r2_report['추천대체품 DESCRIPTION'] = ''
        r2_report['대체품 재고 현황 (SITE별)'] = ''

    r2_report = r2_report[r2_report['TOTAL_SHORTAGE'] > 0].reset_index()
    
    r2_report = r2_report.rename(columns={
        'TOTAL_REQ': '총 소요량',
        'TOTAL_INV': '총 재고',
        'TOTAL_SHORTAGE': '총 결품 수량'
    })
    
    # Fill NaN with 0 for numeric columns
    exclude_fill_cols = ['PKID', '결품 발생처', '추천 대체품', '추천대체품 DESCRIPTION', '대체품 재고 현황 (SITE별)']
    cols_to_fill = [c for c in r2_report.columns if c not in exclude_fill_cols]
    r2_report[cols_to_fill] = r2_report[cols_to_fill].fillna(0)
    
    # Reorder Columns
    fixed_cols = ['IS_URGENT', 'PKID', '결품 발생처', '총 소요량', '총 재고', '총 결품 수량']
    site_req_cols = [f'{site} 소요량' for site in all_plant_sites]
    site_inv_cols = [f'{site} 재고' for site in all_plant_sites]
    sub_cols = ['추천 대체품', '추천대체품 DESCRIPTION', '대체품 재고 현황 (SITE별)']
    
    final_cols = fixed_cols + site_req_cols + site_inv_cols + sub_cols
    final_cols = [c for c in final_cols if c in r2_report.columns]
    r2_report = r2_report[final_cols]
    
    # --- Step 6: R3 Report ---
    r3_rows = []
    
    for _, product in products.iterrows():
        pn = product['PN']
        part_name = product['PART_NAME']
        customer = product['CUSTOMER']
        plant_site = product['PLANT_SITE']
        
        product_bom = bom[bom['PARENT_PN'] == pn]
        
        if product_bom.empty:
            continue
        
        min_producible = float('inf')
        limiting_component = ''
        component_details = []
        
        for _, bom_row in product_bom.iterrows():
            child_pkid = bom_row['CHILD_PKID']
            bom_qty = bom_row['BOM_QTY']
            
            comp_inv = inventory[(inventory['PKID'] == child_pkid) & (inventory['PLANT_SITE'] == plant_site)]
            
            available_qty = comp_inv['PKID_QTY'].sum() if not comp_inv.empty else 0
            
            if bom_qty > 0:
                producible_with_this_comp = int(available_qty / bom_qty)
            else:
                producible_with_this_comp = 0
            
            if producible_with_this_comp < min_producible:
                min_producible = producible_with_this_comp
                limiting_component = child_pkid
            
            component_details.append(f"{child_pkid}: {available_qty}/{bom_qty}")
        
        if min_producible > 0 and min_producible != float('inf'):
            r3_rows.append({
                'PN': pn,
                'PART_NAME': part_name,
                'CUSTOMER': customer,
                'PLANT_SITE': plant_site,
                '생산가능수량': min_producible,
                '제한부품': limiting_component,
                '부품현황': ' | '.join(component_details)
            })
    
    r3_report = pd.DataFrame(r3_rows)
    if not r3_report.empty:
        r3_report = r3_report.sort_values('생산가능수량', ascending=False).reset_index(drop=True)
    
    return r1_report, r2_report, r3_report, None


def show_shortage_analysis():
    st.title("🚨 결품 분석 리포트 (Shortage Analysis)")
    
    st.info("""
    **분석 프로세스:**
    1. **필터 선택**: 고객사, 주문 상태를 선택합니다.
    2. **분석 실행**: 선택된 조건에 맞는 데이터만 로드하여 분석합니다.
    3. **결과 확인**: R1(통합), R2(상세) 리포트를 확인하고 다운로드합니다.
    """)
    
    # --- 1. Pre-Filtering UI (Site Filter Removed) ---
    st.subheader("1. 분석 대상 필터 (Pre-Filtering)")
    
    avail_customers = get_filter_options()
    
    col1, col2 = st.columns(2)
    with col1:
    # 기본값으로 빈 리스트 [] 를 사용합니다.
        sel_customers = st.multiselect("고객사 (Customer)", avail_customers, default=[])
    with col2:
    # 기본값으로 빈 리스트 [] 를 사용합니다.
        sel_statuses = st.multiselect("주문 상태 (Order Status)", ['OPEN', 'URGENT'], default=[])
    
    # Initialize Session State
    if 'sa_r1' not in st.session_state:
        st.session_state['sa_r1'] = None
    if 'sa_r2' not in st.session_state:
        st.session_state['sa_r2'] = None
    if 'sa_r3' not in st.session_state:
        st.session_state['sa_r3'] = None
    if 'sa_error' not in st.session_state:
        st.session_state['sa_error'] = None
    if 'sa_done' not in st.session_state:
        st.session_state['sa_done'] = False
        
    # --- 2. Run Analysis ---
    if st.button("결품 분석 실행 (Run Analysis)", type="primary"):
        if not sel_statuses:
            st.error("주문 상태를 최소 하나 이상 선택해주세요.")
        else:
            with st.spinner("데이터 로딩 및 분석 중(4~5분 소요...)"):
                r1, r2, r3, error = perform_shortage_analysis(sel_customers, sel_statuses)
                
                st.session_state['sa_r1'] = r1
                st.session_state['sa_r2'] = r2
                st.session_state['sa_r3'] = r3
                st.session_state['sa_error'] = error
                st.session_state['sa_done'] = True
                st.rerun()
    
    # --- 3. Results ---
    if st.session_state['sa_done']:
        st.divider()
        r1 = st.session_state['sa_r1']
        r2 = st.session_state['sa_r2']
        r3 = st.session_state['sa_r3']
        error = st.session_state['sa_error']
        
        if error:
            st.error(error)
        else:
            # R1 Report
            st.subheader("R1. 고객사-생산처별 통합 결품 현황")
            if r1 is not None and not r1.empty:
                st.dataframe(r1, use_container_width=True)
                csv_r1 = r1.to_csv(index=False).encode('utf-8-sig')
                st.download_button("📥 R1 다운로드 (CSV)", csv_r1, f"R1_Shortage_{datetime.now().strftime('%Y%m%d')}.csv", "text/csv")
            else:
                st.info("조건에 맞는 결품 데이터가 없습니다.")
            
            st.divider()
            
            # R2 Report
            st.subheader("R2. 핵심 부품 결품 요약 (Wide Format)")
            if r2 is not None and not r2.empty:
                st.dataframe(
                    r2.style.apply(lambda x: ['background-color: #ffcdd2' if x['IS_URGENT'] else '' for i in x], axis=1),
                    use_container_width=True
                )
                csv_r2 = r2.to_csv(index=False).encode('utf-8-sig')
                st.download_button("📥 R2 다운로드 (CSV)", csv_r2, f"R2_Shortage_Detail_{datetime.now().strftime('%Y%m%d')}.csv", "text/csv")
            else:
                st.info("조건에 맞는 결품 데이터가 없습니다.")
