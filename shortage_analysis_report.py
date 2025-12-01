import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
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
    conn = get_db_connection()
    df=pd.read_sql_query("SELECT SITE_CODE FROM PLANT_SITE_MASTER",conn)
    conn.close()
    df.columns=df.columns.str.upper()


def perform_shortage_analysis(target_customers, target_sites, target_statuses):
    """
    Core Logic with Pre-Filtering
    """
    orders, products, bom, inventory, substitutes, snapshot_date, all_plant_sites = load_data(target_customers, target_sites, target_statuses)
    
    if orders.empty:
        return None, None, "No orders found matching the status criteria."
    if products.empty:
        return None, None, "No products found matching the customer/site criteria."
    
    # --- Step 1: Prepare Demand Data ---
    
    # Join Orders with Product Info (Inner Join applies Product filters to Orders)
    order_details = orders.merge(products, on='PN', how='inner')
    
    if order_details.empty:
        return None, None, "No matching orders found for the selected customers/sites."
    
    # Calculate Remaining Qty
    order_details['REMAINING_QTY'] = order_details['ORDER_QTY'] - order_details['DELIVERED_QTY']
    order_details['REMAINING_QTY'] = order_details['REMAINING_QTY'].clip(lower=0)
    
    # Explode BOM
    exploded = order_details.merge(bom, left_on='PN', right_on='PARENT_PN', how='inner')
    
    if exploded.empty:
        return None, None, "No BOM data found for the selected products."
    
    # Calculate Component Demand
    exploded['REQUIRED_QTY'] = exploded['REMAINING_QTY'] * exploded['BOM_QTY']
    
    # --- Step 2: URGENT Propagation ---
    # Identify which PKIDs are used in URGENT orders (within the filtered scope)
    urgent_pkids = exploded[exploded['URGENT_FLAG'] == 'Y']['CHILD_PKID'].unique()
    
    # --- Step 3: Aggregate Demand by PKID and Site ---
    demand_agg = exploded.groupby(['CHILD_PKID', 'PLANT_SITE'])['REQUIRED_QTY'].sum().reset_index()
    
    # Merge with Inventory (Site-Specific Matching)
    analysis_df = demand_agg.merge(
        inventory, 
        left_on=['CHILD_PKID', 'PLANT_SITE'], 
        right_on=['PKID', 'PLANT_SITE'], 
        how='left'
    )
    
    # Fill NaN inventory with 0
    analysis_df['PKID_QTY'] = analysis_df['PKID_QTY'].fillna(0)
    
    # Calculate Shortage
    analysis_df['SHORTAGE_QTY'] = analysis_df['REQUIRED_QTY'] - analysis_df['PKID_QTY']
    analysis_df['SHORTAGE_QTY'] = analysis_df['SHORTAGE_QTY'].clip(lower=0)
    analysis_df['IS_SHORT'] = analysis_df['SHORTAGE_QTY'] > 0
    
    # Add URGENT Flag
    analysis_df['IS_URGENT'] = analysis_df['CHILD_PKID'].isin(urgent_pkids)
    
    # --- Step 4: Generate R1 Report ---
    # R1: CUSTOMER, PLANT_SITE, ORDER_STATUS, PN, 총 주문 건수, 잔여 수량 (PN), 부족 PKID 개수, 결품 부품 상세
    
    # We need to link shortages back to the Order/Product level.
    # Join exploded with analysis_df to get shortage info for each component of each order
    r1_base = exploded.merge(
        analysis_df[['CHILD_PKID', 'PLANT_SITE', 'IS_SHORT', 'SHORTAGE_QTY']],
        on=['CHILD_PKID', 'PLANT_SITE'],
        how='left'
    )
    
    # Group by Product/Order level
    r1_report = r1_base.groupby(['CUSTOMER', 'PLANT_SITE', 'ORDER_STATUS', 'PN']).agg(
        TOTAL_ORDER_COUNT=('ORDER_KEY', 'nunique'),
        TOTAL_REMAINING_QTY=('REMAINING_QTY', 'sum'), # Sum of remaining qty for these products
        SHORT_PKID_COUNT=('CHILD_PKID', lambda x: x[r1_base.loc[x.index, 'IS_SHORT']].nunique()),
        SHORT_PKID_DETAILS=('CHILD_PKID', lambda x: ', '.join(sorted(x[r1_base.loc[x.index, 'IS_SHORT']].unique())))
    ).reset_index()
    
    # Rename columns to match request
    r1_report = r1_report.rename(columns={
        'TOTAL_ORDER_COUNT': '총 주문 건수',
        'TOTAL_REMAINING_QTY': '잔여 수량 (PN)',
        'SHORT_PKID_COUNT': '부족 PKID 개수',
        'SHORT_PKID_DETAILS': '결품 부품 상세'
    })
    
    # --- Step 5: Generate R2 Report (Wide Format) ---
    # R2: IS_URGENT, PKID, 결품 발생처, 총 소요량, 총 재고, 총 결품 수량, [ALL SITES] 소요량, [ALL SITES] 재고, 대체품...
    
    # Pivot for Wide Format
    # We use all_plant_sites to ensure all columns exist
    
    pivot_req = analysis_df.pivot(index='CHILD_PKID', columns='PLANT_SITE', values='REQUIRED_QTY')
    pivot_inv = analysis_df.pivot(index='CHILD_PKID', columns='PLANT_SITE', values='PKID_QTY')
    
    # Reindex with all sites (fill 0)
    pivot_req = pivot_req.reindex(columns=all_plant_sites, fill_value=0).add_suffix(' 소요량')
    pivot_inv = pivot_inv.reindex(columns=all_plant_sites, fill_value=0).add_suffix(' 재고')
    
    r2_wide = pd.concat([pivot_req, pivot_inv], axis=1)
    
    # Summary Columns
    summary_cols = analysis_df.groupby('CHILD_PKID').agg(
        TOTAL_REQ=('REQUIRED_QTY', 'sum'),
        TOTAL_INV=('PKID_QTY', 'sum'),
        TOTAL_SHORTAGE=('SHORTAGE_QTY', 'sum'),
        IS_URGENT=('IS_URGENT', 'max')
    )
    
    # Shortage Sites
    shortage_sites = analysis_df[analysis_df['SHORTAGE_QTY'] > 0].groupby('CHILD_PKID')['PLANT_SITE'].apply(lambda x: ', '.join(x))
    summary_cols['결품 발생처'] = shortage_sites
    
    # Merge
    r2_report = summary_cols.join(r2_wide, how='left')
    
    # Add Substitutes
    # Also need Substitute Inventory per site?
    # This is complex because Substitute Inventory is in Inventory_Master under SUBSTITUTE_PKID.
    # We need to query Inventory_Master for Substitute PKIDs.
    
    # Get unique substitute PKIDs
    sub_pkids = substitutes['SUBSTITUTE_PKID'].unique()
    if len(sub_pkids) > 0:
        sub_inv = inventory[inventory['PKID'].isin(sub_pkids)]
        # Aggregate sub inventory by PKID: "Site: Qty, Site: Qty"
        sub_inv['INV_STR'] = sub_inv['PLANT_SITE'] + ': ' + sub_inv['PKID_QTY'].astype(str)
        sub_inv_agg = sub_inv.groupby('PKID')['INV_STR'].apply(lambda x: ', '.join(x)).reset_index()
        sub_inv_agg.columns = ['SUBSTITUTE_PKID', 'SUB_INV_DETAILS']
        
        # Merge back to substitutes df
        substitutes_with_inv = substitutes.merge(sub_inv_agg, on='SUBSTITUTE_PKID', how='left')
        substitutes_with_inv['SUB_INV_DETAILS'] = substitutes_with_inv['SUB_INV_DETAILS'].fillna('재고 없음')
        
        # Now aggregate for R2
        subs_agg = substitutes_with_inv.groupby('CHILD_PKID').agg({
            'SUBSTITUTE_PKID': lambda x: ', '.join(x.dropna().astype(str)),
            'DESCRIPTION': lambda x: ', '.join(x.dropna().astype(str)),
            'SUB_INV_DETAILS': lambda x: ' | '.join(x.dropna().astype(str))
        }).rename(columns={
            'SUBSTITUTE_PKID': '추천 대체품', 
            'DESCRIPTION': '추천대체품 DESCRIPTION',
            'SUB_INV_DETAILS': '대체품 재고 현황 (SITE별)'
        })
        
        r2_report = r2_report.join(subs_agg, how='left')
    else:
        r2_report['추천 대체품'] = ''
        r2_report['추천대체품 DESCRIPTION'] = ''
        r2_report['대체품 재고 현황 (SITE별)'] = ''

    # Filter: Only show items with Shortage > 0
    r2_report = r2_report[r2_report['TOTAL_SHORTAGE'] > 0]
    
    r2_report = r2_report.reset_index()
    
    # Rename Summary Columns
    r2_report = r2_report.rename(columns={
        'CHILD_PKID': 'PKID',
        'TOTAL_REQ': '총 소요량',
        'TOTAL_INV': '총 재고',
        'TOTAL_SHORTAGE': '총 결품 수량'
    })
    
    # Reorder Columns
    # IS_URGENT, PKID, 결품 발생처, 총 소요량, 총 재고, 총 결품 수량
    fixed_cols = ['IS_URGENT', 'PKID', '결품 발생처', '총 소요량', '총 재고', '총 결품 수량']
    
    # Site Columns: [Site] 소요량, [Site] 재고 ...
    # We want them interleaved? Or all Req then all Inv?
    # Prompt says: [모든 PLANT_SITE 코드] 소요량... [모든 PLANT_SITE 코드] 재고...
    # So all Req columns first, then all Inv columns.
    
    site_req_cols = [f'{site} 소요량' for site in all_plant_sites]
    site_inv_cols = [f'{site} 재고' for site in all_plant_sites]
    
    sub_cols = ['추천 대체품', '추천대체품 DESCRIPTION', '대체품 재고 현황 (SITE별)']
    
    final_cols = fixed_cols + site_req_cols + site_inv_cols + sub_cols
    
    # Ensure all columns exist
    final_cols = [c for c in final_cols if c in r2_report.columns]
    
    r2_report = r2_report[final_cols]
    
    return r1_report, r2_report, None

def show_shortage_analysis():
    st.title("🚨 결품 분석 리포트 (Shortage Analysis)")
    
    st.info("""
    **분석 프로세스:**
    1. **필터 선택**: 고객사, 생산처, 주문 상태를 선택합니다.
    2. **분석 실행**: 선택된 조건에 맞는 데이터만 로드하여 분석합니다.
    3. **결과 확인**: R1(통합), R2(상세) 리포트를 확인하고 다운로드합니다.
    """)
    
    # --- 1. Pre-Filtering UI ---
    st.subheader("1. 분석 대상 필터 (Pre-Filtering)")
    
    avail_customers, avail_sites = get_filter_options()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        sel_customers = st.multiselect("고객사 (Customer)", avail_customers, default=avail_customers)
    with col2:
        sel_sites = st.multiselect("생산처 (Plant Site)", avail_sites, default=avail_sites)
    with col3:
        sel_statuses = st.multiselect("주문 상태 (Order Status)", ['OPEN', 'URGENT'], default=['OPEN', 'URGENT'])
    
    # Initialize Session State
    if 'sa_r1' not in st.session_state:
        st.session_state['sa_r1'] = None
    if 'sa_r2' not in st.session_state:
        st.session_state['sa_r2'] = None
    if 'sa_error' not in st.session_state:
        st.session_state['sa_error'] = None
    if 'sa_done' not in st.session_state:
        st.session_state['sa_done'] = False
        
    # --- 2. Run Analysis ---
    if st.button("결품 분석 실행 (Run Analysis)", type="primary"):
        if not sel_statuses:
            st.error("주문 상태를 최소 하나 이상 선택해주세요.")
        else:
            with st.spinner("데이터 로딩 및 분석 중... (Pre-Filtering Applied)"):
                r1, r2, error = perform_shortage_analysis(sel_customers, sel_sites, sel_statuses)
                
                st.session_state['sa_r1'] = r1
                st.session_state['sa_r2'] = r2
                st.session_state['sa_error'] = error
                st.session_state['sa_done'] = True
                st.rerun()
    
    # --- 3. Results ---
    if st.session_state['sa_done']:
        st.divider()
        r1 = st.session_state['sa_r1']
        r2 = st.session_state['sa_r2']
        error = st.session_state['sa_error']
        
        if error:
            st.error(error)
        else:
            # R1 Report
            st.subheader("R1. 고객사-생산처별 통합 결품 현황")
            if r1 is not None and not r1.empty:
                st.dataframe(r1, use_container_width=True)
                csv_r1 = r1.to_csv(index=False).encode('utf-8')
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
                csv_r2 = r2.to_csv(index=False).encode('utf-8')
                st.download_button("📥 R2 다운로드 (CSV)", csv_r2, f"R2_Shortage_Detail_{datetime.now().strftime('%Y%m%d')}.csv", "text/csv")
            else:
                st.info("조건에 맞는 결품 데이터가 없습니다.")
