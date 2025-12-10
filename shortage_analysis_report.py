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
    """Get available customers and plant sites from Product_Master for filtering"""
    conn = get_db_connection()
    try:
        query = "SELECT DISTINCT CUSTOMER, PLANT_SITE FROM Product_Master ORDER BY CUSTOMER, PLANT_SITE"
        df = pd.read_sql_query(query, conn)
        df.columns = df.columns.str.upper()
        
        customers = sorted(df['CUSTOMER'].dropna().unique().tolist())
        sites = sorted(df['PLANT_SITE'].dropna().unique().tolist())
        
        return customers, sites
    except Exception as e:
        st.error(f"Error loading filter options: {e}")
        return [], []
    finally:
        conn.close()


def load_data(target_customers, target_sites, target_statuses):
    """
    Load all required data for shortage analysis with pre-filtering
    """
    conn = get_db_connection()
    
    try:
        # Build filter conditions
        customer_filter = "(" + ",".join([f"'{c}'" for c in target_customers]) + ")" if target_customers else "('')"
        site_filter = "(" + ",".join([f"'{s}'" for s in target_sites]) + ")" if target_sites else "('')"
        status_filter = "(" + ",".join([f"'{s}'" for s in target_statuses]) + ")" if target_statuses else "('')"
        
        # Load Orders (filtered by status)
        orders_query = f"""
            SELECT ORDER_KEY, PN, ORDER_QTY, DELIVERED_QTY, ORDER_DATE, ORDER_STATUS, URGENT_FLAG
            FROM AS_Order
            WHERE ORDER_STATUS IN {status_filter}
        """
        orders = pd.read_sql_query(orders_query, conn)
        orders.columns = orders.columns.str.upper()
        
        # Load Products
        products_query = f"""
            SELECT PN, PART_NAME, CUSTOMER, PLANT_SITE
            FROM Product_Master
            WHERE CUSTOMER IN {customer_filter} AND PLANT_SITE IN {site_filter}
        """
        products = pd.read_sql_query(products_query, conn)
        products.columns = products.columns.str.upper()
        
        # Load BOM
        bom_query = "SELECT PARENT_PN, CHILD_PKID, BOM_QTY FROM BOM_Master"
        bom = pd.read_sql_query(bom_query, conn)
        bom.columns = bom.columns.str.upper()
        
        # Load Inventory
        inv_query = """
            SELECT PKID, PLANT_SITE, PKID_QTY, SNAPSHOT_DATE
            FROM Inventory_Master
            WHERE SNAPSHOT_DATE = (SELECT MAX(SNAPSHOT_DATE) FROM Inventory_Master)
        """
        inventory = pd.read_sql_query(inv_query, conn)
        inventory.columns = inventory.columns.str.upper()
        snapshot_date = inventory['SNAPSHOT_DATE'].iloc[0] if not inventory.empty else None
        
        # Load Substitutes
        sub_query = "SELECT CHILD_PKID, SUBSTITUTE_PKID, DESCRIPTION FROM Substitute_Master"
        substitutes = pd.read_sql_query(sub_query, conn)
        substitutes.columns = substitutes.columns.str.upper()
        
        # Get all plant sites
        all_plant_sites = sorted(products['PLANT_SITE'].unique().tolist()) if not products.empty else []
        
        return orders, products, bom, inventory, substitutes, snapshot_date, all_plant_sites
        
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), None, []
    finally:
        conn.close()


def perform_shortage_analysis(target_customers, target_sites, target_statuses):
    """
    Core Logic with Pre-Filtering and Corrected Aggregation
    """
    orders, products, bom, inventory, substitutes, snapshot_date, all_plant_sites = load_data(target_customers, target_sites, target_statuses)
    
    if orders.empty:
        return None, None, None, "No orders found matching the status criteria."
    if products.empty:
        return None, None, None, "No products found matching the customer/site criteria."
    
    # --- Step 1: Prepare Demand Data ---
    
    # Join Orders with Product Info
    order_details = orders.merge(products, on='PN', how='inner')
    
    if order_details.empty:
        return None, None, None, "No matching orders found for the selected customers/sites."
    
    # Calculate Remaining Qty per Order
    order_details['REMAINING_QTY'] = order_details['ORDER_QTY'] - order_details['DELIVERED_QTY']
    order_details['REMAINING_QTY'] = order_details['REMAINING_QTY'].clip(lower=0)
    
    # Explode BOM (Creates duplication of orders per component)
    exploded = order_details.merge(bom, left_on='PN', right_on='PARENT_PN', how='inner')
    
    if exploded.empty:
        return None, None, None, "No BOM data found for the selected products."
    
    # Calculate Component Demand
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
    
    # --- Step 4: Generate R1 Report (Corrected Aggregation) ---
    
    # 4-1. 기본 정보 집계 (Order Detail 레벨에서 집계하여 중복 방지)
    # PN별 총 주문수량, 총 잔여수량 계산
    r1_stats = order_details.groupby(['CUSTOMER', 'PLANT_SITE', 'ORDER_STATUS', 'PN']).agg(
        TOTAL_ORDER_QTY=('ORDER_QTY', 'sum'),
        TOTAL_REMAINING_QTY=('REMAINING_QTY', 'sum')
    ).reset_index()
    
    # 4-2. 결품 정보 맵핑
    # exploded 데이터에 결품 여부(IS_SHORT)를 붙임
    r1_base = exploded.merge(
        analysis_df[['CHILD_PKID', 'PLANT_SITE', 'IS_SHORT']],
        on=['CHILD_PKID', 'PLANT_SITE'],
        how='left'
    )
    
    # 그룹별로 결품인 PKID 개수와 상세 내용 집계
    r1_shortage = r1_base.groupby(['CUSTOMER', 'PLANT_SITE', 'ORDER_STATUS', 'PN']).agg(
        SHORT_PKID_COUNT=('CHILD_PKID', lambda x: x[r1_base.loc[x.index, 'IS_SHORT']].nunique()),
        SHORT_PKID_DETAILS=('CHILD_PKID', lambda x: ', '.join(sorted(x[r1_base.loc[x.index, 'IS_SHORT']].unique())))
    ).reset_index()
    
    # 4-3. 두 결과 병합
    r1_report = r1_stats.merge(r1_shortage, on=['CUSTOMER', 'PLANT_SITE', 'ORDER_STATUS', 'PN'], how='left')
    
    # 컬럼명 정리
    r1_report = r1_report.rename(columns={
        'TOTAL_ORDER_QTY': '총 주문 수량',
        'TOTAL_REMAINING_QTY': '총 잔여 수량 (PN)',
        'SHORT_PKID_COUNT': '부족 PKID 개수',
        'SHORT_PKID_DETAILS': '결품 부품 상세'
    })
    
    # --- Step 5: Generate R2 Report (Wide Format with 0 Fill) ---
    pivot_req = analysis_df.pivot(index='CHILD_PKID', columns='PLANT_SITE', values='REQUIRED_QTY')
    pivot_inv = analysis_df.pivot(index='CHILD_PKID', columns='PLANT_SITE', values='PKID_QTY')
    
    pivot_req = pivot_req.reindex(columns=all_plant_sites, fill_value=0).add_suffix(' 소요량')
    pivot_inv = pivot_inv.reindex(columns=all_plant_sites, fill_value=0).add_suffix(' 재고')
    
    r2_wide = pd.concat([pivot_req, pivot_inv], axis=1)
    
    summary_cols = analysis_df.groupby('CHILD_PKID').agg(
        TOTAL_REQ=('REQUIRED_QTY', 'sum'),
        TOTAL_INV=('PKID_QTY', 'sum'),
        TOTAL_SHORTAGE=('SHORTAGE_QTY', 'sum'),
        IS_URGENT=('IS_URGENT', 'max')
    )
    
    shortage_sites = analysis_df[analysis_df['SHORTAGE_QTY'] > 0].groupby('CHILD_PKID')['PLANT_SITE'].apply(lambda x: ', '.join(x))
    summary_cols['결품 발생처'] = shortage_sites
    
    r2_report = summary_cols.join(r2_wide, how='left')
    
    # Substitutes logic
    sub_pkids = substitutes['SUBSTITUTE_PKID'].unique()
    if len(sub_pkids) > 0:
        sub_inv = inventory[inventory['PKID'].isin(sub_pkids)]
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
        
        r2_report = r2_report.join(subs_agg, how='left')
    else:
        r2_report['추천 대체품'] = ''
        r2_report['추천대체품 DESCRIPTION'] = ''
        r2_report['대체품 재고 현황 (SITE별)'] = ''

    r2_report = r2_report[r2_report['TOTAL_SHORTAGE'] > 0].reset_index()
    
    r2_report = r2_report.rename(columns={
        'CHILD_PKID': 'PKID',
        'TOTAL_REQ': '총 소요량',
        'TOTAL_INV': '총 재고',
        'TOTAL_SHORTAGE': '총 결품 수량'
    })
    
    # Fill NaN with 0 for numeric/main columns (Text columns are excluded)
    # Define columns that should strictly be text/empty if null
    exclude_fill_cols = ['PKID', '결품 발생처', '추천 대체품', '추천대체품 DESCRIPTION', '대체품 재고 현황 (SITE별)']
    cols_to_fill = [c for c in r2_report.columns if c not in exclude_fill_cols]
    r2_report[cols_to_fill] = r2_report[cols_to_fill].fillna(0)
    
    # Reorder Columns
    fixed_cols = ['IS_URGENT', 'PKID', '결품 발생처', '총 소요량', '총 재고', '총 결품 수량']
    site_req_cols = [f'{site} 소요량' for site in all_plant_sites]
    site_inv_cols = [f'{site} 재고' for site in all_plant_sites]
    sub_cols = ['추천 대체품', '추천대체품 DESCRIPTION', '대체품 재고 현황 (SITE별)']
    
    final_cols = fixed_cols + site_req_cols + site_inv_cols + sub_cols
    # Ensure columns exist before selecting
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
            with st.spinner("데이터 로딩 및 분석 중..."):
                r1, r2, r3, error = perform_shortage_analysis(sel_customers, sel_sites, sel_statuses)
                
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
                # UTF-8-SIG Encoding for Korean CSV support
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
                # UTF-8-SIG Encoding for Korean CSV support
                csv_r2 = r2.to_csv(index=False).encode('utf-8-sig')
                st.download_button("📥 R2 다운로드 (CSV)", csv_r2, f"R2_Shortage_Detail_{datetime.now().strftime('%Y%m%d')}.csv", "text/csv")
            else:
                st.info("조건에 맞는 결품 데이터가 없습니다.")
