import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import io
import duckdb
import tempfile
import os

# Page configuration
st.set_page_config(
    page_title="Campaign Analysis - Final Version",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
.main-header {
    font-size: 2.5rem;
    font-weight: bold;
    color: #1f77b4;
    text-align: center;
    margin-bottom: 2rem;
}
.input-form {
    background-color: #f8f9fa;
    padding: 20px;
    border-radius: 10px;
    margin: 20px 0;
    border: 2px solid #007bff;
}
.report-section {
    background-color: #ffffff;
    padding: 20px;
    border-radius: 10px;
    margin: 20px 0;
    border: 1px solid #dee2e6;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_and_convert_data(uploaded_file, file_type="ga"):
    """Load data and convert to parquet for faster processing"""
    try:
        # Load data
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
        
        # Preprocess based on file type
        if file_type == "ga":
            df = preprocess_ga_data(df)
        else:
            df = preprocess_shopify_data(df)
        
        return df
    except Exception as e:
        return None, str(e)

def preprocess_ga_data(df):
    """Preprocess GA data"""
    df = df.copy()
    
    # Parse date column
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    
    # Remove rows with invalid dates
    df = df.dropna(subset=['Date'])
    
    # Ensure numeric columns are numeric
    numeric_columns = ['Sessions', 'Total users', 'New users', 'Items viewed', 
                      'Add to carts', 'Total purchasers', 'Engaged sessions']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Clean up text columns
    text_columns = ['Session source']
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    
    return df

def preprocess_shopify_data(df):
    """Preprocess Shopify data"""
    df = df.copy()
    
    # Parse date column
    df['Day'] = pd.to_datetime(df['Day'], errors='coerce')
    
    # Remove rows with invalid dates
    df = df.dropna(subset=['Day'])
    
    # Ensure numeric columns are numeric
    numeric_columns = ['Net sales', 'Net items sold', 'Orders', 'Average order value', 
                      'Discounts', 'Gross margin', 'Customers', 'New customers']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    return df

def calculate_weeks_in_period(start_date, end_date):
    """Calculate number of weeks in a period, rounded to nearest whole number"""
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    days = (end - start).days + 1
    weeks = days / 7
    # Round to nearest whole number of weeks (minimum 1)
    return max(1, round(weeks))

def calculate_percentage_change(base_value, campaign_value):
    """Calculate percentage change between base and campaign values"""
    if base_value == 0:
        return "N/A" if campaign_value == 0 else "∞"
    
    change = ((campaign_value - base_value) / base_value) * 100
    return f"{change:+.1f}%"

def create_analysis_with_duckdb(ga_data, shopify_data, regions, 
                               base_week1_start, base_week1_end, base_week2_start, base_week2_end,
                               campaign_start, campaign_end, control_regions, google_sources, 
                               base_week_method, region_column, shopify_region_column):
    """Create analysis using DuckDB for faster processing"""
    
    # Initialize DuckDB connection
    conn = duckdb.connect()
    
    try:
        # Register dataframes with DuckDB
        conn.register('ga_data', ga_data)
        conn.register('shopify_data', shopify_data)
        
        # Calculate weeks for averaging (always rounded to nearest whole number)
        base_week1_weeks = calculate_weeks_in_period(base_week1_start, base_week1_end)
        base_week2_weeks = calculate_weeks_in_period(base_week2_start, base_week2_end)
        campaign_weeks = calculate_weeks_in_period(campaign_start, campaign_end)
        
        # Base weeks are ALWAYS averaged (divided by number of weeks)
        # Campaign can be averaged or summed based on user preference
        base1_divisor = base_week1_weeks  # Always divide base week 1 by its weeks
        base2_divisor = base_week2_weeks  # Always divide base week 2 by its weeks
        campaign_divisor = campaign_weeks if base_week_method == "Average (÷weeks)" else 1
        
        # Create Google sources filter
        google_sources_str = "', '".join(google_sources)
        google_filter = f'"Session source" IN (\'{google_sources_str}\')' if google_sources else "1=0"
        
        results = []
        
        # Process target regions
        target_regions = [r for r in regions if r not in control_regions]
        
        for region in target_regions:
            # GA Base Week 1 query
            ga_base1_query = f"""
            SELECT 
                SUM(Sessions) as total_sessions,
                SUM(CASE WHEN {google_filter} THEN Sessions ELSE 0 END) as google_sessions
            FROM ga_data 
            WHERE "{region_column}" = '{region}' 
            AND Date >= '{base_week1_start}' 
            AND Date <= '{base_week1_end}'
            """
            
            # GA Base Week 2 query
            ga_base2_query = f"""
            SELECT 
                SUM(Sessions) as total_sessions,
                SUM(CASE WHEN {google_filter} THEN Sessions ELSE 0 END) as google_sessions
            FROM ga_data 
            WHERE "{region_column}" = '{region}' 
            AND Date >= '{base_week2_start}' 
            AND Date <= '{base_week2_end}'
            """
            
            # GA Campaign query
            ga_campaign_query = f"""
            SELECT 
                SUM(Sessions) as total_sessions,
                SUM(CASE WHEN {google_filter} THEN Sessions ELSE 0 END) as google_sessions
            FROM ga_data 
            WHERE "{region_column}" = '{region}' 
            AND Date >= '{campaign_start}' 
            AND Date <= '{campaign_end}'
            """
            
            # Shopify queries
            shopify_base1_query = f"""
            SELECT SUM("Net sales") as net_sales
            FROM shopify_data 
            WHERE "{shopify_region_column}" = '{region}' 
            AND Day >= '{base_week1_start}' 
            AND Day <= '{base_week1_end}'
            """
            
            shopify_base2_query = f"""
            SELECT SUM("Net sales") as net_sales
            FROM shopify_data 
            WHERE "{shopify_region_column}" = '{region}' 
            AND Day >= '{base_week2_start}' 
            AND Day <= '{base_week2_end}'
            """
            
            shopify_campaign_query = f"""
            SELECT SUM("Net sales") as net_sales
            FROM shopify_data 
            WHERE "{shopify_region_column}" = '{region}' 
            AND Day >= '{campaign_start}' 
            AND Day <= '{campaign_end}'
            """
            
            # Execute queries
            ga_base1_result = conn.execute(ga_base1_query).fetchone()
            ga_base2_result = conn.execute(ga_base2_query).fetchone()
            ga_campaign_result = conn.execute(ga_campaign_query).fetchone()
            
            shopify_base1_result = conn.execute(shopify_base1_query).fetchone()
            shopify_base2_result = conn.execute(shopify_base2_query).fetchone()
            shopify_campaign_result = conn.execute(shopify_campaign_query).fetchone()
            
            # Calculate metrics with proper divisors (base weeks always averaged)
            sessions_total_base1 = (ga_base1_result[0] or 0) / base1_divisor
            sessions_total_base2 = (ga_base2_result[0] or 0) / base2_divisor
            sessions_total_campaign = (ga_campaign_result[0] or 0) / campaign_divisor
            
            sessions_google_base1 = (ga_base1_result[1] or 0) / base1_divisor
            sessions_google_base2 = (ga_base2_result[1] or 0) / base2_divisor
            sessions_google_campaign = (ga_campaign_result[1] or 0) / campaign_divisor
            
            net_sales_base1 = (shopify_base1_result[0] or 0) / base1_divisor
            net_sales_base2 = (shopify_base2_result[0] or 0) / base2_divisor
            net_sales_campaign = (shopify_campaign_result[0] or 0) / campaign_divisor
            
            # Calculate percentage changes
            sessions_total_change1 = calculate_percentage_change(sessions_total_base1, sessions_total_campaign)
            sessions_total_change2 = calculate_percentage_change(sessions_total_base2, sessions_total_campaign)
            sessions_google_change1 = calculate_percentage_change(sessions_google_base1, sessions_google_campaign)
            sessions_google_change2 = calculate_percentage_change(sessions_google_base2, sessions_google_campaign)
            net_sales_change1 = calculate_percentage_change(net_sales_base1, net_sales_campaign)
            net_sales_change2 = calculate_percentage_change(net_sales_base2, net_sales_campaign)
            
            results.append({
                'Region': region,
                'Sessions_Total_Base1': sessions_total_base1,
                'Sessions_Total_Base2': sessions_total_base2,
                'Sessions_Total_Campaign': sessions_total_campaign,
                'Sessions_Total_Change1': sessions_total_change1,
                'Sessions_Total_Change2': sessions_total_change2,
                'Sessions_Google_Base1': sessions_google_base1,
                'Sessions_Google_Base2': sessions_google_base2,
                'Sessions_Google_Campaign': sessions_google_campaign,
                'Sessions_Google_Change1': sessions_google_change1,
                'Sessions_Google_Change2': sessions_google_change2,
                'Net_Sales_Base1': net_sales_base1,
                'Net_Sales_Base2': net_sales_base2,
                'Net_Sales_Campaign': net_sales_campaign,
                'Net_Sales_Change1': net_sales_change1,
                'Net_Sales_Change2': net_sales_change2
            })
        
        return results, base1_divisor, base2_divisor, campaign_divisor, conn
        
    except Exception as e:
        conn.close()
        raise e

def process_control_regions_duckdb(conn, control_regions, google_sources, 
                                  base_week1_start, base_week1_end, base_week2_start, base_week2_end,
                                  campaign_start, campaign_end, region_column, shopify_region_column,
                                  base1_divisor, base2_divisor, campaign_divisor):
    """Process control regions using DuckDB aggregation"""
    
    if not control_regions:
        return None
    
    # Create control regions filter
    control_regions_str = "', '".join(control_regions)
    control_filter = f'"{region_column}" IN (\'{control_regions_str}\')'
    shopify_control_filter = f'"{shopify_region_column}" IN (\'{control_regions_str}\')'
    
    # Create Google sources filter
    google_sources_str = "', '".join(google_sources)
    google_filter = f'"Session source" IN (\'{google_sources_str}\')' if google_sources else "1=0"
    
    # Aggregate GA data for control regions
    ga_control_query = f"""
    SELECT 
        SUM(CASE WHEN Date >= '{base_week1_start}' AND Date <= '{base_week1_end}' THEN Sessions ELSE 0 END) as sessions_base1,
        SUM(CASE WHEN Date >= '{base_week2_start}' AND Date <= '{base_week2_end}' THEN Sessions ELSE 0 END) as sessions_base2,
        SUM(CASE WHEN Date >= '{campaign_start}' AND Date <= '{campaign_end}' THEN Sessions ELSE 0 END) as sessions_campaign,
        SUM(CASE WHEN Date >= '{base_week1_start}' AND Date <= '{base_week1_end}' AND {google_filter} THEN Sessions ELSE 0 END) as google_sessions_base1,
        SUM(CASE WHEN Date >= '{base_week2_start}' AND Date <= '{base_week2_end}' AND {google_filter} THEN Sessions ELSE 0 END) as google_sessions_base2,
        SUM(CASE WHEN Date >= '{campaign_start}' AND Date <= '{campaign_end}' AND {google_filter} THEN Sessions ELSE 0 END) as google_sessions_campaign
    FROM ga_data 
    WHERE {control_filter}
    """
    
    # Aggregate Shopify data for control regions
    shopify_control_query = f"""
    SELECT 
        SUM(CASE WHEN Day >= '{base_week1_start}' AND Day <= '{base_week1_end}' THEN "Net sales" ELSE 0 END) as sales_base1,
        SUM(CASE WHEN Day >= '{base_week2_start}' AND Day <= '{base_week2_end}' THEN "Net sales" ELSE 0 END) as sales_base2,
        SUM(CASE WHEN Day >= '{campaign_start}' AND Day <= '{campaign_end}' THEN "Net sales" ELSE 0 END) as sales_campaign
    FROM shopify_data 
    WHERE {shopify_control_filter}
    """
    
    # Execute queries
    ga_control_result = conn.execute(ga_control_query).fetchone()
    shopify_control_result = conn.execute(shopify_control_query).fetchone()
    
    # Calculate control region count and apply divisors
    control_region_count = len(control_regions)
    
    # Apply control region and week divisors (base weeks always averaged)
    sessions_total_base1 = (ga_control_result[0] or 0) / (control_region_count * base1_divisor)
    sessions_total_base2 = (ga_control_result[1] or 0) / (control_region_count * base2_divisor)
    sessions_total_campaign = (ga_control_result[2] or 0) / (control_region_count * campaign_divisor)
    
    sessions_google_base1 = (ga_control_result[3] or 0) / (control_region_count * base1_divisor)
    sessions_google_base2 = (ga_control_result[4] or 0) / (control_region_count * base2_divisor)
    sessions_google_campaign = (ga_control_result[5] or 0) / (control_region_count * campaign_divisor)
    
    net_sales_base1 = (shopify_control_result[0] or 0) / (control_region_count * base1_divisor)
    net_sales_base2 = (shopify_control_result[1] or 0) / (control_region_count * base2_divisor)
    net_sales_campaign = (shopify_control_result[2] or 0) / (control_region_count * campaign_divisor)
    
    # Calculate percentage changes
    sessions_total_change1 = calculate_percentage_change(sessions_total_base1, sessions_total_campaign)
    sessions_total_change2 = calculate_percentage_change(sessions_total_base2, sessions_total_campaign)
    sessions_google_change1 = calculate_percentage_change(sessions_google_base1, sessions_google_campaign)
    sessions_google_change2 = calculate_percentage_change(sessions_google_base2, sessions_google_campaign)
    net_sales_change1 = calculate_percentage_change(net_sales_base1, net_sales_campaign)
    net_sales_change2 = calculate_percentage_change(net_sales_base2, net_sales_campaign)
    
    return {
        'Region': 'Control set',
        'Sessions_Total_Base1': sessions_total_base1,
        'Sessions_Total_Base2': sessions_total_base2,
        'Sessions_Total_Campaign': sessions_total_campaign,
        'Sessions_Total_Change1': sessions_total_change1,
        'Sessions_Total_Change2': sessions_total_change2,
        'Sessions_Google_Base1': sessions_google_base1,
        'Sessions_Google_Base2': sessions_google_base2,
        'Sessions_Google_Campaign': sessions_google_campaign,
        'Sessions_Google_Change1': sessions_google_change1,
        'Sessions_Google_Change2': sessions_google_change2,
        'Net_Sales_Base1': net_sales_base1,
        'Net_Sales_Base2': net_sales_base2,
        'Net_Sales_Campaign': net_sales_campaign,
        'Net_Sales_Change1': net_sales_change1,
        'Net_Sales_Change2': net_sales_change2
    }

def create_display_dataframes(analysis_df, base1_label, base2_label, campaign_label):
    """Create formatted dataframes for display"""
    
    # Create Base Week 1 vs Campaign comparison
    df1 = pd.DataFrame()
    df1['Region'] = analysis_df['Region']
    df1[f'Sessions Total - {base1_label}'] = analysis_df['Sessions_Total_Base1'].apply(lambda x: f"{x:,.0f}")
    df1[f'Sessions Total - {campaign_label}'] = analysis_df['Sessions_Total_Campaign'].apply(lambda x: f"{x:,.0f}")
    df1['Sessions Total - %Change'] = analysis_df['Sessions_Total_Change1']
    df1[f'Sessions Google - {base1_label}'] = analysis_df['Sessions_Google_Base1'].apply(lambda x: f"{x:,.0f}")
    df1[f'Sessions Google - {campaign_label}'] = analysis_df['Sessions_Google_Campaign'].apply(lambda x: f"{x:,.0f}")
    df1['Sessions Google - %Change'] = analysis_df['Sessions_Google_Change1']
    df1[f'Net Sales - {base1_label}'] = analysis_df['Net_Sales_Base1'].apply(lambda x: f"${x:,.0f}")
    df1[f'Net Sales - {campaign_label}'] = analysis_df['Net_Sales_Campaign'].apply(lambda x: f"${x:,.0f}")
    df1['Net Sales - %Change'] = analysis_df['Net_Sales_Change1']
    
    # Create Base Week 2 vs Campaign comparison
    df2 = pd.DataFrame()
    df2['Region'] = analysis_df['Region']
    df2[f'Sessions Total - {base2_label}'] = analysis_df['Sessions_Total_Base2'].apply(lambda x: f"{x:,.0f}")
    df2[f'Sessions Total - {campaign_label}'] = analysis_df['Sessions_Total_Campaign'].apply(lambda x: f"{x:,.0f}")
    df2['Sessions Total - %Change'] = analysis_df['Sessions_Total_Change2']
    df2[f'Sessions Google - {base2_label}'] = analysis_df['Sessions_Google_Base2'].apply(lambda x: f"{x:,.0f}")
    df2[f'Sessions Google - {campaign_label}'] = analysis_df['Sessions_Google_Campaign'].apply(lambda x: f"{x:,.0f}")
    df2['Sessions Google - %Change'] = analysis_df['Sessions_Google_Change2']
    df2[f'Net Sales - {base2_label}'] = analysis_df['Net_Sales_Base2'].apply(lambda x: f"${x:,.0f}")
    df2[f'Net Sales - {campaign_label}'] = analysis_df['Net_Sales_Campaign'].apply(lambda x: f"${x:,.0f}")
    df2['Net Sales - %Change'] = analysis_df['Net_Sales_Change2']
    
    return df1, df2

def create_csv_export_data(df, base1_label, base2_label, campaign_label):
    """Create CSV data that matches the exact display format"""
    
    # Create the CSV content to match the dataframe format exactly
    csv_lines = []
    
    # First table: Base Week 1 vs Campaign
    csv_lines.append(f"Base Week 1 ({base1_label}) vs Campaign ({campaign_label}) Comparison")
    csv_lines.append("")
    
    # Headers for first table
    headers1 = [
        "Region",
        "Sessions (Total) - Base week",
        "Sessions (Total) - Campaign", 
        "Sessions (Total) - %change",
        "Sessions (Google) - Base week",
        "Sessions (Google) - Campaign",
        "Sessions (Google) - %change", 
        "Net Sales (Total) - Base week",
        "Net Sales (Total) - Campaign",
        "Net Sales (Total) - %change"
    ]
    csv_lines.append(','.join([f'"{h}"' for h in headers1]))
    
    # Data rows for first table
    for _, row in df.iterrows():
        data_row1 = [
            f'"{row["Region"]}"',
            f'"{row["Sessions_Total_Base1"]:,.0f}"',
            f'"{row["Sessions_Total_Campaign"]:,.0f}"',
            f'"{row["Sessions_Total_Change1"]}"',
            f'"{row["Sessions_Google_Base1"]:,.0f}"',
            f'"{row["Sessions_Google_Campaign"]:,.0f}"',
            f'"{row["Sessions_Google_Change1"]}"',
            f'"${row["Net_Sales_Base1"]:,.0f}"',
            f'"${row["Net_Sales_Campaign"]:,.0f}"',
            f'"{row["Net_Sales_Change1"]}"'
        ]
        csv_lines.append(','.join(data_row1))
    
    # Separator
    csv_lines.append("")
    csv_lines.append("")
    
    # Second table: Base Week 2 vs Campaign
    csv_lines.append(f"Base Week 2 ({base2_label}) vs Campaign ({campaign_label}) Comparison")
    csv_lines.append("")
    
    # Headers for second table
    headers2 = [
        "Region",
        "Sessions (Total) - Base week",
        "Sessions (Total) - Campaign", 
        "Sessions (Total) - %change",
        "Sessions (Google) - Base week",
        "Sessions (Google) - Campaign",
        "Sessions (Google) - %change", 
        "Net Sales (Total) - Base week",
        "Net Sales (Total) - Campaign",
        "Net Sales (Total) - %change"
    ]
    csv_lines.append(','.join([f'"{h}"' for h in headers2]))
    
    # Data rows for second table
    for _, row in df.iterrows():
        data_row2 = [
            f'"{row["Region"]}"',
            f'"{row["Sessions_Total_Base2"]:,.0f}"',
            f'"{row["Sessions_Total_Campaign"]:,.0f}"',
            f'"{row["Sessions_Total_Change2"]}"',
            f'"{row["Sessions_Google_Base2"]:,.0f}"',
            f'"{row["Sessions_Google_Campaign"]:,.0f}"',
            f'"{row["Sessions_Google_Change2"]}"',
            f'"${row["Net_Sales_Base2"]:,.0f}"',
            f'"${row["Net_Sales_Campaign"]:,.0f}"',
            f'"{row["Net_Sales_Change2"]}"'
        ]
        csv_lines.append(','.join(data_row2))
    
    return "\n".join(csv_lines)

def render_analysis_section(ga_data, shopify_data, section_id):
    """Render a complete analysis section with input form and report display"""
    
    # Initialize this section's data in session state if it doesn't exist
    if f'section_{section_id}' not in st.session_state:
        st.session_state[f'section_{section_id}'] = {
            'report_generated': False,
            'analysis_df': None,
            'config': None,
            'timestamp': None
        }
    
    section_data = st.session_state[f'section_{section_id}']
    
    # Input form (always visible and in the same place)
    st.markdown(f"""
    <div class="input-form">
        <h3>🔧 Analysis Configuration #{section_id}</h3>
    </div>
    """, unsafe_allow_html=True)
    
    # Get date range from GA data for defaults
    min_date = ga_data['Date'].min().date()
    max_date = ga_data['Date'].max().date()
    
    # Column selection
    st.subheader("📊 Column Configuration")
    col1, col2 = st.columns(2)
    
    with col1:
        ga_columns = list(ga_data.columns)
        region_column = st.selectbox(
            "Select Region Column from GA Data",
            options=ga_columns,
            index=next((i for i, col in enumerate(ga_columns) if 'region' in col.lower()), 0),
            help="Select the column that contains region information",
            key=f"region_col_{section_id}"
        )
    
    with col2:
        shopify_columns = list(shopify_data.columns)
        shopify_region_column = st.selectbox(
            "Select Region Column from Shopify Data",
            options=shopify_columns,
            index=next((i for i, col in enumerate(shopify_columns) if 'region' in col.lower()), 0),
            help="Select the column that contains region information in Shopify data",
            key=f"shopify_region_col_{section_id}"
        )
    
    # Session source configuration
    st.subheader("🔍 Session Source Configuration")
    
    all_sources = sorted(list(ga_data['Session source'].unique())) if 'Session source' in ga_data.columns else []
    
    google_sources = st.multiselect(
        "Select Google Session Sources",
        options=all_sources,
        default=[source for source in all_sources if 'google' in source.lower()],
        help="Select which session sources should be counted as Google sessions",
        key=f"google_sources_{section_id}"
    )
    
    # Base week calculation method
    st.subheader("📊 Calculation Method")
    base_week_method = st.radio(
        "Campaign Period Calculation",
        options=["Average (÷weeks)", "Sum (Total)"],
        index=0,
        help="Base weeks are ALWAYS averaged by number of weeks. Choose how to handle campaign period.",
        key=f"base_week_method_{section_id}"
    )
    
    st.info("ℹ️ Base weeks are automatically averaged by their respective number of weeks (rounded to nearest whole number)")
    
    # Period configuration
    st.subheader("📅 Period Configuration")
    
    st.write(f"**Available Date Range:** {min_date} to {max_date}")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.write("**Base Week 1:**")
        base_week1_start = st.date_input(
            "Base Week 1 Start", 
            value=min_date, 
            min_value=min_date, 
            max_value=max_date,
            key=f"base1_start_{section_id}"
        )
        base_week1_end = st.date_input(
            "Base Week 1 End", 
            value=min_date + timedelta(days=20), 
            min_value=min_date, 
            max_value=max_date,
            key=f"base1_end_{section_id}"
        )
    
    with col2:
        st.write("**Base Week 2:**")
        base_week2_start = st.date_input(
            "Base Week 2 Start", 
            value=min_date + timedelta(days=365), 
            min_value=min_date, 
            max_value=max_date,
            key=f"base2_start_{section_id}"
        )
        base_week2_end = st.date_input(
            "Base Week 2 End", 
            value=min_date + timedelta(days=385), 
            min_value=min_date, 
            max_value=max_date,
            key=f"base2_end_{section_id}"
        )
    
    with col3:
        st.write("**Campaign Period:**")
        campaign_start = st.date_input(
            "Campaign Start", 
            value=min_date + timedelta(days=21), 
            min_value=min_date, 
            max_value=max_date,
            key=f"campaign_start_{section_id}"
        )
        campaign_end = st.date_input(
            "Campaign End", 
            value=min_date + timedelta(days=27), 
            min_value=min_date, 
            max_value=max_date,
            key=f"campaign_end_{section_id}"
        )
    
    # Validation
    if base_week1_start > base_week1_end:
        st.error("Base week 1 start date must be before end date")
        return
    if base_week2_start > base_week2_end:
        st.error("Base week 2 start date must be before end date")
        return
    if campaign_start > campaign_end:
        st.error("Campaign start date must be before end date")
        return
    
    # Show week calculations
    st.subheader("📊 Week Calculations")
    
    base1_weeks = calculate_weeks_in_period(base_week1_start, base_week1_end)
    base2_weeks = calculate_weeks_in_period(base_week2_start, base_week2_end)
    campaign_weeks_calc = calculate_weeks_in_period(campaign_start, campaign_end)
    
    calc_col1, calc_col2, calc_col3 = st.columns(3)
    
    with calc_col1:
        base1_days = (base_week1_end - base_week1_start).days + 1
        st.write(f"**Base Week 1:** {base1_days} days → {base1_weeks} weeks (averaged)")
    
    with calc_col2:
        base2_days = (base_week2_end - base_week2_start).days + 1
        st.write(f"**Base Week 2:** {base2_days} days → {base2_weeks} weeks (averaged)")
    
    with calc_col3:
        campaign_days = (campaign_end - campaign_start).days + 1
        campaign_method = "averaged" if base_week_method == "Average (÷weeks)" else "total"
        st.write(f"**Campaign:** {campaign_days} days → {campaign_weeks_calc} weeks ({campaign_method})")
    
    # Labels
    st.subheader("🏷️ Period Labels")
    label_col1, label_col2, label_col3 = st.columns(3)
    
    with label_col1:
        base1_label = st.text_input("Base Week 1 Label", value="Base week 25", key=f"base1_label_{section_id}")
    with label_col2:
        base2_label = st.text_input("Base Week 2 Label", value="Base week 26", key=f"base2_label_{section_id}")
    with label_col3:
        campaign_label = st.text_input("Campaign Label", value="Campaign - Week 1", key=f"campaign_label_{section_id}")
    
    # Region selection
    st.subheader("🌍 Region Configuration")
    
    available_regions = sorted(list(ga_data[region_column].unique())) if region_column in ga_data.columns else []
    
    st.write(f"**Available Regions from '{region_column}' ({len(available_regions)}):**")
    if len(available_regions) <= 10:
        st.write(", ".join(available_regions))
    else:
        st.write(f"{', '.join(available_regions[:10])}... and {len(available_regions)-10} more")
    
    region_col1, region_col2 = st.columns(2)
    
    with region_col1:
        selected_regions = st.multiselect(
            "Select Target Regions",
            options=available_regions,
            default=available_regions[:3] if len(available_regions) >= 3 else available_regions,
            help="Select regions to include in the analysis",
            key=f"selected_regions_{section_id}"
        )
    
    with region_col2:
        control_regions = st.multiselect(
            "Select Control Regions",
            options=available_regions,
            help="Select which regions should be labeled as 'Control set'",
            key=f"control_regions_{section_id}"
        )
    
    if not selected_regions:
        st.warning("Please select at least one region for analysis.")
        return
    
    # Generate/Update analysis button
    button_col1, button_col2 = st.columns(2)
    
    with button_col1:
        if section_data['report_generated']:
            generate_button = st.button("🔄 Update Report", type="primary", key=f"update_{section_id}")
        else:
            generate_button = st.button("🚀 Generate Analysis", type="primary", key=f"generate_{section_id}")
    
    with button_col2:
        if section_data['report_generated']:
            if st.button("📊 Generate Another Report", type="secondary", key=f"generate_another_{section_id}"):
                # Add a new section
                if 'next_section_id' not in st.session_state:
                    st.session_state.next_section_id = 2
                else:
                    st.session_state.next_section_id += 1
                
                if 'active_sections' not in st.session_state:
                    st.session_state.active_sections = [1]
                
                st.session_state.active_sections.append(st.session_state.next_section_id)
                st.rerun()
    
    # Generate analysis if button clicked
    if generate_button:
        with st.spinner("Generating analysis..."):
            try:
                # Create analysis using DuckDB
                results, base1_divisor, base2_divisor, campaign_divisor, conn = create_analysis_with_duckdb(
                    ga_data, shopify_data, selected_regions,
                    base_week1_start, base_week1_end, 
                    base_week2_start, base_week2_end,
                    campaign_start, campaign_end, 
                    control_regions, google_sources, 
                    base_week_method, region_column, 
                    shopify_region_column
                )
                
                # Process control regions if any
                if control_regions:
                    control_result = process_control_regions_duckdb(
                        conn, control_regions, google_sources,
                        base_week1_start, base_week1_end, 
                        base_week2_start, base_week2_end,
                        campaign_start, campaign_end, 
                        region_column, shopify_region_column,
                        base1_divisor, base2_divisor, campaign_divisor
                    )
                    if control_result:
                        results.append(control_result)
                
                # Close DuckDB connection
                conn.close()
                
                # Convert to DataFrame
                analysis_df = pd.DataFrame(results)
                
                # Store the results in session state
                st.session_state[f'section_{section_id}'] = {
                    'report_generated': True,
                    'analysis_df': analysis_df,
                    'config': {
                        'region_column': region_column,
                        'shopify_region_column': shopify_region_column,
                        'google_sources': google_sources,
                        'base_week_method': base_week_method,
                        'base_week1_start': base_week1_start,
                        'base_week1_end': base_week1_end,
                        'base_week2_start': base_week2_start,
                        'base_week2_end': base_week2_end,
                        'campaign_start': campaign_start,
                        'campaign_end': campaign_end,
                        'base1_label': base1_label,
                        'base2_label': base2_label,
                        'campaign_label': campaign_label,
                        'selected_regions': selected_regions,
                        'control_regions': control_regions
                    },
                    'timestamp': datetime.now()
                }
                
                st.success(f"✅ Analysis #{section_id} generated successfully!")
                st.rerun()
                
            except Exception as e:
                st.error(f"Error generating analysis: {str(e)}")
                import traceback
                st.code(traceback.format_exc())
    
    # Display report if it exists (right below the input form)
    if section_data['report_generated']:
        st.markdown("---")
        
        # Report header
        st.markdown(f"""
        <div class="report-section">
            <h3>📊 Analysis Report #{section_id}</h3>
            <p><strong>Generated:</strong> {section_data['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
        """, unsafe_allow_html=True)
        
        analysis_df = section_data['analysis_df']
        config = section_data['config']
        
        # Show configuration summary
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**📅 Period Configuration:**")
            st.write(f"• Base Week 1: {config['base_week1_start']} to {config['base_week1_end']}")
            st.write(f"• Base Week 2: {config['base_week2_start']} to {config['base_week2_end']}")
            st.write(f"• Campaign: {config['campaign_start']} to {config['campaign_end']}")
            
        with col2:
            st.write("**🌍 Region Configuration:**")
            st.write(f"• Target Regions: {', '.join(config['selected_regions'])}")
            if config['control_regions']:
                st.write(f"• Control Regions: {', '.join(config['control_regions'])}")
            st.write(f"• Google Sources: {len(config['google_sources'])} selected")
            st.write(f"• Method: {config['base_week_method']}")
        
        # Create display dataframes
        df1, df2 = create_display_dataframes(analysis_df, config['base1_label'], config['base2_label'], config['campaign_label'])
        
        # Display tables
        st.subheader(f"📊 {config['base1_label']} vs {config['campaign_label']} Comparison")
        st.dataframe(df1, use_container_width=True)
        
        st.subheader(f"📊 {config['base2_label']} vs {config['campaign_label']} Comparison")
        st.dataframe(df2, use_container_width=True)
        
        # Create CSV data for download
        csv_data = create_csv_export_data(analysis_df, config['base1_label'], config['base2_label'], config['campaign_label'])
        
        # Download button
        st.download_button(
            label=f"📥 Download Report #{section_id} as CSV",
            data=csv_data,
            file_name=f"campaign_analysis_report_{section_id}_{section_data['timestamp'].strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            key=f"download_report_{section_id}"
        )
        
        # Show summary statistics
        st.markdown("### 📊 Summary Statistics")
        
        # Calculate summary stats
        target_regions = [r for r in analysis_df['Region'].tolist() if r != 'Control set']
        control_regions_list = [r for r in analysis_df['Region'].tolist() if r == 'Control set']
        
        summary_col1, summary_col2, summary_col3 = st.columns(3)
        
        with summary_col1:
            st.metric("Target Regions", len(target_regions))
            st.metric("Control Regions", len(control_regions_list))
        
        with summary_col2:
            # Calculate average changes for Base Week 1 vs Campaign
            base1_changes = []
            for _, row in analysis_df.iterrows():
                if row['Sessions_Total_Change1'] != 'N/A' and row['Sessions_Total_Change1'] != '∞':
                    try:
                        change_val = float(row['Sessions_Total_Change1'].replace('%', '').replace('+', ''))
                        base1_changes.append(change_val)
                    except:
                        pass
            
            if base1_changes:
                avg_change_base1 = sum(base1_changes) / len(base1_changes)
                st.metric("Avg Sessions Change (Base1)", f"{avg_change_base1:+.1f}%")
        
        with summary_col3:
            # Calculate average changes for Base Week 2 vs Campaign
            base2_changes = []
            for _, row in analysis_df.iterrows():
                if row['Sessions_Total_Change2'] != 'N/A' and row['Sessions_Total_Change2'] != '∞':
                    try:
                        change_val = float(row['Sessions_Total_Change2'].replace('%', '').replace('+', ''))
                        base2_changes.append(change_val)
                    except:
                        pass
            
            if base2_changes:
                avg_change_base2 = sum(base2_changes) / len(base2_changes)
                st.metric("Avg Sessions Change (Base2)", f"{avg_change_base2:+.1f}%")

def main():
    # Header
    st.markdown('<h1 class="main-header">📊 Campaign Analysis - Final Version</h1>', unsafe_allow_html=True)
    
    # Initialize session state
    if 'active_sections' not in st.session_state:
        st.session_state.active_sections = [1]  # Start with section 1
    if 'next_section_id' not in st.session_state:
        st.session_state.next_section_id = 2
    
    # Sidebar for file uploads ONLY
    st.sidebar.header("📁 Dataset Upload")
    
    # GA data upload
    ga_file = st.sidebar.file_uploader(
        "Upload Merged GA Data (CSV/Excel/Parquet)",
        type=['csv', 'xlsx', 'xls', 'parquet'],
        help="Upload your merged Google Analytics data file",
        key="ga_upload"
    )
    
    # Shopify data upload
    shopify_file = st.sidebar.file_uploader(
        "Upload Shopify Data (CSV/Excel/Parquet)",
        type=['csv', 'xlsx', 'xls', 'parquet'],
        help="Upload your Shopify data file",
        key="shopify_upload"
    )
    
    # Data preview in sidebar
    if ga_file and shopify_file:
        st.sidebar.markdown("---")
        st.sidebar.header("📊 Dataset Info")
        
        # Load data with caching
        with st.spinner("Loading and optimizing data..."):
            ga_data = load_and_convert_data(ga_file, "ga")
            shopify_data = load_and_convert_data(shopify_file, "shopify")
            
            if ga_data is None:
                st.error("Error loading GA data")
                return
            if shopify_data is None:
                st.error("Error loading Shopify data")
                return
        
        st.sidebar.success("✅ Data loaded!")
        st.sidebar.write(f"**GA Data:** {len(ga_data):,} rows")
        st.sidebar.write(f"**Shopify Data:** {len(shopify_data):,} rows")
        
        if not ga_data.empty:
            min_date = ga_data['Date'].min().date()
            max_date = ga_data['Date'].max().date()
            st.sidebar.write(f"**Date Range:** {min_date} to {max_date}")
    
    # Main content area
    if ga_file is None or shopify_file is None:
        st.info("👆 Please upload both merged GA data and Shopify data files in the sidebar to begin analysis")
        
        st.markdown("""
        ### Expected Data Formats:
        
        **Merged GA Data should contain:**
        - Date, Region, Session source, Sessions, Total users, New users
        - Items viewed, Add to carts, Total purchasers, Engaged sessions
        - Average session duration, Items added to cart
        
        **Shopify Data should contain:**
        - Day, Shipping postal code, Shipping region, Net sales, Net items sold
        - Orders, Average order value, Discounts, Gross margin
        - Customers, New customers, DMA
        
        ### Performance Tips:
        - **Parquet files** are processed much faster than CSV/Excel
        - Use parquet format for large datasets (>100MB)
        - DuckDB optimization provides 10-100x speedup for analytical queries
        """)
        return
    
    # Render all active analysis sections
    for section_id in st.session_state.active_sections:
        render_analysis_section(ga_data, shopify_data, section_id)
        
        # Add separator between sections (except for the last one)
        if section_id != st.session_state.active_sections[-1]:
            st.markdown("---")
            st.markdown("---")
    
    # Data preview
    if ga_file and shopify_file:
        with st.expander("👀 Data Preview"):
            tab1, tab2 = st.tabs(["GA Data", "Shopify Data"])
            
            with tab1:
                if not ga_data.empty:
                    st.write(f"**Date Range:** {ga_data['Date'].min()} to {ga_data['Date'].max()}")
                    st.write(f"**Memory Usage:** {ga_data.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
                    st.dataframe(ga_data.head(10), use_container_width=True)
            
            with tab2:
                if not shopify_data.empty:
                    st.write(f"**Memory Usage:** {shopify_data.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
                    st.dataframe(shopify_data.head(10), use_container_width=True)

if __name__ == "__main__":
    main()