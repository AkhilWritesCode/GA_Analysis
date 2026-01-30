import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import io

# Page configuration
st.set_page_config(
    page_title="Campaign Analysis - Merged GA Data",
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
.analysis-table {
    border-collapse: collapse;
    width: 100%;
    margin: 20px 0;
    font-size: 12px;
    font-family: Arial, sans-serif;
}
.analysis-table th, .analysis-table td {
    border: 1px solid #333;
    padding: 8px;
    text-align: center;
    vertical-align: middle;
}
.analysis-table th {
    background-color: #f2f2f2;
    font-weight: bold;
}
.sessions-total { background-color: #e6f3ff; }
.sessions-google { background-color: #fff2cc; }
.net-sales { background-color: #e8f5e8; }
.region-row { background-color: #f0f0f0; font-weight: bold; }
.control-row { background-color: #f8f8f8; }
.summary-section {
    margin-top: 30px;
    padding: 20px;
    background-color: #f9f9f9;
    border-radius: 10px;
}
</style>
""", unsafe_allow_html=True)

def load_data(uploaded_file):
    """Load data from uploaded CSV or Excel file"""
    try:
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
        return df, None
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
    text_columns = ['Region', 'Session source']
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
    
    # Clean up text columns
    if 'Shipping region' in df.columns:
        df['Shipping region'] = df['Shipping region'].astype(str).str.strip()
    
    return df

def filter_data_by_period(df, date_col, start_date, end_date):
    """Filter dataframe by date period"""
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    mask = (df[date_col] >= start) & (df[date_col] <= end)
    return df[mask]

def calculate_weeks_in_period(start_date, end_date):
    """Calculate number of weeks in a period"""
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    days = (end - start).days + 1
    weeks = days / 7
    return max(1, weeks)  # At least 1 week

def calculate_percentage_change(base_value, campaign_value):
    """Calculate percentage change between base and campaign values"""
    if base_value == 0:
        return "N/A" if campaign_value == 0 else "∞"
    
    change = ((campaign_value - base_value) / base_value) * 100
    return f"{change:+.1f}%"
def create_analysis_table(ga_data, shopify_data, regions, 
                         base_week1_start, base_week1_end, base_week2_start, base_week2_end,
                         campaign_start, campaign_end, control_regions, google_sources, 
                         base_week_method, region_column, shopify_region_column):
    """Create the main analysis table with merged GA data and flexible date ranges"""
    
    results = []
    
    # Calculate weeks for averaging
    base_week1_weeks = calculate_weeks_in_period(base_week1_start, base_week1_end)
    base_week2_weeks = calculate_weeks_in_period(base_week2_start, base_week2_end)
    campaign_weeks = calculate_weeks_in_period(campaign_start, campaign_end)
    
    # Determine base week divisors based on user selection
    base1_divisor = base_week1_weeks if base_week_method == "Average (÷weeks)" else 1
    base2_divisor = base_week2_weeks if base_week_method == "Average (÷weeks)" else 1
    
    # Process target regions (non-control)
    target_regions = [r for r in regions if r not in control_regions]
    
    for region in target_regions:
        # Filter GA data for this region using selected column
        ga_region = ga_data[ga_data[region_column] == region]
        shopify_region_data = shopify_data[shopify_data[shopify_region_column] == region]
        
        # Base Week 1 data
        ga_base1 = filter_data_by_period(ga_region, 'Date', base_week1_start, base_week1_end)
        shopify_base1 = filter_data_by_period(shopify_region_data, 'Day', base_week1_start, base_week1_end)
        
        # Base Week 2 data
        ga_base2 = filter_data_by_period(ga_region, 'Date', base_week2_start, base_week2_end)
        shopify_base2 = filter_data_by_period(shopify_region_data, 'Day', base_week2_start, base_week2_end)
        
        # Campaign data
        ga_campaign = filter_data_by_period(ga_region, 'Date', campaign_start, campaign_end)
        shopify_campaign = filter_data_by_period(shopify_region_data, 'Day', campaign_start, campaign_end)
        
        # Calculate Base Week 1 metrics
        sessions_total_base1 = ga_base1['Sessions'].sum() / base1_divisor
        ga_google_base1 = ga_base1[ga_base1['Session source'].isin(google_sources)]
        sessions_google_base1 = ga_google_base1['Sessions'].sum() / base1_divisor
        net_sales_base1 = shopify_base1['Net sales'].sum() / base1_divisor
        
        # Calculate Base Week 2 metrics
        sessions_total_base2 = ga_base2['Sessions'].sum() / base2_divisor
        ga_google_base2 = ga_base2[ga_base2['Session source'].isin(google_sources)]
        sessions_google_base2 = ga_google_base2['Sessions'].sum() / base2_divisor
        net_sales_base2 = shopify_base2['Net sales'].sum() / base2_divisor
        
        # Calculate Campaign metrics
        sessions_total_campaign = ga_campaign['Sessions'].sum() / campaign_weeks
        ga_google_campaign = ga_campaign[ga_campaign['Session source'].isin(google_sources)]
        sessions_google_campaign = ga_google_campaign['Sessions'].sum() / campaign_weeks
        net_sales_campaign = shopify_campaign['Net sales'].sum() / campaign_weeks
        
        # Calculate percentage changes
        sessions_total_change1 = calculate_percentage_change(sessions_total_base1, sessions_total_campaign)
        sessions_total_change2 = calculate_percentage_change(sessions_total_base2, sessions_total_campaign)
        sessions_google_change1 = calculate_percentage_change(sessions_google_base1, sessions_google_campaign)
        sessions_google_change2 = calculate_percentage_change(sessions_google_base2, sessions_google_campaign)
        net_sales_change1 = calculate_percentage_change(net_sales_base1, net_sales_campaign)
        net_sales_change2 = calculate_percentage_change(net_sales_base2, net_sales_campaign)
        
        # Add target region results
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
    
    # Process control regions as aggregated "Control set"
    if control_regions:
        # Aggregate control data
        control_sessions_total_base1 = 0
        control_sessions_total_base2 = 0
        control_sessions_total_campaign = 0
        control_sessions_google_base1 = 0
        control_sessions_google_base2 = 0
        control_sessions_google_campaign = 0
        control_net_sales_base1 = 0
        control_net_sales_base2 = 0
        control_net_sales_campaign = 0
        
        for control_region in control_regions:
            ga_control = ga_data[ga_data[region_column] == control_region]
            shopify_control_data = shopify_data[shopify_data[shopify_region_column] == control_region]
            
            # Base Week 1 control data
            ga_base1_control = filter_data_by_period(ga_control, 'Date', base_week1_start, base_week1_end)
            shopify_base1_control = filter_data_by_period(shopify_control_data, 'Day', base_week1_start, base_week1_end)
            
            # Base Week 2 control data
            ga_base2_control = filter_data_by_period(ga_control, 'Date', base_week2_start, base_week2_end)
            shopify_base2_control = filter_data_by_period(shopify_control_data, 'Day', base_week2_start, base_week2_end)
            
            # Campaign control data
            ga_campaign_control = filter_data_by_period(ga_control, 'Date', campaign_start, campaign_end)
            shopify_campaign_control = filter_data_by_period(shopify_control_data, 'Day', campaign_start, campaign_end)
            
            # Aggregate totals
            control_sessions_total_base1 += ga_base1_control['Sessions'].sum()
            control_sessions_total_base2 += ga_base2_control['Sessions'].sum()
            control_sessions_total_campaign += ga_campaign_control['Sessions'].sum()
            
            # Google sessions
            ga_google_base1_control = ga_base1_control[ga_base1_control['Session source'].isin(google_sources)]
            ga_google_base2_control = ga_base2_control[ga_base2_control['Session source'].isin(google_sources)]
            ga_google_campaign_control = ga_campaign_control[ga_campaign_control['Session source'].isin(google_sources)]
            
            control_sessions_google_base1 += ga_google_base1_control['Sessions'].sum()
            control_sessions_google_base2 += ga_google_base2_control['Sessions'].sum()
            control_sessions_google_campaign += ga_google_campaign_control['Sessions'].sum()
            
            # Net Sales
            control_net_sales_base1 += shopify_base1_control['Net sales'].sum()
            control_net_sales_base2 += shopify_base2_control['Net sales'].sum()
            control_net_sales_campaign += shopify_campaign_control['Net sales'].sum()
        
        # Apply control region and week divisors
        control_region_count = len(control_regions)
        
        # Final control calculations (divide by control regions AND weeks)
        control_sessions_total_base1_final = control_sessions_total_base1 / (control_region_count * base1_divisor)
        control_sessions_total_base2_final = control_sessions_total_base2 / (control_region_count * base2_divisor)
        control_sessions_total_campaign_final = control_sessions_total_campaign / (control_region_count * campaign_weeks)
        
        control_sessions_google_base1_final = control_sessions_google_base1 / (control_region_count * base1_divisor)
        control_sessions_google_base2_final = control_sessions_google_base2 / (control_region_count * base2_divisor)
        control_sessions_google_campaign_final = control_sessions_google_campaign / (control_region_count * campaign_weeks)
        
        control_net_sales_base1_final = control_net_sales_base1 / (control_region_count * base1_divisor)
        control_net_sales_base2_final = control_net_sales_base2 / (control_region_count * base2_divisor)
        control_net_sales_campaign_final = control_net_sales_campaign / (control_region_count * campaign_weeks)
        
        # Calculate percentage changes for control set
        control_sessions_total_change1 = calculate_percentage_change(control_sessions_total_base1_final, control_sessions_total_campaign_final)
        control_sessions_total_change2 = calculate_percentage_change(control_sessions_total_base2_final, control_sessions_total_campaign_final)
        control_sessions_google_change1 = calculate_percentage_change(control_sessions_google_base1_final, control_sessions_google_campaign_final)
        control_sessions_google_change2 = calculate_percentage_change(control_sessions_google_base2_final, control_sessions_google_campaign_final)
        control_net_sales_change1 = calculate_percentage_change(control_net_sales_base1_final, control_net_sales_campaign_final)
        control_net_sales_change2 = calculate_percentage_change(control_net_sales_base2_final, control_net_sales_campaign_final)
        
        # Add aggregated control set results
        results.append({
            'Region': 'Control set',
            'Sessions_Total_Base1': control_sessions_total_base1_final,
            'Sessions_Total_Base2': control_sessions_total_base2_final,
            'Sessions_Total_Campaign': control_sessions_total_campaign_final,
            'Sessions_Total_Change1': control_sessions_total_change1,
            'Sessions_Total_Change2': control_sessions_total_change2,
            'Sessions_Google_Base1': control_sessions_google_base1_final,
            'Sessions_Google_Base2': control_sessions_google_base2_final,
            'Sessions_Google_Campaign': control_sessions_google_campaign_final,
            'Sessions_Google_Change1': control_sessions_google_change1,
            'Sessions_Google_Change2': control_sessions_google_change2,
            'Net_Sales_Base1': control_net_sales_base1_final,
            'Net_Sales_Base2': control_net_sales_base2_final,
            'Net_Sales_Campaign': control_net_sales_campaign_final,
            'Net_Sales_Change1': control_net_sales_change1,
            'Net_Sales_Change2': control_net_sales_change2
        })
    
    return pd.DataFrame(results)
    # Process control regions as aggregated "Control set"
    if control_regions:
        # Aggregate control data
        control_sessions_total_base1 = 0
        control_sessions_total_base2 = 0
        control_sessions_total_campaign = 0
        control_sessions_google_base1 = 0
        control_sessions_google_base2 = 0
        control_sessions_google_campaign = 0
        control_net_sales_base1 = 0
        control_net_sales_base2 = 0
        control_net_sales_campaign = 0
        
        for control_region in control_regions:
            ga_control = ga_data[ga_data[region_column] == control_region]
            shopify_control_data = shopify_data[shopify_data[shopify_region_column] == control_region]
            
            # Base Week 1 control data
            ga_base1_control = filter_data_by_period(ga_control, 'Date', base_week1_start, base_week1_end)
            shopify_base1_control = filter_data_by_period(shopify_control_data, 'Day', base_week1_start, base_week1_end)
            
            # Base Week 2 control data
            ga_base2_control = filter_data_by_period(ga_control, 'Date', base_week2_start, base_week2_end)
            shopify_base2_control = filter_data_by_period(shopify_control_data, 'Day', base_week2_start, base_week2_end)
            
            # Campaign control data
            ga_campaign_control = filter_data_by_period(ga_control, 'Date', campaign_start, campaign_end)
            shopify_campaign_control = filter_data_by_period(shopify_control_data, 'Day', campaign_start, campaign_end)
            
            # Aggregate totals
            control_sessions_total_base1 += ga_base1_control['Sessions'].sum()
            control_sessions_total_base2 += ga_base2_control['Sessions'].sum()
            control_sessions_total_campaign += ga_campaign_control['Sessions'].sum()
            
            # Google sessions
            ga_google_base1_control = ga_base1_control[ga_base1_control['Session source'].isin(google_sources)]
            ga_google_base2_control = ga_base2_control[ga_base2_control['Session source'].isin(google_sources)]
            ga_google_campaign_control = ga_campaign_control[ga_campaign_control['Session source'].isin(google_sources)]
            
            control_sessions_google_base1 += ga_google_base1_control['Sessions'].sum()
            control_sessions_google_base2 += ga_google_base2_control['Sessions'].sum()
            control_sessions_google_campaign += ga_google_campaign_control['Sessions'].sum()
            
            # Net Sales
            control_net_sales_base1 += shopify_base1_control['Net sales'].sum()
            control_net_sales_base2 += shopify_base2_control['Net sales'].sum()
            control_net_sales_campaign += shopify_campaign_control['Net sales'].sum()
        
        # Apply control region and week divisors
        control_region_count = len(control_regions)
        
        # Final control calculations (divide by control regions AND weeks)
        control_sessions_total_base1_final = control_sessions_total_base1 / (control_region_count * base1_divisor)
        control_sessions_total_base2_final = control_sessions_total_base2 / (control_region_count * base2_divisor)
        control_sessions_total_campaign_final = control_sessions_total_campaign / (control_region_count * campaign_weeks)
        
        control_sessions_google_base1_final = control_sessions_google_base1 / (control_region_count * base1_divisor)
        control_sessions_google_base2_final = control_sessions_google_base2 / (control_region_count * base2_divisor)
        control_sessions_google_campaign_final = control_sessions_google_campaign / (control_region_count * campaign_weeks)
        
        control_net_sales_base1_final = control_net_sales_base1 / (control_region_count * base1_divisor)
        control_net_sales_base2_final = control_net_sales_base2 / (control_region_count * base2_divisor)
        control_net_sales_campaign_final = control_net_sales_campaign / (control_region_count * campaign_weeks)
        
        # Calculate percentage changes for control set
        control_sessions_total_change1 = calculate_percentage_change(control_sessions_total_base1_final, control_sessions_total_campaign_final)
        control_sessions_total_change2 = calculate_percentage_change(control_sessions_total_base2_final, control_sessions_total_campaign_final)
        control_sessions_google_change1 = calculate_percentage_change(control_sessions_google_base1_final, control_sessions_google_campaign_final)
        control_sessions_google_change2 = calculate_percentage_change(control_sessions_google_base2_final, control_sessions_google_campaign_final)
        control_net_sales_change1 = calculate_percentage_change(control_net_sales_base1_final, control_net_sales_campaign_final)
        control_net_sales_change2 = calculate_percentage_change(control_net_sales_base2_final, control_net_sales_campaign_final)
        
        # Add aggregated control set results
        results.append({
            'Region': 'Control set',
            'Sessions_Total_Base1': control_sessions_total_base1_final,
            'Sessions_Total_Base2': control_sessions_total_base2_final,
            'Sessions_Total_Campaign': control_sessions_total_campaign_final,
            'Sessions_Total_Change1': control_sessions_total_change1,
            'Sessions_Total_Change2': control_sessions_total_change2,
            'Sessions_Google_Base1': control_sessions_google_base1_final,
            'Sessions_Google_Base2': control_sessions_google_base2_final,
            'Sessions_Google_Campaign': control_sessions_google_campaign_final,
            'Sessions_Google_Change1': control_sessions_google_change1,
            'Sessions_Google_Change2': control_sessions_google_change2,
            'Net_Sales_Base1': control_net_sales_base1_final,
            'Net_Sales_Base2': control_net_sales_base2_final,
            'Net_Sales_Campaign': control_net_sales_campaign_final,
            'Net_Sales_Change1': control_net_sales_change1,
            'Net_Sales_Change2': control_net_sales_change2
        })
    
    return pd.DataFrame(results)
def format_analysis_table_html(df, base1_label, base2_label, campaign_label):
    """Format the analysis table as HTML with same format as original - two separate comparison tables"""
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            .analysis-table {{
                border-collapse: collapse;
                width: 100%;
                margin: 20px 0;
                font-size: 12px;
                font-family: Arial, sans-serif;
            }}
            .analysis-table th, .analysis-table td {{
                border: 1px solid #333;
                padding: 8px;
                text-align: center;
                vertical-align: middle;
            }}
            .analysis-table th {{
                background-color: #f2f2f2;
                font-weight: bold;
            }}
            .sessions-total {{ background-color: #e6f3ff; }}
            .sessions-google {{ background-color: #fff2cc; }}
            .net-sales {{ background-color: #e8f5e8; }}
            .region-row {{ background-color: #f0f0f0; font-weight: bold; }}
            .control-row {{ background-color: #f8f8f8; }}
        </style>
    </head>
    <body>
    
    <!-- First comparison table: Base Week 1 vs Campaign -->
    <table class="analysis-table">
        <thead>
            <tr>
                <th rowspan="2">Region</th>
                <th colspan="3" class="sessions-total">Sessions (Total)</th>
                <th colspan="3" class="sessions-google">Sessions (Google)</th>
                <th colspan="3" class="net-sales">Net Sales (Total)</th>
            </tr>
            <tr>
                <th class="sessions-total">{base1_label}</th>
                <th class="sessions-total">{campaign_label}</th>
                <th class="sessions-total">%change</th>
                <th class="sessions-google">{base1_label}</th>
                <th class="sessions-google">{campaign_label}</th>
                <th class="sessions-google">%change</th>
                <th class="net-sales">{base1_label}</th>
                <th class="net-sales">{campaign_label}</th>
                <th class="net-sales">%change</th>
            </tr>
        </thead>
        <tbody>
    """
    
    # Add rows for Base Week 1 vs Campaign comparison
    for _, row in df.iterrows():
        row_class = "region-row" if row['Region'] not in ['Control set'] else "control-row"
        html += f"""
            <tr class="{row_class}">
                <td>{row['Region']}</td>
                <td class="sessions-total">{row['Sessions_Total_Base1']:,.0f}</td>
                <td class="sessions-total">{row['Sessions_Total_Campaign']:,.0f}</td>
                <td class="sessions-total">{row['Sessions_Total_Change1']}</td>
                <td class="sessions-google">{row['Sessions_Google_Base1']:,.0f}</td>
                <td class="sessions-google">{row['Sessions_Google_Campaign']:,.0f}</td>
                <td class="sessions-google">{row['Sessions_Google_Change1']}</td>
                <td class="net-sales">${row['Net_Sales_Base1']:,.0f}</td>
                <td class="net-sales">${row['Net_Sales_Campaign']:,.0f}</td>
                <td class="net-sales">{row['Net_Sales_Change1']}</td>
            </tr>
        """
    
    # Add separator and second comparison table: Base Week 2 vs Campaign
    html += f"""
        </tbody>
    </table>
    
    <br><br>
    
    <!-- Second comparison table: Base Week 2 vs Campaign -->
    <table class="analysis-table">
        <thead>
            <tr>
                <th rowspan="2">Region</th>
                <th colspan="3" class="sessions-total">Sessions (Total)</th>
                <th colspan="3" class="sessions-google">Sessions (Google)</th>
                <th colspan="3" class="net-sales">Net Sales (Total)</th>
            </tr>
            <tr>
                <th class="sessions-total">{base2_label}</th>
                <th class="sessions-total">{campaign_label}</th>
                <th class="sessions-total">%change</th>
                <th class="sessions-google">{base2_label}</th>
                <th class="sessions-google">{campaign_label}</th>
                <th class="sessions-google">%change</th>
                <th class="net-sales">{base2_label}</th>
                <th class="net-sales">{campaign_label}</th>
                <th class="net-sales">%change</th>
            </tr>
        </thead>
        <tbody>
    """
    
    # Add rows for Base Week 2 vs Campaign comparison
    for _, row in df.iterrows():
        row_class = "region-row" if row['Region'] not in ['Control set'] else "control-row"
        html += f"""
            <tr class="{row_class}">
                <td>{row['Region']}</td>
                <td class="sessions-total">{row['Sessions_Total_Base2']:,.0f}</td>
                <td class="sessions-total">{row['Sessions_Total_Campaign']:,.0f}</td>
                <td class="sessions-total">{row['Sessions_Total_Change2']}</td>
                <td class="sessions-google">{row['Sessions_Google_Base2']:,.0f}</td>
                <td class="sessions-google">{row['Sessions_Google_Campaign']:,.0f}</td>
                <td class="sessions-google">{row['Sessions_Google_Change2']}</td>
                <td class="net-sales">${row['Net_Sales_Base2']:,.0f}</td>
                <td class="net-sales">${row['Net_Sales_Campaign']:,.0f}</td>
                <td class="net-sales">{row['Net_Sales_Change2']}</td>
            </tr>
        """
    
    html += """
        </tbody>
    </table>
    </body>
    </html>
    """
    
    return html
def main():
    # Header
    st.markdown('<h1 class="main-header">📊 Campaign Analysis: Merged GA Data</h1>', unsafe_allow_html=True)
    
    # Sidebar for file uploads
    st.sidebar.header("📁 Data Upload")
    
    # GA data upload (single merged file)
    ga_file = st.sidebar.file_uploader(
        "Upload Merged GA Data (CSV/Excel)",
        type=['csv', 'xlsx', 'xls'],
        help="Upload your merged Google Analytics data file",
        key="ga_upload"
    )
    
    # Shopify data upload
    shopify_file = st.sidebar.file_uploader(
        "Upload Shopify Data (CSV/Excel)",
        type=['csv', 'xlsx', 'xls'],
        help="Upload your Shopify data file",
        key="shopify_upload"
    )
    
    if ga_file is None or shopify_file is None:
        st.info("👆 Please upload both merged GA data and Shopify data files to begin analysis")
        
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
        """)
        return
    
    # Load data
    with st.spinner("Loading data..."):
        ga_data, ga_error = load_data(ga_file)
        shopify_data, shopify_error = load_data(shopify_file)
        
        if ga_error:
            st.error(f"Error loading GA data: {ga_error}")
            return
        if shopify_error:
            st.error(f"Error loading Shopify data: {shopify_error}")
            return
    
    st.success(f"✅ Data loaded successfully!")
    st.write(f"GA Data: {len(ga_data)} rows, {len(ga_data.columns)} columns")
    st.write(f"Shopify Data: {len(shopify_data)} rows, {len(shopify_data.columns)} columns")
    
    # Preprocess data
    with st.spinner("Preprocessing data..."):
        ga_data = preprocess_ga_data(ga_data)
        shopify_data = preprocess_shopify_data(shopify_data)
    
    # Configuration sidebar
    st.sidebar.header("⚙️ Analysis Configuration")
    
    # Column selection for regions
    st.sidebar.subheader("📊 Column Configuration")
    
    # Let user select the region column from GA data
    ga_columns = list(ga_data.columns)
    region_column = st.sidebar.selectbox(
        "Select Region Column from GA Data",
        options=ga_columns,
        index=next((i for i, col in enumerate(ga_columns) if 'region' in col.lower()), 0),
        help="Select the column that contains region information"
    )
    
    # Let user select shopify region column
    shopify_columns = list(shopify_data.columns)
    shopify_region_column = st.sidebar.selectbox(
        "Select Region Column from Shopify Data",
        options=shopify_columns,
        index=next((i for i, col in enumerate(shopify_columns) if 'region' in col.lower()), 0),
        help="Select the column that contains region information in Shopify data"
    )
    
    # Session source configuration
    st.sidebar.subheader("🔍 Session Source Configuration")
    
    # Get available session sources
    all_sources = sorted(list(ga_data['Session source'].unique())) if 'Session source' in ga_data.columns else []
    
    google_sources = st.sidebar.multiselect(
        "Select Google Session Sources",
        options=all_sources,
        default=[source for source in all_sources if 'google' in source.lower()],
        help="Select which session sources should be counted as Google sessions"
    )
    
    # Base week calculation method
    st.sidebar.subheader("📊 Base Week Calculation")
    base_week_method = st.sidebar.radio(
        "Base Week Values Calculation",
        options=["Average (÷weeks)", "Sum (Total)"],
        index=0,
        help="Choose whether base week values should be averaged by number of weeks or show the total sum"
    )
    # Period configuration
    st.sidebar.subheader("📅 Period Configuration")
    
    # Get date range from GA data
    if not ga_data.empty:
        min_date = ga_data['Date'].min().date()
        max_date = ga_data['Date'].max().date()
        
        st.sidebar.write(f"**Available Date Range:** {min_date} to {max_date}")
        
        # Base Week 1
        st.sidebar.write("**Base Week 1:**")
        base_week1_start = st.sidebar.date_input(
            "Base Week 1 Start", 
            value=min_date, 
            min_value=min_date, 
            max_value=max_date,
            key="base1_start"
        )
        base_week1_end = st.sidebar.date_input(
            "Base Week 1 End", 
            value=min_date + timedelta(days=20), 
            min_value=min_date, 
            max_value=max_date,
            key="base1_end"
        )
        
        # Base Week 2
        st.sidebar.write("**Base Week 2:**")
        base_week2_start = st.sidebar.date_input(
            "Base Week 2 Start", 
            value=min_date + timedelta(days=365), 
            min_value=min_date, 
            max_value=max_date,
            key="base2_start"
        )
        base_week2_end = st.sidebar.date_input(
            "Base Week 2 End", 
            value=min_date + timedelta(days=385), 
            min_value=min_date, 
            max_value=max_date,
            key="base2_end"
        )
        
        # Campaign Period
        st.sidebar.write("**Campaign Period:**")
        campaign_start = st.sidebar.date_input(
            "Campaign Start", 
            value=min_date + timedelta(days=21), 
            min_value=min_date, 
            max_value=max_date,
            key="campaign_start"
        )
        campaign_end = st.sidebar.date_input(
            "Campaign End", 
            value=min_date + timedelta(days=27), 
            min_value=min_date, 
            max_value=max_date,
            key="campaign_end"
        )
        
        # Validation
        if base_week1_start > base_week1_end:
            st.sidebar.error("Base week 1 start date must be before end date")
            return
        if base_week2_start > base_week2_end:
            st.sidebar.error("Base week 2 start date must be before end date")
            return
        if campaign_start > campaign_end:
            st.sidebar.error("Campaign start date must be before end date")
            return
    else:
        st.error("No valid GA data found")
        return
    
    # Labels
    st.sidebar.subheader("🏷️ Period Labels")
    base1_label = st.sidebar.text_input("Base Week 1 Label", value="Base week 25")
    base2_label = st.sidebar.text_input("Base Week 2 Label", value="Base week 26")
    campaign_label = st.sidebar.text_input("Campaign Label", value="Campaign - Week 1")
    
    # Region selection
    st.sidebar.subheader("🌍 Region Configuration")
    
    # Get available regions from the selected column
    available_regions = sorted(list(ga_data[region_column].unique())) if region_column in ga_data.columns else []
    
    # Show available regions for debugging
    st.sidebar.write(f"**Available Regions from '{region_column}' ({len(available_regions)}):**")
    if len(available_regions) <= 10:
        st.sidebar.write(", ".join(available_regions))
    else:
        st.sidebar.write(f"{', '.join(available_regions[:10])}... and {len(available_regions)-10} more")
    
    selected_regions = st.sidebar.multiselect(
        "Select Target Regions",
        options=available_regions,
        default=available_regions[:3] if len(available_regions) >= 3 else available_regions,
        help="Select regions to include in the analysis"
    )
    
    control_regions = st.sidebar.multiselect(
        "Select Control Regions",
        options=available_regions,  # Same options as target regions
        help="Select which regions should be labeled as 'Control set'"
    )
    
    if not selected_regions:
        st.warning("Please select at least one region for analysis.")
        return
    # Generate analysis
    if st.sidebar.button("🚀 Generate Analysis", type="primary"):
        with st.spinner("Generating campaign analysis..."):
            try:
                # Create analysis table
                analysis_df = create_analysis_table(
                    ga_data, shopify_data, selected_regions,
                    base_week1_start, base_week1_end, base_week2_start, base_week2_end,
                    campaign_start, campaign_end, control_regions, google_sources, 
                    base_week_method, region_column, shopify_region_column
                )
                
                # Display results
                st.subheader("📊 Campaign Analysis Results")
                
                # Format and display HTML table
                html_table = format_analysis_table_html(analysis_df, base1_label, base2_label, campaign_label)
                
                # Use st.components.v1.html to properly render the table
                import streamlit.components.v1 as components
                components.html(f"""
                <div style="width: 100%; overflow-x: auto;">
                    {html_table}
                </div>
                """, height=600)
                
                # Summary section
                st.markdown('<div class="summary-section">', unsafe_allow_html=True)
                st.subheader("📋 Analysis Summary")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write("**Control Set Information:**")
                    if control_regions:
                        control_list = ", ".join(control_regions)
                        st.write(f"Control regions: {control_list} ({len(control_regions)} regions)")
                    else:
                        st.write("No control regions selected")
                    
                    # Calculate weeks for display
                    base1_weeks = calculate_weeks_in_period(base_week1_start, base_week1_end)
                    base2_weeks = calculate_weeks_in_period(base_week2_start, base_week2_end)
                    st.write(f"Base week 1: {base1_weeks:.1f} weeks (averaging applied)")
                    st.write(f"Base week 2: {base2_weeks:.1f} weeks (averaging applied)")
                
                with col2:
                    st.write("**Campaign Information:**")
                    campaign_weeks = calculate_weeks_in_period(campaign_start, campaign_end)
                    st.write(f"Campaign period: {campaign_weeks:.1f} weeks (averaging applied)")
                    st.write(f"Google sources: {', '.join(google_sources) if google_sources else 'None selected'}")
                    st.write(f"Base week calculation: {base_week_method}")
                    
                # Debug information
                st.write("**Debug Information:**")
                st.write(f"Using GA region column: '{region_column}'")
                st.write(f"Using Shopify region column: '{shopify_region_column}'")
                st.write(f"Target regions selected: {', '.join(selected_regions)}")
                st.write(f"Control regions selected: {', '.join(control_regions) if control_regions else 'None'} ({len(control_regions)} regions)")
                st.write(f"Total rows in results: {len(analysis_df)}")
                
                base1_weeks = calculate_weeks_in_period(base_week1_start, base_week1_end)
                base2_weeks = calculate_weeks_in_period(base_week2_start, base_week2_end)
                campaign_weeks = calculate_weeks_in_period(campaign_start, campaign_end)
                
                base1_divisor_text = f"{base1_weeks:.1f} weeks" if base_week_method == "Average (÷weeks)" else "1 (Sum)"
                base2_divisor_text = f"{base2_weeks:.1f} weeks" if base_week_method == "Average (÷weeks)" else "1 (Sum)"
                st.write(f"Base week 1 divisor: {base1_divisor_text}")
                st.write(f"Base week 2 divisor: {base2_divisor_text}")
                
                if control_regions:
                    control_base1_total = len(control_regions) * (base1_weeks if base_week_method == "Average (÷weeks)" else 1)
                    control_base2_total = len(control_regions) * (base2_weeks if base_week_method == "Average (÷weeks)" else 1)
                    control_campaign_total = len(control_regions) * campaign_weeks
                    st.write(f"Control set divisors:")
                    st.write(f"  - Base week 1: {len(control_regions)} regions × {base1_weeks:.1f} weeks = {control_base1_total:.1f}")
                    st.write(f"  - Base week 2: {len(control_regions)} regions × {base2_weeks:.1f} weeks = {control_base2_total:.1f}")
                    st.write(f"  - Campaign: {len(control_regions)} regions × {campaign_weeks:.1f} weeks = {control_campaign_total:.1f}")
                
                st.markdown('</div>', unsafe_allow_html=True)
                
                # Export functionality
                st.subheader("📥 Export Results")
                
                csv_data = analysis_df.to_csv(index=False)
                st.download_button(
                    label="📊 Download Analysis Results (CSV)",
                    data=csv_data,
                    file_name=f"campaign_analysis_merged_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
                
            except Exception as e:
                st.error(f"Error generating analysis: {str(e)}")
                st.write("Please check your data format and configuration.")
    
    # Data preview
    with st.expander("👀 Data Preview"):
        tab1, tab2 = st.tabs(["GA Data", "Shopify Data"])
        
        with tab1:
            if not ga_data.empty:
                st.write(f"**Date Range:** {ga_data['Date'].min()} to {ga_data['Date'].max()}")
                st.dataframe(ga_data.head(10), use_container_width=True)
        
        with tab2:
            if not shopify_data.empty:
                st.dataframe(shopify_data.head(10), use_container_width=True)

if __name__ == "__main__":
    main()