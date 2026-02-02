import streamlit as st
import streamlit.components.v1
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import io
import duckdb
import tempfile
import os

# Page configuration
st.set_page_config(
    page_title="Campaign Analysis - DuckDB Optimized",
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

def format_analysis_table_html(df, base1_label, base2_label, campaign_label):
    """Format the analysis table as HTML with same format as original"""
    
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
    
    # Add separator and second comparison table
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

def display_report(report):
    """Display a stored analysis report with download option"""
    
    # Extract report data
    report_id = report['id']
    timestamp = report['timestamp']
    analysis_df = report['analysis_df']
    base1_label = report['base1_label']
    base2_label = report['base2_label']
    campaign_label = report['campaign_label']
    config = report['config']
    
    # Create expandable section for each report
    with st.expander(f"📊 Analysis Report #{report_id} - Generated at {timestamp.strftime('%Y-%m-%d %H:%M:%S')}", expanded=True):
        
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
        
        # Generate and display HTML table
        html_table = format_analysis_table_html(analysis_df, base1_label, base2_label, campaign_label)
        st.components.v1.html(html_table, height=600, scrolling=True)
        
        # Create CSV data for download (matching exact display format)
        csv_data = create_csv_export_data(analysis_df, base1_label, base2_label, campaign_label)
        
        # Download button for this specific report
        st.download_button(
            label=f"📥 Download Report #{report_id} as CSV",
            data=csv_data,
            file_name=f"campaign_analysis_report_{report_id}_{timestamp.strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            key=f"download_report_{report_id}"
        )
        
        # Show summary statistics
        st.markdown("### 📊 Summary Statistics")
        
        # Calculate summary stats
        target_regions = [r for r in analysis_df['Region'].tolist() if r != 'Control set']
        control_regions = [r for r in analysis_df['Region'].tolist() if r == 'Control set']
        
        summary_col1, summary_col2, summary_col3 = st.columns(3)
        
        with summary_col1:
            st.metric("Target Regions", len(target_regions))
            st.metric("Control Regions", len(control_regions))
        
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

def create_csv_export_data(df, base1_label, base2_label, campaign_label):
    """Create CSV data that matches the exact display format"""
    
    # Create the CSV content to match the HTML table format exactly
    csv_lines = []
    
    # First table: Base Week 1 vs Campaign
    csv_lines.append(f"Base Week 1 ({base1_label}) vs Campaign ({campaign_label}) Comparison")
    csv_lines.append("")
    
    # Headers for first table - matching HTML structure exactly
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
    csv_lines.append('","'.join([''] + headers1 + ['']))
    csv_lines[-1] = '"' + csv_lines[-1] + '"'
    
    # Data rows for first table - properly escape values with commas
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
    csv_lines.append('","'.join([''] + headers2 + ['']))
    csv_lines[-1] = '"' + csv_lines[-1] + '"'
    
    # Data rows for second table - properly escape values with commas
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

def main():
    # Header
    st.markdown('<h1 class="main-header">🚀 Campaign Analysis: DuckDB Optimized</h1>', unsafe_allow_html=True)
    
    # Initialize session state for storing multiple reports
    if 'analysis_reports' not in st.session_state:
        st.session_state.analysis_reports = []
    if 'report_counter' not in st.session_state:
        st.session_state.report_counter = 0
    
    # Initialize session state for storing multiple reports
    if 'analysis_reports' not in st.session_state:
        st.session_state.analysis_reports = []
    if 'report_counter' not in st.session_state:
        st.session_state.report_counter = 0
    
    # Sidebar for file uploads
    st.sidebar.header("📁 Data Upload")
    
    # GA data upload (single merged file)
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
        
        ### Performance Tips:
        - **Parquet files** are processed much faster than CSV/Excel
        - Use parquet format for large datasets (>100MB)
        - DuckDB optimization provides 10-100x speedup for analytical queries
        """)
        return
    
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
    
    st.success(f"✅ Data loaded and optimized!")
    st.write(f"GA Data: {len(ga_data):,} rows, {len(ga_data.columns)} columns")
    st.write(f"Shopify Data: {len(shopify_data):,} rows, {len(shopify_data.columns)} columns")
    
    # Configuration sidebar
    st.sidebar.header("⚙️ Analysis Configuration")
    
    # Column selection
    st.sidebar.subheader("📊 Column Configuration")
    
    ga_columns = list(ga_data.columns)
    region_column = st.sidebar.selectbox(
        "Select Region Column from GA Data",
        options=ga_columns,
        index=next((i for i, col in enumerate(ga_columns) if 'region' in col.lower()), 0),
        help="Select the column that contains region information"
    )
    
    shopify_columns = list(shopify_data.columns)
    shopify_region_column = st.sidebar.selectbox(
        "Select Region Column from Shopify Data",
        options=shopify_columns,
        index=next((i for i, col in enumerate(shopify_columns) if 'region' in col.lower()), 0),
        help="Select the column that contains region information in Shopify data"
    )
    
    # Session source configuration
    st.sidebar.subheader("🔍 Session Source Configuration")
    
    all_sources = sorted(list(ga_data['Session source'].unique())) if 'Session source' in ga_data.columns else []
    
    google_sources = st.sidebar.multiselect(
        "Select Google Session Sources",
        options=all_sources,
        default=[source for source in all_sources if 'google' in source.lower()],
        help="Select which session sources should be counted as Google sessions"
    )
    
    # Base week calculation method
    st.sidebar.subheader("📊 Calculation Method")
    base_week_method = st.sidebar.radio(
        "Campaign Period Calculation",
        options=["Average (÷weeks)", "Sum (Total)"],
        index=0,
        help="Base weeks are ALWAYS averaged by number of weeks. Choose how to handle campaign period."
    )
    
    st.sidebar.info("ℹ️ Base weeks are automatically averaged by their respective number of weeks (rounded to nearest whole number)")
    
    # Show week calculations
    if not ga_data.empty:
        st.sidebar.subheader("📊 Week Calculations")
        
        # Calculate and display weeks for each period
        base1_weeks = calculate_weeks_in_period(base_week1_start, base_week1_end) if 'base_week1_start' in locals() else 0
        base2_weeks = calculate_weeks_in_period(base_week2_start, base_week2_end) if 'base_week2_start' in locals() else 0
        campaign_weeks_calc = calculate_weeks_in_period(campaign_start, campaign_end) if 'campaign_start' in locals() else 0
        
        if base1_weeks > 0:
            base1_days = (pd.to_datetime(base_week1_end) - pd.to_datetime(base_week1_start)).days + 1
            st.sidebar.write(f"**Base Week 1:** {base1_days} days → {base1_weeks} weeks (averaged)")
        
        if base2_weeks > 0:
            base2_days = (pd.to_datetime(base_week2_end) - pd.to_datetime(base_week2_start)).days + 1
            st.sidebar.write(f"**Base Week 2:** {base2_days} days → {base2_weeks} weeks (averaged)")
        
        if campaign_weeks_calc > 0:
            campaign_days = (pd.to_datetime(campaign_end) - pd.to_datetime(campaign_start)).days + 1
            campaign_method = "averaged" if base_week_method == "Average (÷weeks)" else "total"
            st.sidebar.write(f"**Campaign:** {campaign_days} days → {campaign_weeks_calc} weeks ({campaign_method})")
    
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
    
    available_regions = sorted(list(ga_data[region_column].unique())) if region_column in ga_data.columns else []
    
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
        options=available_regions,
        help="Select which regions should be labeled as 'Control set'"
    )
    
    if not selected_regions:
        st.warning("Please select at least one region for analysis.")
        return
    # Generate analysis
    if st.sidebar.button("🚀 Generate Analysis", type="primary"):
        with st.spinner("Generating high-speed analysis with DuckDB..."):
            try:
                # Create analysis using DuckDB
                results, base1_divisor, base2_divisor, campaign_divisor, conn = create_analysis_with_duckdb(
                    ga_data, shopify_data, selected_regions,
                    base_week1_start, base_week1_end, base_week2_start, base_week2_end,
                    campaign_start, campaign_end, control_regions, google_sources, 
                    base_week_method, region_column, shopify_region_column
                )
                
                # Process control regions if any
                if control_regions:
                    control_result = process_control_regions_duckdb(
                        conn, control_regions, google_sources,
                        base_week1_start, base_week1_end, base_week2_start, base_week2_end,
                        campaign_start, campaign_end, region_column, shopify_region_column,
                        base1_divisor, base2_divisor, campaign_divisor
                    )
                    if control_result:
                        results.append(control_result)
                
                # Close DuckDB connection
                conn.close()
                
                # Convert to DataFrame
                analysis_df = pd.DataFrame(results)
                
                # Increment report counter and store the report
                st.session_state.report_counter += 1
                report_id = st.session_state.report_counter
                
                # Store report data in session state
                report_data = {
                    'id': report_id,
                    'timestamp': datetime.now(),
                    'analysis_df': analysis_df,
                    'base1_label': base1_label,
                    'base2_label': base2_label,
                    'campaign_label': campaign_label,
                    'config': {
                        'base_week1_start': base_week1_start,
                        'base_week1_end': base_week1_end,
                        'base_week2_start': base_week2_start,
                        'base_week2_end': base_week2_end,
                        'campaign_start': campaign_start,
                        'campaign_end': campaign_end,
                        'selected_regions': selected_regions,
                        'control_regions': control_regions,
                        'google_sources': google_sources,
                        'region_column': region_column,
                        'shopify_region_column': shopify_region_column,
                        'base_week_method': base_week_method
                    }
                }
                
                st.session_state.analysis_reports.append(report_data)
                
                st.success(f"✅ Analysis Report #{report_id} generated successfully!")
                
            except Exception as e:
                st.error(f"Error generating analysis: {str(e)}")
                st.write("Please check your data format and configuration.")
                # Show detailed error for debugging
                import traceback
                st.code(traceback.format_exc())
    
    # Add button to generate another analysis
    if len(st.session_state.analysis_reports) > 0:
        st.sidebar.markdown("---")
        if st.sidebar.button("📊 Generate Another Analysis", type="secondary"):
            st.sidebar.success("Configure new analysis parameters above and click 'Generate Analysis'")
    
    # Display all generated reports
    if len(st.session_state.analysis_reports) > 0:
        st.markdown("---")
        st.header("📋 Generated Analysis Reports")
        
        # Show reports in reverse order (newest first)
        for report in reversed(st.session_state.analysis_reports):
            display_report(report)
    
    else:
        st.info("No analysis reports generated yet. Configure your analysis parameters and click 'Generate Analysis' to create your first report.")
    
    # Data preview
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