import sqlite3
import pandas as pd
import plotly.graph_objects as go
import os

# --- Configuration ---
DATABASE_PATH = os.path.join('nse_stocks.db')
CHARTS_DIR = 'charts'
TIME_PERIODS = {
    'monthly': 'M',
    'quarterly': 'Q',
    'annual': 'A'
}

def create_chart_directories():
    """
    Ensures that the output directories for charts exist.
    """
    if not os.path.exists(CHARTS_DIR):
        os.makedirs(CHARTS_DIR)
        
    for period in TIME_PERIODS.keys():
        period_path = os.path.join(CHARTS_DIR, period)
        if not os.path.exists(period_path):
            os.makedirs(period_path)
    print("Chart directories are ready.")

def fetch_data_from_db():
    """
    Fetches all stock data and joins with stock info to get tickers and names.
    Returns a pandas DataFrame.
    """
    if not os.path.exists(DATABASE_PATH):
        print(f"Error: Database not found at {DATABASE_PATH}")
        print("Please run '1_setup_database.py' and '2_fetch_data.py' first.")
        return None

    print("Connecting to database and fetching all data...")
    conn = sqlite3.connect(DATABASE_PATH)
    
    query = """
    SELECT 
        s.ticker, 
        s.company_name, 
        d.date, 
        d.open, 
        d.high, 
        d.low, 
        d.close, 
        d.volume
    FROM stock_data d
    JOIN stocks s ON s.id = d.stock_id
    ORDER BY s.ticker, d.date
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        print("No data found in the database.")
        return None
        
    # Convert date column to datetime objects for processing
    df['date'] = pd.to_datetime(df['date'])
    print(f"Successfully loaded {len(df)} rows of data for {df['ticker'].nunique()} stocks.")
    return df

def aggregate_ohlcv(df, rule):
    """
    Aggregates daily OHLCV data to a different time frame (rule).
    'rule' can be 'M' (Month), 'Q' (Quarter), 'A' (Annual).
    """
    # Set the date as the index, which is required for resampling
    df_indexed = df.set_index('date')
    
    # Define the aggregation logic
    agg_logic = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }
    
    # Resample the data
    resampled_df = df_indexed.resample(rule).apply(agg_logic)
    
    # Drop any rows that are all empty (e.g., weekends/holidays with no data)
    resampled_df.dropna(how='all', inplace=True)
    
    return resampled_df.reset_index()


def create_candlestick_chart(df, ticker, company_name, timeframe):
    """
    Creates an interactive Plotly candlestick chart and saves it as an HTML file.
    """
    fig = go.Figure(data=[go.Candlestick(x=df['date'],
                                           open=df['open'],
                                           high=df['high'],
                                           low=df['low'],
                                           close=df['close'])])
    
    # Customize the chart layout
    chart_title = f"{company_name} ({ticker}) - {timeframe.title()} Chart"
    fig.update_layout(
        title=chart_title,
        yaxis_title='Stock Price (INR)',
        xaxis_title='Date',
        xaxis_rangeslider_visible=False, # Hide the range slider for a cleaner look
        template='plotly_dark'
    )
    
    # Define the output file path
    filename = f"{ticker}.html"
    save_path = os.path.join(CHARTS_DIR, timeframe, filename)
    
    # Save the chart as an interactive HTML file
    fig.write_html(save_path)
    print(f"Saved chart: {save_path}")

def generate_all_charts():
    """
    Main function to run the entire chart generation process.
    """
    create_chart_directories()
    
    master_df = fetch_data_from_db()
    if master_df is None:
        return

    # Get a list of all unique stocks in our dataset
    stocks_to_chart = master_df['ticker'].unique()
    
    for ticker in stocks_to_chart:
        # Get the company name (it's the same for all rows of this ticker)
        company_name = master_df[master_df['ticker'] == ticker]['company_name'].iloc[0]
        print(f"\n--- Generating charts for: {company_name} ---")
        
        # Get just the data for this one stock
        stock_df = master_df[master_df['ticker'] == ticker].copy()
        
        # Loop through each time period (monthly, quarterly, annual)
        for timeframe, rule in TIME_PERIODS.items():
            
            # 1. Aggregate the data
            agg_df = aggregate_ohlcv(stock_df, rule)
            
            if agg_df.empty:
                print(f"No aggregated data for {ticker} ({timeframe}). Skipping.")
                continue
            
            # 2. Create and save the chart
            create_candlestick_chart(agg_df, ticker, company_name, timeframe)

# --- Main execution ---
if __name__ == "__main__":
    print("--- 📊 Starting Chart Generation ---")
    generate_all_charts()
    print("\n--- ✅ All charts have been generated successfully! ---")