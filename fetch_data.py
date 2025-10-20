import sqlite3
import yfinance as yf
import pandas as pd
import os

# --- Configuration ---
# Define the path to the database
DATABASE_NAME = 'nse_stocks.db'
DATABASE_PATH = os.path.join(DATABASE_NAME)

# Define the start date for fetching historical data
DATA_START_DATE = '2010-01-01'

def get_stocks_from_db():
    """
    Fetches the list of stocks (id, ticker) from the database.
    """
    if not os.path.exists(DATABASE_PATH):
        print(f"Error: Database file not found at {DATABASE_PATH}")
        print("Please run '1_setup_database.py' first.")
        return []
        
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, ticker FROM stocks")
    stocks = cursor.fetchall()
    
    conn.close()
    return stocks

def fetch_and_insert_data():
    """
    Main function to fetch data for all stocks and insert into the database.
    """
    stocks_list = get_stocks_from_db()
    
    if not stocks_list:
        print("No stocks found in the database. Exiting.")
        return

    print(f"Found {len(stocks_list)} stocks to process. Starting data download...")
    
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    total_rows_inserted = 0
    
    try:
        for stock_id, ticker in stocks_list:
            print(f"\n--- Processing: {ticker} (ID: {stock_id}) ---")
            
            # 1. Fetch data from yfinance
            try:
                data = yf.download(ticker, start=DATA_START_DATE, progress=False)
                
                if data.empty:
                    print(f"No data found for {ticker}. Skipping.")
                    continue
                    
                print(f"Downloaded {len(data)} data points for {ticker}.")

                # 2. Clean and format the data
                data.reset_index(inplace=True)
                
                # Rename columns to match our database schema
                data.rename(columns={
                    'Date': 'date',
                    'Open': 'open',
                    'High': 'high',
                    'Low': 'low',
                    'Close': 'close',
                    'Volume': 'volume'
                }, inplace=True)
                
                # Filter to only the columns we need
                data = data[['date', 'open', 'high', 'low', 'close', 'volume']]
                
                # Drop any rows with missing data
                data.dropna(inplace=True)
                
                # Add the stock_id to each row for insertion
                data['stock_id'] = stock_id
                
                # Convert 'date' to a string in 'YYYY-MM-DD' format
                data['date'] = data['date'].dt.strftime('%Y-%m-%d')
                
                # Reorder columns for insertion
                data_tuples = [tuple(x) for x in data[['stock_id', 'date', 'open', 'high', 'low', 'close', 'volume']].values]

                # 3. Insert data into the database
                insert_query = '''
                INSERT OR IGNORE INTO stock_data 
                (stock_id, date, open, high, low, close, volume) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
                '''
                
                cursor.executemany(insert_query, data_tuples)
                conn.commit()
                
                print(f"Successfully inserted {cursor.rowcount} new rows for {ticker}.")
                total_rows_inserted += cursor.rowcount

            except Exception as e:
                print(f"An error occurred while processing {ticker}: {e}")
                
    except sqlite3.Error as e:
        print(f"A database error occurred: {e}")
    finally:
        if conn:
            conn.close()
            
    print("\n--- ✅ Data fetching complete! ---")
    print(f"Total new rows inserted across all stocks: {total_rows_inserted}")

# --- Main execution ---
if __name__ == "__main__":
    fetch_and_insert_data()