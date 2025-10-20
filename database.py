import sqlite3

# Define the list of 20 NSE stocks we will track
# Format: (Yahoo Finance Ticker, Company Name)
STOCKS_TO_TRACK = [
    ('RELIANCE.NS', 'Reliance Industries Ltd.'),
    ('TCS.NS', 'Tata Consultancy Services Ltd.'),
    ('HDFCBANK.NS', 'HDFC Bank Ltd.'),
    ('ICICIBANK.NS', 'ICICI Bank Ltd.'),
    ('BHARTIARTL.NS', 'Bharti Airtel Ltd.'),
    ('SBIN.NS', 'State Bank of India'),
    ('INFY.NS', 'Infosys Ltd.'),
    ('HINDUNILVR.NS', 'Hindustan Unilever Ltd.'),
    ('LT.NS', 'Larsen & Toubro Ltd.'),
    ('BAJFINANCE.NS', 'Bajaj Finance Ltd.'),
    ('ITC.NS', 'ITC Ltd.'),
    ('HCLTECH.NS', 'HCL Technologies Ltd.'),
    ('MARUTI.NS', 'Maruti Suzuki India Ltd.'),
    ('SUNPHARMA.NS', 'Sun Pharmaceutical Industries Ltd.'),
    ('KOTAKBANK.NS', 'Kotak Mahindra Bank Ltd.'),
    ('M&M.NS', 'Mahindra & Mahindra Ltd.'),
    ('AXISBANK.NS', 'Axis Bank Ltd.'),
    ('ULTRACEMCO.NS', 'UltraTech Cement Ltd.'),
    ('BAJAJFINSV.NS', 'Bajaj Finserv Ltd.'),
    ('NTPC.NS', 'NTPC Ltd.')
]

DATABASE_NAME = 'nse_stocks.db'

def create_database():
    """
    Creates the initial database and the required tables: stocks and stock_data.
    """
    try:
        # Connect to the SQLite database (it will be created if it doesn't exist)
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        
        print(f"Successfully connected to {DATABASE_NAME}")

        # --- Create the 'stocks' table ---
        # This table will store the list of stocks we are tracking.
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS stocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL UNIQUE,
            company_name TEXT NOT NULL
        )
        ''')
        print("Table 'stocks' created or already exists.")

        # --- Create the 'stock_data' table ---
        # This table will store the daily price data for each stock.
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_id INTEGER NOT NULL,
            date DATE NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume INTEGER NOT NULL,
            FOREIGN KEY (stock_id) REFERENCES stocks (id),
            UNIQUE(stock_id, date) -- Prevents duplicate entries for the same stock on the same day
        )
        ''')
        print("Table 'stock_data' created or already exists.")

        # Commit the changes and close the connection
        conn.commit()
        
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print(f"Connection to {DATABASE_NAME} closed.")

def populate_stocks_table():
    """
    Inserts our list of 20 stocks into the 'stocks' table.
    Uses 'INSERT OR IGNORE' to avoid errors if a stock already exists.
    """
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()

        # Insert the list of stocks
        # 'INSERT OR IGNORE' will skip inserting a row if the 'ticker' (which is UNIQUE) already exists.
        cursor.executemany('''
        INSERT OR IGNORE INTO stocks (ticker, company_name) VALUES (?, ?)
        ''', STOCKS_TO_TRACK)
        
        conn.commit()
        
        print(f"Successfully populated 'stocks' table with {len(STOCKS_TO_TRACK)} stocks.")
        
    except sqlite3.Error as e:
        print(f"An error occurred while populating stocks: {e}")
    finally:
        if conn:
            conn.close()

# --- Main execution ---
if __name__ == "__main__":
    print("--- 🐍 Starting Database Setup ---")
    create_database()
    print("\n--- 🏦 Populating Stocks Table ---")
    populate_stocks_table()
    print("\n--- ✅ Database setup is complete. ---")