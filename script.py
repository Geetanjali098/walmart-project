import sqlite3
import pandas as pd
from pathlib import Path

def process_spreadsheets(db_path, spreadsheet_paths):
    """Process all spreadsheets and populate the database"""
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Process spreadsheet 0 (independent data)
    print("Processing spreadsheet 0...")
    df0 = pd.read_excel(spreadsheet_paths[0])
    df0.to_sql('independent_data', conn, if_exists='replace', index=False)
    
    # Process spreadsheets 1 and 2 (dependent data)
    print("Processing spreadsheets 1 and 2...")
    df1 = pd.read_excel(spreadsheet_paths[1])
    df2 = pd.read_excel(spreadsheet_paths[2])
    
    # Merge shipment details with origin/destination info
    merged = pd.merge(df1, df2, on='shipping_identifier', how='left')
    
    # Group by shipping identifier to combine products
    shipments = []
    for shipping_id, group in merged.groupby('shipping_identifier'):
        origin = group['origin'].iloc[0]
        destination = group['destination'].iloc[0]
        date = group['date'].iloc[0]
        
        # Create shipment record
        shipment = {
            'shipping_identifier': shipping_id,
            'origin': origin,
            'destination': destination,
            'date': date,
            'products': []
        }
        
        # Add products with quantities
        for _, row in group.iterrows():
            shipment['products'].append({
                'product_name': row['product_name'],
                'quantity': row['quantity']
            })
        
        shipments.append(shipment)
    
    # Prepare database tables
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS shipments (
        shipping_id TEXT PRIMARY KEY,
        origin TEXT,
        destination TEXT,
        date TEXT
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS shipment_products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        shipping_id TEXT,
        product_name TEXT,
        quantity INTEGER,
        FOREIGN KEY (shipping_id) REFERENCES shipments(shipping_id)
    )
    ''')
    
    # Insert data into database
    for shipment in shipments:
        # Insert shipment record
        cursor.execute('''
        INSERT INTO shipments (shipping_id, origin, destination, date)
        VALUES (?, ?, ?, ?)
        ''', (shipment['shipping_identifier'], 
              shipment['origin'], 
              shipment['destination'], 
              shipment['date']))
        
        # Insert products for this shipment
        for product in shipment['products']:
            cursor.execute('''
            INSERT INTO shipment_products (shipping_id, product_name, quantity)
            VALUES (?, ?, ?)
            ''', (shipment['shipping_identifier'], 
                  product['product_name'], 
                  product['quantity']))
    
    # Commit changes and close connection
    conn.commit()
    conn.close()
    print("Database population complete!")

def main():
    # Path configuration
    repo_path = Path.cwd()  # Assuming script is run from repo root
    db_path = repo_path / 'shipping.db'  # Adjust if different
    spreadsheet_paths = [
        repo_path / 'spreadsheet0.xlsx',  # Independent data
        repo_path / 'spreadsheet1.xlsx',  # Products data
        repo_path / 'spreadsheet2.xlsx'   # Shipment locations
    ]
    
    # Verify files exist
    for path in [db_path] + spreadsheet_paths:
        if not path.exists():
            raise FileNotFoundError(f"Required file not found: {path}")
    
    # Process the data
    process_spreadsheets(db_path, spreadsheet_paths)

if __name__ == '__main__':
    main()