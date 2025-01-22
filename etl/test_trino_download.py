import trino
import pandas as pd
from pathlib import Path
import argparse


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-limit", type=int, default=10,
                       help="Number of rows to download")
    parser.add_argument("--output-dir", type=str, default="./downloaded_data",
                       help="Directory where CSV files will be saved")
    parser.add_argument("--catalog", type=str, default="delta",
                       help="Catalog to query (hive or delta)")
    parser.add_argument("--schema", type=str, default="default",
                       help="Schema name")
    return parser.parse_args()


def download_data(args):
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Connect to Trino
    conn = trino.dbapi.connect(
        host='localhost',
        port=9090,
        user='admin',
        catalog=args.catalog,
        schema=args.schema
    )
    
    cursor = conn.cursor()
    
    try:
        # Get list of tables
        print(f"\nGetting tables from {args.catalog}.{args.schema}")
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        
        if not tables:
            print(f"No tables found in {args.catalog}.{args.schema}")
            return
        
        print(f"Found {len(tables)} tables: {[table[0] for table in tables]}")
        
        # Download data from each table
        for table in tables:
            table_name = table[0]
            print(f"\nDownloading data from table: {table_name}")
            
            # Get column names
            cursor.execute(f"DESCRIBE {table_name}")
            columns = [col[0] for col in cursor.fetchall()]
            
            # Fetch data
            query = f"SELECT * FROM {table_name}"
            if args.data_limit:
                query += f" LIMIT {args.data_limit}"
            
            print(f"Executing query: {query}")
            cursor.execute(query)
            data = cursor.fetchall()
            
            # Convert to DataFrame and save
            df = pd.DataFrame(data, columns=columns)
            output_file = output_dir / f"{table_name}.csv"
            df.to_csv(output_file, index=False)
            print(f"Saved {len(df)} rows to {output_file}")
            
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    args = parse_args()
    print(f"Starting download from {args.catalog} catalog")
    download_data(args)
    print("\nDownload completed!")
