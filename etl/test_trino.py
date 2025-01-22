import trino
import time


def verify_trino():
    print("Waiting for services to be ready...")
    time.sleep(3)
    
    try:
        # Try to connect
        conn = trino.dbapi.connect(
            host='localhost',
            port=9090,
            user='admin'
        )
        
        cursor = conn.cursor()
        
        # Check catalogs
        print("\nChecking available catalogs:")
        cursor.execute("SHOW CATALOGS")
        catalogs = cursor.fetchall()
        print("Available catalogs:", [catalog[0] for catalog in catalogs])

        available_catalogs = [catalog[0] for catalog in catalogs]
            
        if 'delta' in available_catalogs:
            print("✅ Delta catalog is available")
        else:
            print("❌ Delta catalog is missing")
        
        cursor.close()
        conn.close()
        print("\n✅ Trino connection test completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error during verification: {str(e)}")


if __name__ == "__main__":
    verify_trino()
