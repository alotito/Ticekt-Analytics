# test_connection.py
# Updated to read from config.ini and use the pytds driver.

import pytds
import configparser

print("--- Standalone Connection Test (using config.ini and pytds) ---")

try:
    # 1. Read configuration from the INI file
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    # Test the destination 'analytics_db' connection
    db_config = config['analytics_db']

    # Parse the server and port from the 'server' key
    server_and_port = db_config['server'].split(',')
    server = server_and_port[0]
    port = int(server_and_port[1]) if len(server_and_port) > 1 else 1433
    
    database = db_config['database']
    user = db_config['user']
    # Uses the plain-text password for the test
    password = db_config['password'] 

    print(f"Attempting to connect to server: '{server}' on port: {port}...")

    # 2. Try to connect using pytds
    cnxn = pytds.connect(
        server=server,
        database=database,
        user=user,
        password=password,
        port=port
    )
    
    print("\n✅✅✅ SUCCESS! Connection was established successfully. ✅✅✅")
    
    # 3. Prove it works by getting the server version
    cursor = cnxn.cursor()
    cursor.execute("SELECT SERVERPROPERTY('productversion'), SERVERPROPERTY ('productlevel'), SERVERPROPERTY ('edition')")
    version_info = cursor.fetchone()
    print("\nSQL Server Info:")
    print(f" Version: {version_info[0]}")
    print(f" Level:   {version_info[1]}")
    print(f" Edition: {version_info[2]}")
    
    cnxn.close()

except Exception as ex:
    print(f"\n❌❌❌ FAILED to connect. ❌❌❌")
    print(f"Error details: {ex}")