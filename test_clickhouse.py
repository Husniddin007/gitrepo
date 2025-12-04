from clickhouse_driver import Client


try:
    client = Client(host='localhost',port=9000)
    result = client.execute('SELECT version()')
    print(f'ClickHouse ishlayapti')
    print(f'Version {result[0][0]}')

    client.execute('CREATE DATABASE IF NOT EXISTS test_db')
    print('test_db yartaildi')

    client.execute('''

        CREATE TABLE IF NOT EXISTS test_db.test_table(
                   id UInt32,
                   name String,
                   created_at DateTime
                   ) ENGINE = MergeTree()
                   ORDER BY id
    ''')
    print('test table yartildi')


    from datetime import datetime
    client.execute(
        'INSERT INTO test_db.test_table (id ,name, created_at) VALUES',
        [(1,'Test',datetime.now()),(2,"salom",datetime.now())]
    )
    print('malumotlar qosildi')

    result = client.execute("SELECT * FROM test_db.test_table")
    print(f"Malumotlar:{result}")

except Exception as e:
    print(f"Xatolik {e}")