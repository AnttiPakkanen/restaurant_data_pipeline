import psycopg2

DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'Almaty',
    'host': 'localhost',
    'port': '5432'
}

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    tables_data = [
        (1, 'VIP', 6),
        (2, 'VIP', 6),
        (3, 'Основной зал', 4),
        (4, 'Основной зал', 4),
        (5, 'Основной зал', 4),
        (6, 'Основной зал', 4),
        (7, 'Основной зал', 4),
        (8, 'Терраса', 2),
        (9, 'Терраса', 2),
        (10, 'Терраса', 2)
    ]

    cursor.execute("TRUNCATE list_of_tables CASCADE;")

    insert_query = """
        INSERT INTO list_of_tables (table_id, zone_name, capacity)
        VALUES (%s, %s, %s);
    """

    cursor.executemany(insert_query, tables_data)

    conn.commit()
    cursor.close()
    conn.close()
    print('Список столов загружен в базу')

except Exception as e:
    print('Ой! Ошибка!')
    print(e)    
