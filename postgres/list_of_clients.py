import psycopg2
import uuid
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker('ru_RU')
loyalty_levels = ['Gold', 'Silver', 'Bronze']
clients_data = []
today = datetime.now()

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

    cursor.execute("TRUNCATE clients CASCADE;")

    for _ in range(100):

        client_id = str(uuid.uuid4())
        full_name = fake.name()

        random_phone_tail = random.randint(1000000, 9999999)
        phone = f"+7799{random_phone_tail}"

        random_days_ago = random.randint(1, 90)
        registration_date = today - timedelta(days=random_days_ago)

        loyalty = random.choice(loyalty_levels)

        client_tuple = (client_id, full_name, phone, registration_date.date(), loyalty)
        clients_data.append(client_tuple)

    insert_client_query = """
        INSERT INTO clients (client_id, full_name, phone, registration_date, loyalty_level)
        VALUES (%s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_client_query, clients_data)

    conn.commit()
    cursor.close()
    conn.close()

    print("Список клиентов загружен в базу")

except Exception as e:
    print("Ой! Ошибка!")
    print(e)