import psycopg2
import uuid
import random
from datetime import datetime, timedelta

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

    cursor.execute("SELECT staff_id FROM staff WHERE staff_position IN ('Официант', 'Бармен');")
    staff_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT item_id, price, popularity_weight FROM menu;")
    menu_items = cursor.fetchall()
    weights = [item[2] for item in menu_items]

    today = datetime.now()
    total_receipts_created = 0

    for day_offset in range (30, 0, -1):

        current_date = today - timedelta(days=day_offset)
        daily_receipts_count = random.randint(30, 50)

        for _ in range(daily_receipts_count):

            new_receipt_id = str(uuid.uuid4())

            random_hour = random.randint(8, 23)
            random_minute = random.randint(0, 59)
            random_second = random.randint(0, 59)
            receipt_time = current_date.replace(hour=random_hour, minute=random_minute, second=random_second, microsecond=0)

            staff_id = random.choice(staff_ids)
            receipt_total_price = 0

            num_items_in_receipt = random.randint(1, 5)
            chosen_dishes = random.choices(menu_items, weights=weights, k=num_items_in_receipt)

            temp_items_basket = []

            for dish_id, dish_price, _ in chosen_dishes:

                quantity = random.randint(1, 3)
                total_item_price = dish_price * quantity
                receipt_total_price += total_item_price

                temp_items_basket.append((dish_id, quantity, dish_price, total_item_price))

            insert_receipt_query = """
                INSERT INTO receipts (receipt_id, receipt_time, staff_id, total_price)
                VALUES (%s, %s, %s, %s)
            """
            cursor.execute(insert_receipt_query, (new_receipt_id, receipt_time, staff_id, receipt_total_price))

            insert_item_query = """
                INSERT INTO receipt_items (receipt_id, item_id, quantity, price_per_item, total_price)
                VALUES (%s, %s, %s, %s, %s)
            """

            for item in temp_items_basket:
                cursor.execute(insert_item_query, (new_receipt_id, item[0], item[1], item[2], item[3]))

            total_receipts_created += 1

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Всего загружено чеков: {total_receipts_created}")

except Exception as e:
    print('Ошибка')
    print(e)

            


