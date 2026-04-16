import uuid
import random
from datetime import datetime, timedelta

names_list = ['Алина', 'Марина', 'Тима', 'Карла', 'Сара', 'Марат', 'Самина', 'Карина', 'Бахром']
loyalty_levels = ['Gold', 'Silver', 'Bronze']

clients_data = []
today = datetime.now()

for _ in range(5):

    client_id = str(uuid.uuid4())
    full_name = random.choice(names_list)

    random_phone_tail = random.randint(1000000, 9999999)
    phone = f"+7799{random_phone_tail}"

    random_days_ago = random.randint(1, 90)
    registration_date = today - timedelta(days=random_days_ago)

    loyalty = random.choice(loyalty_levels)
    

    client_tuple = (client_id, full_name, phone, registration_date.date(), loyalty)

    clients_data.append(client_tuple)

for client in clients_data:
    print(client)