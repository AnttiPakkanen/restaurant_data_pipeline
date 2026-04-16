import sys
import requests
from datetime import datetime, timedelta

if len(sys.argv) > 1:
    airflow_date_str = sys.argv[1].strip()
    target_date = datetime.strptime(airflow_date_str, '%Y-%m-%d').date()
else:
    target_date = (datetime.now() - timedelta(days=1)).date()

CLICKHOUSE_URL = 'http://host.docker.internal:8123'
AUTH = ('Andrei', '****')

def hello_clickhouse_query(query):
    response = requests.post(CLICKHOUSE_URL, auth=AUTH, data=query.encode('utf-8'))
    if response.status_code != 200:
        raise Exception(f"Ошибка ClickHouse: {response.text}")
    return response.text

try:
    delete_old_data = f"ALTER TABLE raw_data.receipts DELETE WHERE toDate(receipt_time) = '{target_date}' SETTINGS mutations_sync = 1"
    hello_clickhouse_query(delete_old_data)

    insert_new_data = f"""
        INSERT INTO raw_data.receipts
        SELECT * FROM postgres_dwh.receipts
        WHERE toDate(receipt_time) = '{target_date}'
    """
    hello_clickhouse_query(insert_new_data)

    check_total_receipts = f"SELECT COUNT(*) FROM raw_data.receipts WHERE toDate(receipt_time) = '{target_date}'"
    total_receipts_result = hello_clickhouse_query(check_total_receipts)
    downloaded_receipts = int(total_receipts_result.strip())

    print(f"Чеки, в количестве {downloaded_receipts} за {target_date} успешно загружены в ClickHouse")

except Exception as e:
    print("Ой! Ошибка!")
    print(e)
    sys.exit(1)
