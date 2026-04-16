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
    delete_old_data = f"ALTER TABLE for_analytics_dwh.daily_revenue DELETE WHERE report_date = '{target_date}' SETTINGS mutations_sync = 1"
    hello_clickhouse_query(delete_old_data)

    insert_new_data = f"""
        INSERT INTO for_analytics_dwh.daily_revenue
        SELECT 
	        toDate(receipt_time) AS report_date,
	        count() AS total_receipts,
	        sum(final_price) AS total_revenue,
	        sum(discount_amount) AS total_discount,
	        round(avg(final_price), 2) AS avg_receipt
        FROM raw_data.receipts
        WHERE toDate(receipt_time) = '{target_date}'
        GROUP BY report_date
    """
    hello_clickhouse_query(insert_new_data)
    print(f"Чеки за {target_date} успешно загружены в для анализа")

except Exception as e:
    print("Ой! Ошибка!")
    print(e)
    sys.exit(1)