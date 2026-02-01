from airflow.sdk import DAG 
from airflow.providers.standard.operators.python import PythonOperator
from airflow.timetables.interval import CronDataIntervalTimetable
from datetime import datetime
from airflow.utils.timezone import utc
import requests
import json
import os
import pendulum

dag = DAG(
    dag_id='API_Incremental_Data_Load',
    start_date=datetime(2026, 1, 1),
    # schedule='0 * * * *'
    schedule=CronDataIntervalTimetable(cron="0 * * * *",
        timezone=pendulum.timezone("Asia/Kolkata")),
    catchup = True

)

def fetch_api_data(**context):
    # url = "http://fastapi-app:5000/getAll"
    
    url = context['templates_dict']['url']
    output_path = context['templates_dict']['output_path']
    
    start_date = context['ds']
    end_date = context['ds']
    
    payload = {
        "start_date": start_date,
        "end_date": end_date,
        "limit": 50  # Add limit parameter
    }
    
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Basic YWRtaW46bWFuaXNo"
    }

    response = requests.post(
        url,
        headers=headers,
        json=payload  
    )

    print(f"Status Code: {response.status_code}")

    data = response.json()
     
    # output_path = f"/opt/airflow/output_files/dag_result_2026-01-04.json"
    
    with open(output_path, "w") as f:
        json.dump(data, f, indent=4)
        
    print(f"Saved API output to: {output_path}")
    
    # Print summary
    if isinstance(data, dict):
        print(f"Login users: {len(data.get('login_users', []))}")
        print(f"Product page users: {len(data.get('product_page_users', []))}")
        print(f"Checkout users: {len(data.get('checkout_users', []))}")

pull_api_data = PythonOperator(
    dag=dag,
    task_id='pull_api_data',
    python_callable=fetch_api_data,
#             op_args=["http://fastapi-app:5000/getAll",
#                      f"/opt/airflow/output_files/dag_result_{{ds}}.json"]
# ) 
    templates_dict={
    "output_path": "/opt/airflow/output_files/dag_result_{{ ds }}.json",
    "url": "http://fastapi-app:5000/getAll",
}
)