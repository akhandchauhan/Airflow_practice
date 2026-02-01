from airflow import DAG
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime

# DAG definition
dag = DAG(
    dag_id="Pipeline_Layout_Example",
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/2 * * * *",
    catchup=False,
)

# Start & End
start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(
    task_id="end",
    dag=dag,
    trigger_rule="none_failed"
)

# Tasks
product_page = BashOperator(
    task_id="product_page",
    bash_command="echo 'This is the product page task.'",
    dag=dag,
)

checkout_page = BashOperator(
    task_id="checkout_page",
    bash_command="echo 'This is the checkout page task.'",
    dag=dag,
)

user_login_page = BashOperator(
    task_id="user_login_page",
    bash_command="echo 'This is the user login page task.'",
    dag=dag,
)

# Branch logic (DEFINED BEFORE USE)
def decide_flow(**context):
    checkout_amount = 50
    if checkout_amount < 100:
        return "checkout_page"
    else:
        return "merged"

read_raw = BranchPythonOperator(
    task_id="read_raw",
    python_callable=decide_flow,
    dag=dag,
)

merged = BashOperator(
    task_id="merged",
    bash_command="echo 'This is the merged task.'",
    dag=dag,
)

alarming_situation = BashOperator(
    task_id="alarming_situation",
    bash_command="echo 'This is the alarming situation task.'",
    dag=dag,
)

notify = BashOperator(
    task_id="notify",
    bash_command="echo 'This is the notify task.'",
    dag=dag,
)

# Dependencies (unchanged logic)
start >> [product_page, user_login_page, checkout_page]
[product_page, user_login_page, checkout_page] >> read_raw
read_raw >> [checkout_page, merged]
checkout_page >> alarming_situation
merged >> notify
[alarming_situation, notify] >> end
