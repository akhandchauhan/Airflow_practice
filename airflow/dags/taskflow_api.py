from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from airflow.sdk import dag
from airflow.sdk import task

from airflow.providers.standard import get_provider_info
from airflow.providers.standard.decorators import python


# ---------------- DAG definition ----------------
# dag = DAG(
#     dag_id="task_flow_api_dag",
#     start_date=datetime(2026, 1, 1),
#     schedule="@daily",
#     catchup=False,
# ) 

    # ---------------- Extract task ----------------
    # def extract(**context):
    #     ti = context["ti"]
    #     ti.xcom_push(
    #         key="output_path",
    #         value="/path/to/data.csv"
    #     )
    #     print("Pushed XCom: output_path = /path/to/data.csv")

    # ---------------- Transform task ----------------
    # def transform(**context):
    #     ti = context["ti"]
    #     input_path = ti.xcom_pull(
    #         task_ids="extract",
    #         key="output_path"
    #     )
    #     print(f"Transforming data from {input_path}")

    # ---------------- Operators ----------------
    # extract_task = PythonOperator(
    #     task_id="extract",
    #     python_callable=extract,
    # )

    # transform_task = PythonOperator(
    #     task_id="transform",
    #     python_callable=transform,
    # )

    # ---------------- Dependency ----------------
    # extract_task >> transform_task

########################################################################################################

# Refactored_code

@dag(dag_id="task_flow_api_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False)

def task_flow_api_dag():
    
    @task(multiple_outputs= True)
    def extract():
        return {"output_path":"/path/to/data.csv", "status":"extracted", "location":"us"} # xcom push
    
    @task
    def transform(extracted_data):
        input_path = extracted_data["output_path"]
        print(f"Transforming data from {input_path}")
    
    def load():
        print("Loading data...")
    
    load = PythonOperator(
        task_id = "load",
        python_callable=load,
    )
    # transform(extract())
    data = extract()
    transform(data) >> load

task_flow_api_dag()