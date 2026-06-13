from airflow import DAG
from airflow.utils import timezone
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


def _get_air_quality_data(**context):
    print(context)

    ds = context["ds"]

    import requests

    #url = "https://dog.ceo/api/breeds/image/random"
    url = f"https://air-quality-api.open-meteo.com/v1/air-quality?latitude=13.8732348&longitude=100.5513485&hourly=pm2_5&start_date={ds}&end_date={ds}#"
    response = requests.get(url)
    print(response)
    data = response.json()
    print(data)

    import json

    with open(f"/opt/airflow/dags/{ds}-output.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _find_average_pm25(**context):
    ds = context["ds"]
    
    import json

    with open(f"/opt/airflow/dags/{ds}-output.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    pm25_values = data["hourly"]["pm2_5"]
    average_pm25 = sum(pm25_values) / len(pm25_values)
    print(f"Average PM2.5 for {ds}: {average_pm25}")

    with open(f"/opt/airflow/dags/{ds}-average.json", "w", encoding="utf-8") as f:
        data = {
            "average_pm25": average_pm25
        }
        json.dump(data, f, ensure_ascii=False, indent=2)

with DAG(
    "weather_data_dag",
    start_date=timezone.datetime(2026, 6, 6),
    schedule="0 0  * * *" # หรือพิมพ์ "@daily"
):
    get_air_quality_data = PythonOperator(
        task_id="get_air_quality_data",
        python_callable=_get_air_quality_data,
    )

    find_average_pm25 = PythonOperator(
        task_id="find_average_pm25",
        python_callable=_find_average_pm25,
    )

    get_air_quality_data >> find_average_pm25

    echo_date = BashOperator(
        task_id="echo_date",
        bash_command="echo {{ ds }}",
    )