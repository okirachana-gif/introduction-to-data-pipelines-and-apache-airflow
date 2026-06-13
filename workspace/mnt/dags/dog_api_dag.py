from airflow import DAG
from airflow.utils import timezone
from airflow.providers.standard.operators.python import PythonOperator


def _get_dog_image_url():
    print("Hello!")

    import requests

    url = "https://dog.ceo/api/breeds/image/random"
    response = requests.get(url)
    data = response.json()
    print(data)


with DAG(
    "dog_api_dag.py",
    start_date = timezone.datetime(2026, 6, 6),
    schedule = "50 15 30 * *" #การตั้ง schedule ให้ไปดูที่ web crontab guru ถ้าไม่ schedule ให้ใส่ None โดยไม่ต้องมี ""
):
    get_dog_image_url = PythonOperator(
        task_id = "get_dog_image_url",
        python_callable = _get_dog_image_url,
    )
