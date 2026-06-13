from airflow import DAG
from airflow.utils import timezone
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


def _get_dog_image_url():
    print("Hello!")

    import requests

    # url = "https://dog.ceo/api/breeds/image/random"
    # url = "https://raw.githubusercontent.com/zkan/data-engineering-bootcamp/refs/heads/main/dataset/greenery/addresses.csv"
    # response = requests.get(url)
    # print(response)
    # data = response.json()
    # print(data)

    import csv
    from io import StringIO

    url = "https://raw.githubusercontent.com/zkan/data-engineering-bootcamp/refs/heads/main/dataset/greenery/addresses.csv"

    response = requests.get(url)
    response.raise_for_status()

    csv_file = StringIO(response.text)

    reader = csv.DictReader(csv_file)

    rows = list(reader)

    print(rows[:3])


with DAG(
    "weather_data_dag",
    start_date=timezone.datetime(2026, 6, 6),
    schedule="50 15 30 * *"
):
    get_dog_image_url = PythonOperator(
        task_id="get_dog_image_url",
        python_callable=_get_dog_image_url,
    )

    echo_date = BashOperator(
        task_id="echo_date",
        bash_command="echo {{ ds }}",
    )