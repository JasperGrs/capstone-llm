from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
import os
default_args = {
    "owner": "airflow",
    "description": "Use of the DockerOperator",
    "depend_on_past": False,
    "start_date": datetime(2021, 5, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

env = {
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY") ,
    "AWS_ACCESS_KEY_ID" : os.getenv("AWS_ACCESS_KEY_ID"),
    "AWS_SESSION_TOKEN": os.getenv("AWS_SESSION_TOKEN")
}

def create_dockeroperator(task):
    return DockerOperator(
        task_id="docker run",
        image="llm-capstone-grp5",
        container_name="task___docker_install",
        api_version="auto",
        auto_remove=True,
        command=f"python src/capstonellm/tasks/{task}.py ",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )
with DAG(
    "Capstone project",
    default_args=default_args,
    schedule_interval="5 * * * *",
    catchup=False,
) as dag:

    # docker_ingest = create_dockeroperator("ingest")

    test_docker_ingest = DockerOperator(
        task_id="docker run",
        image="llm-capstone-grp5",
        container_name="task___docker_install",
        api_version="auto",
        auto_remove=True,
        command="/bin/sleep 30",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    # docker_clean = create_dockeroperator("clean")

    test_docker_ingest >> docker_clean