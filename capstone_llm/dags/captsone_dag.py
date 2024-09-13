from datetime import datetime, timedelta
from airflow import DAG
from conveyor.operators import ConveyorContainerOperatorV2
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

role = "capstone_conveyor_llm"

# def conveyor_spark_generator(task):
#     return ConveyorSparkSubmitOperatorV2(
#         task_id=task,
#         num_executors=1,
#         driver_instance_type="mx.small",
#         executor_instance_type="mx.small",
#         application=f"src/capstonellm/tasks/{task}.py",
#         aws_role=role,
#         env_vars = env,
#         application_args=[
#             "--environment", "{{ macros.conveyor.env() }}"]
#         )



def conveyor_container_generator(task):
    ConveyorContainerOperatorV2(
    task_id=task,
    aws_role=role,
    instance_type='mx.micro',
    cmds=[
        "python3", "-m", f"capstonellm.tasks.{task}"
    ],
    arguments = ["-e", "{{ macros.conveyor.env() }}"],
)

with DAG(
    "capstone_project",
    default_args=default_args,
    schedule_interval="5 * * * *",
    catchup=False,
) as dag:

    conveyor_container_generator("clean")
