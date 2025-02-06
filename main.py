from airflow_client.client.api import dag_run_api, task_instance_api
from airflow_connection import client
from datetime import datetime, timedelta

    

def get_dag_runs(api_instance, dag_id):
    limit = 100
    offset = 0
    dag_runs = []

    while True:
        response = api_instance.get_dag_runs(dag_id=dag_id, limit=limit, offset=offset)
        dag_runs.extend(response['dag_runs'])
        if len(response['dag_runs']) < limit:
            break
        offset += limit
    return dag_runs

def get_last_dag_run(api_instance: dag_run_api.DAGRunApi, dag_id: str) -> dict:
    dag_runs = get_dag_runs(api_instance, dag_id)
    return dag_runs[0]

def get_all_task_instances_paginated(api_instance, dag_id, dag_run_id):
    limit = 100
    offset = 0
    task_instances = []

    while True:
        response = api_instance.get_task_instances(dag_id=dag_id, dag_run_id=dag_run_id, limit=limit, offset=offset)
        task_instances.extend(response['task_instances'])
        if len(response['task_instances']) < limit:
            break
        offset += limit

    return task_instances

def get_all_task_instances(dag_run_api_instance, task_instance_api_instance, dag_id):
    task_instances = []
    dag_runs = get_dag_runs(dag_run_api_instance, dag_id)
    for dag_run in dag_runs:
        task_instances.extend(get_all_task_instances_paginated(task_instance_api_instance, dag_id, dag_run['dag_run_id']))
    return task_instances

def get_task_instances(api_instance: task_instance_api.TaskInstanceApi, dag_id: str, dag_run_id: str) -> list[dict]:
    response = api_instance.get_task_instances(dag_id=dag_id, dag_run_id=dag_run_id)
    return response['task_instances']

def map_task_instances_to_csv(task_instances: list[dict], filename: str, fields: list[str], date_fields: list[str]) -> None:
    with open(filename, 'w') as f:
        f.write(','.join(fields) + '\n')
        for task_instance in task_instances:
            row = []
            for field in fields:
                value = task_instance.get(field)
                if field in date_fields and isinstance(value, str):
                    try:
                        # Parse the string into a datetime object
                        dt = datetime.fromisoformat(value.rstrip('Z'))
                        # Convert to GMT-3
                        value = (dt - timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        pass  # Keep the value as-is if parsing fails
                row.append(str(value))
            f.write(','.join(row) + '\n')

def dag_history_to_csv(dag_run_api_instance: dag_run_api.DAGRunApi, task_instance_api_instance: task_instance_api.TaskInstanceApi, dag_id: str, fields: list[str]) -> None:

    task_instances = get_all_task_instances(dag_run_api_instance, task_instance_api_instance, dag_id)

    print(f"Writing history to {dag_id}-history.csv")
    print(f"Task Instances: {len(task_instances)}")

    filename = f"output/{dag_id}-history.csv".replace(":", "-")
    datefields = [field for field in fields if 'date' in field.lower()]
    map_task_instances_to_csv(task_instances, filename, fields, datefields)

def last_dag_run_to_csv(dag_run_api_instance: dag_run_api.DAGRunApi, task_instance_api_instance: task_instance_api.TaskInstanceApi, dag_id: str, fields: list[str]) -> None:
    last_dag_run = get_last_dag_run(dag_run_api_instance, dag_id)
    dag_run_id = last_dag_run['dag_run_id']
    task_instances = get_task_instances(task_instance_api_instance, dag_id, dag_run_id)

    print(f"Writing last dag run to {dag_id}-last-dag-run.csv")
    print(f"Task Instances: {len(task_instances)}")

    filename = f"output/{dag_id}-last-dag-run.csv".replace(":", "-")
    datefields = [field for field in fields if 'date' in field.lower()]
    map_task_instances_to_csv(task_instances, filename, fields, datefields)

def main():
    
    dag_run_api_instance = dag_run_api.DAGRunApi(client)
    task_instance_api_instance = task_instance_api.TaskInstanceApi(client)

    dag_id = "ODS-RDM"

    fields = ['dag_run_id','task_id', 'state', 'duration', 'start_date', 'end_date']

    dag_history_to_csv(dag_run_api_instance, task_instance_api_instance, dag_id, fields)


if __name__ == "__main__":
    main()




