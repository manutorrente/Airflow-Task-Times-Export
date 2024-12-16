from airflow_client.client.api import dag_run_api, task_instance_api
from airflow_connection import client

    

def get_dag_runs(api_instance: dag_run_api.DAGRunApi, dag_id: str) -> list[dict]:
    response = api_instance.get_dag_runs(dag_id=dag_id)
    return response['dag_runs']

def get_last_dag_run(api_instance: dag_run_api.DAGRunApi, dag_id: str) -> dict:
    dag_runs = get_dag_runs(api_instance, dag_id)
    return dag_runs[0]

def get_all_task_instances(dag_run_api_instance: dag_run_api.DAGRunApi, task_instance_api_instance: task_instance_api.TaskInstanceApi, dag_id: str) -> list[dict]:
    task_instances = []
    dag_runs = get_dag_runs(dag_run_api_instance, dag_id)
    for dag_run in dag_runs:
        task_instances.extend(get_task_instances(task_instance_api_instance, dag_id, dag_run['dag_run_id']))
    return task_instances

def get_task_instances(api_instance: task_instance_api.TaskInstanceApi, dag_id: str, dag_run_id: str) -> list[dict]:
    response = api_instance.get_task_instances(dag_id=dag_id, dag_run_id=dag_run_id)
    return response['task_instances']

def map_task_instances_to_csv(task_instances: list[dict], filename: str, fields: list[str]) -> None:
    with open(filename, 'w') as f:
        f.write(','.join(fields) + '\n')
        for task_instance in task_instances:
            f.write(','.join([str(task_instance[field]) for field in fields]) + '\n')

def dag_history_to_csv(dag_run_api_instance: dag_run_api.DAGRunApi, task_instance_api_instance: task_instance_api.TaskInstanceApi, dag_id: str, fields: list[str]) -> None:

    task_instances = get_all_task_instances(dag_run_api_instance, task_instance_api_instance, dag_id)

    print(f"Writing history to {dag_id}-history.csv")
    print(f"Task Instances: {len(task_instances)}")

    filename = f"output/{dag_id}-history.csv".replace(":", "-")
    map_task_instances_to_csv(task_instances, filename, fields)


def main():
    
    dag_run_api_instance = dag_run_api.DAGRunApi(client)
    task_instance_api_instance = task_instance_api.TaskInstanceApi(client)

    dag_id = "ODS-RDM"

    fields = ['dag_run_id','task_id', 'state', 'duration', 'start_date', 'end_date']

    dag_history_to_csv(dag_run_api_instance, task_instance_api_instance, dag_id, fields)


if __name__ == "__main__":
    main()




