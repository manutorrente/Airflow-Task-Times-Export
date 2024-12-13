from airflow_client.client.api import dag_run_api, task_instance_api
from airflow_connection import client

def get_last_dag_run(api_instance: dag_run_api.DAGRunApi, dag_id: str) -> dict:
    response = api_instance.get_dag_runs(dag_id=dag_id)
    return response['dag_runs'][-1]

def get_task_instances(api_instance: task_instance_api.TaskInstanceApi, dag_id: str, dag_run_id: str) -> list[dict]:
    response = api_instance.get_task_instances(dag_id=dag_id, dag_run_id=dag_run_id)
    return response['task_instances']

def map_task_instances_to_csv(task_instances: list[dict], filename: str, fields: list[str]) -> None:
    with open(filename, 'w') as f:
        f.write(','.join(fields) + '\n')
        for task_instance in task_instances:
            f.write(','.join([str(task_instance[field]) for field in fields]) + '\n')


def main():
    
    dag_run_api_instance = dag_run_api.DAGRunApi(client)
    task_instance_api_instance = task_instance_api.TaskInstanceApi(client)

    dag_id = "dag-delta1"


    last_dag_run = get_last_dag_run(dag_run_api_instance, dag_id)
    dag_run_id = last_dag_run['dag_run_id']
    task_instances = get_task_instances(task_instance_api_instance, dag_run_id=dag_run_id, dag_id=dag_id)

    filename = f"output/{dag_id}-{dag_run_id}.csv".replace(":", "-")
    fields = ['task_id', 'state', 'duration', 'start_date', 'end_date']
    map_task_instances_to_csv(task_instances, filename, fields)


if __name__ == "__main__":
    main()




