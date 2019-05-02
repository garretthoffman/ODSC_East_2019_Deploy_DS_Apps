from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

# Define Global Var for image prefix. This is the URL of our image repository
IMG_PREFIX = "161833574765.dkr.ecr.us-east-1.amazonaws.com/"

# define K8sPodOpperator Taks Creator helper functions
def get_task_image(dag, task_name):
    dag_id = dag.dag_id
    image = IMG_PREFIX + dag_id + "-" + task_name + ":latest"
    return image

def create_k8s_pod_opperator_task(task_name, dag):
    task = KubernetesPodOperator(namespace='default',
                                image=get_task_image(dag, task_name),
                                image_pull_policy='Always',
                                task_id=task_name,
                                name=task_name,  
                                is_delete_operator_pod=True,
                                get_logs=True,
                                dag=dag)
    return task

# define default dag args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019,5,2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# define our dag
dag = DAG('graph-recs-pipeline', default_args=default_args, schedule_interval='0 8 * * *')

# define our tasks
engagement_etl = create_k8s_pod_opperator_task('engagement-etl', dag)
closest_connect = create_k8s_pod_opperator_task('closest-connect', dag)
closest_rooms = create_k8s_pod_opperator_task('closest-rooms', dag)
follow_rec = create_k8s_pod_opperator_task('follow-rec', dag)
room_rec = create_k8s_pod_opperator_task('room-rec', dag)

# set task dependencies in our dag
closest_connect.set_upstream(engagement_etl)
closest_rooms.set_upstream(engagement_etl)
follow_rec.set_upstream(closest_connect)
room_rec.set_upstream(closest_connect)
room_rec.set_upstream(closest_rooms)
