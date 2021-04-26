from airflow import DAG
from airflow.utils.dates import days_ago, timedelta
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import Connection

dag = DAG(
    dag_id='customer_360_pipeline',
    start_date=days_ago(1)
)

sensor = HttpSensor(
    task_id="watch_for_orders",
    http_conn_id='order_s3',
    endpoint='orders.csv',
    response_check=lambda response: response.status_code == 200,
    retry_delay=timedelta(minutes=5),
    retries=12,
    dag=dag
)

def get_order_url():
    session = session.Session()
    operator = session.query(Connection).filter(Connection.conn_id == 'order_s3')
    return f'{connection.schema}://{connection.host}/orders.csv'

download_order_cmd = f'rm -rf airflow_pipeline && mkdir -p airflow_pipeline && cd airflow_pipeline && wget {get_order_url()}'

download_to_edgenode = SSHOperator(
    task_id='download_orders',
    ssh_conn_id='itversity',
    command=download_order_cmd,
    dag=dag
)

import_customer_info = SSHOperator(
    task_id='download_customers',
    ssh_conn_id='itversity',
    command=fetch_customer_info_cmd(),
    dag=dag
)

import_customer_info = SSHOperator(
    task_id='upload_orders',
    ssh_conn_id='itversity',
    command=,
    dag=dag
)

def fetch_customer_info_cmd():
    command_one = "hive -e 'DROP TABLE airflow.customers'"
    command_one_ext = 'hdfs dfs -rm -R -f customers'
    command_two = "sqoop import --connect jdbc:mysql://nn01.itversity.com:3306/retail_db --username retail_dba --password itversity --table customers --hive-import --create-hive-table --hive-table airflow.customers"
    command_three = "exit 0"
    return f'{command_one} && {command_one_ext} && {command_two} && ({command_two} || {command_three})'

dummy = DummyOperator(
    task_id='dummy',
    dag=dag
)

sensor >> download_to_edgenode >> dummy