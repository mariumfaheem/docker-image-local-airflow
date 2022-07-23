
import os
import sys
from datetime import datetime, timedelta

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '')
scripts_location = AIRFLOW_HOME + "/scripts/bap"
sys.path.insert(0, scripts_location)

import airflow
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.models import Variable
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_containers import EMRContainerOperator



from src.lib.data_lib.workflow_configs import configuration_overrides_args_monitoring,job_driver_arg
from src.lib.alert_lib.slack_alert import task_fail_slack_alert


default_args = {
    "owner": "bazaar-developer",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "provide_context": True,
    "retries": 1
}

with DAG("ops-cdc-pipeline", default_args=default_args, schedule_interval='*/15 * * * *', concurrency = 5, max_active_runs = 1, catchup = False) as dag:
    job_starter = EMRContainerOperator(
        task_id="start_job",
        virtual_cluster_id=Variable.get("VIRTUAL_CLUSTER_ID"),
        execution_role_arn=Variable.get("JOB_ROLE_ARN"),
        aws_conn_id="buraq-bap-prod-airflow-user",
        release_label="emr-6.4.0-latest",
        job_driver=job_driver_arg("bazaar-fulfilment","",""),
        configuration_overrides=configuration_overrides_args_monitoring(),
        name="ORDERS_HUDI_STREAMER",
        on_failure_callback=task_fail_slack_alert
    )






    fulfilment_cdc = EmrAddStepsOperator(
        task_id="fulfilment_cdc_job",
        job_flow_id=Variable.get("emr_ops_cluster"),
        aws_conn_id="buraq-bap-prod-airflow-user",
        steps=fulfilment_pipeline,
    )

    fulfilment_read_checker = EmrStepSensor(
        task_id='fullfilment_read_sensors',
        job_flow_id=Variable.get("emr_ops_cluster"),
        step_id="{{ task_instance.xcom_pull(task_ids='fulfilment_cdc_job', key='return_value')[0] }}",
        aws_conn_id="buraq-bap-prod-airflow-user",
    )

    catalog_cdc = EmrAddStepsOperator(
        task_id='catalog_cdc_job',
        job_flow_id=Variable.get("emr_ops_cluster"),
        aws_conn_id="buraq-bap-prod-airflow-user",
        steps=catalog_pipeline,
    )

    catalog_read_checker = EmrStepSensor(
        task_id='catalog_read_sensors',
        job_flow_id=Variable.get("emr_ops_cluster"),
        step_id="{{ task_instance.xcom_pull(task_ids = 'catalog_cdc_job', key = 'return_value')[0]}}",
        aws_conn_id="buraq-bap-prod-airflow-user",
    )
    rating_cdc = EmrAddStepsOperator(
        task_id='rating_cdc_job',
        job_flow_id=Variable.get("emr_ops_cluster"),
        aws_conn_id="buraq-bap-prod-airflow-user",
        steps=rating_pipeline,
    )

    rating_read_checker = EmrStepSensor(
        task_id='rating_read_sensors',
        job_flow_id=Variable.get("emr_ops_cluster"),
        step_id="{{ task_instance.xcom_pull(task_ids = 'rating_cdc_job', key = 'return_value')[0]}}",
        aws_conn_id="buraq-bap-prod-airflow-user",
    )

    rider_cdc = EmrAddStepsOperator(
        task_id="rider_cdc_job",
        job_flow_id=Variable.get("emr_ops_cluster"),
        aws_conn_id="buraq-bap-prod-airflow-user",
        steps=rider_pipeline,
    )

    rider_read_checker = EmrStepSensor(
        task_id='rider_read_sensors',
        job_flow_id=Variable.get("emr_ops_cluster"),
        step_id="{{ task_instance.xcom_pull(task_ids='rider_cdc_job', key='return_value')[0] }}",
        aws_conn_id="buraq-bap-prod-airflow-user",
    )

    bazaar_warehouse_configurations_cdc = EmrAddStepsOperator(
        task_id="bazaar_warehouse_configurations_cdc_job",
        job_flow_id=Variable.get("emr_ops_cluster"),
        aws_conn_id="buraq-bap-prod-airflow-user",
        steps=bazaar_warehouse_configurations_pipeline,
     )

    bazaar_warehouse_configurations_read_checker = EmrStepSensor(
        task_id='bazaar_warehouse_configurations_read_sensors',
        job_flow_id=Variable.get("emr_ops_cluster"),
        step_id="{{ task_instance.xcom_pull(task_ids='bazaar_warehouse_configurations_cdc_job', key='return_value')[0] }}",
        aws_conn_id="buraq-bap-prod-airflow-user",
    )

    trigger_ops_dp = TriggerDagRunOperator(
        task_id="cdc-ops-rider-metrics-dagrun",
        trigger_dag_id="ops-dp-15Min-SLA"
    )

    start = DummyOperator(
        task_id='start',
        dag=dag)


    end = DummyOperator(
        task_id='end',
        dag=dag
)

start >> [fulfilment_cdc, catalog_cdc] 
fulfilment_cdc >> fulfilment_read_checker >> end 
catalog_cdc >> catalog_read_checker >> rating_cdc
rating_cdc >> rating_read_checker >> end >> trigger_ops_dp
start >> rider_cdc >> rider_read_checker >> end
start >> bazaar_warehouse_configurations_cdc >> bazaar_warehouse_configurations_read_checker >> end
