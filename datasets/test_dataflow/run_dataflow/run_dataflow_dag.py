# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from airflow import DAG
from airflow.contrib.operators import dataflow_operator

default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "start_date": "2021-07-01",
}


with DAG(
    dag_id="test_dataflow.run_dataflow",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="@once",
    catchup=False,
    default_view="graph",
) as dag:

    # Task to run a Dataflow job
    sample_dataflow_task = dataflow_operator.DataFlowPythonOperator(
        task_id="sample_dataflow_task",
        py_file="{{ var.json.shared.airflow_dags_folder }}/test_dataflow/run_dataflow/custom/dataflow_script.py",
        dataflow_default_options={
            "project": "{{ var.json.shared.gcp_project }}",
            "temp_location": "gs://{{ var.json.shared.composer_bucket }}/data/test_dataflow/run_dataflow/dataflow/tmp/",
            "staging_location": "gs://{{ var.json.shared.composer_bucket }}/data/test_dataflow/run_dataflow/dataflow/staging/",
            "runner": "DataflowRunner",
            "machine_type": "n1-standard-2",
        },
        options={
            "satellite": "goes-16",
            "product_type": "ABI-L1b-RadC",
            "year": "2020",
            "target_bucket": "{{ var.json.geos_fp.destination_bucket }}",
            "target_prefix": "{{ var.json.geos_fp.destination_prefix }}",
        },
    )

    sample_dataflow_task
