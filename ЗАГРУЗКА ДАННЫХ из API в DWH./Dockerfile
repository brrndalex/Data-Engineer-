FROM apache/airflow:2.5.3

USER airflow

ENV PIP_USER=false

RUN python -m venv dbt_venv && source dbt_venv/bin/activate &&  \
    pip install --no-cache-dir dbt-greenplum && deactivate

ENV PIP_USER=true

RUN pip install astronomer-cosmos boto3 pyarrow

USER airflow