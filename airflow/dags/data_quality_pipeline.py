from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG("data_quality_observability",start_date=datetime(2026,1,1),schedule="@hourly",catchup=False) as dag:
    generate=BashOperator(task_id="generate",bash_command="python -m src.generators.generate_orders --rows 250000")
    quality=BashOperator(task_id="quality",bash_command="python -m src.quality.run_quality_checks")
    observe=BashOperator(task_id="observe",bash_command="python -m src.observability.build_observability_metrics")
    export=BashOperator(task_id="export",bash_command="python -m src.observability.export_prometheus")
    generate >> quality >> observe >> export
