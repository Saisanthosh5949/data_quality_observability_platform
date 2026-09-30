# Data Quality & Observability Platform

PySpark portfolio project that detects data-quality failures, quarantines bad records,
tracks historical metrics, and prepares operational metrics for monitoring.

## Architecture

Raw Orders -> PySpark Quality Engine -> Clean + Quarantine
                              |
                              v
                    Quality Metric History
                              |
                              v
                    Observability Metrics
                              |
                       Prometheus / Grafana

## Detects
NULL spikes, duplicate IDs, negative amounts, invalid quantities/statuses,
freshness violations, volume anomalies, schema problems, and quality-score degradation.

## Setup
Requires Python 3.11 and Java 17.

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Run
```powershell
python run_quality_pipeline.py --rows 250000
```

Large run:
```powershell
python run_quality_pipeline.py --rows 1000000
```

## Individual stages
```powershell
python -m src.generators.generate_orders --rows 250000
python -m src.quality.run_quality_checks
python -m src.observability.build_observability_metrics
python -m src.observability.export_prometheus
python -m src.reports.print_quality_report
```

## Monitoring scaffold
```powershell
docker compose up -d
```
Grafana: localhost:3000
Prometheus: localhost:9090

Demo Grafana credentials are admin/admin; change them outside a demo environment.

The generated `metrics/data_quality.prom` is Prometheus text format. A production deployment
would expose it through a textfile collector, Pushgateway, or application metrics endpoint.

## Interview story
The platform treats data quality as an observable production system. PySpark evaluates reusable
row and dataset rules, quarantines invalid records, records metric history, computes a transparent
quality score, and emits monitoring metrics. Airflow provides scheduled orchestration and the
monitoring assets show how freshness and quality SLAs can become operational alerts.
