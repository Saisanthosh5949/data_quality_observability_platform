from pathlib import Path
from src.common.spark_session import get_spark_session

def safe(name):
    return "data_quality_"+name.lower().replace("-","_").replace(" ","_")

def main():
    spark=get_spark_session("PrometheusExport")
    rows=spark.read.parquet("data/observability/current_metrics").collect()
    Path("metrics").mkdir(exist_ok=True)
    with open("metrics/data_quality.prom","w",encoding="utf-8") as f:
        for r in rows:
            name=safe(r["metric_name"])
            f.write(f"# TYPE {name} gauge\n{name} {r['metric_value']}\n")
            f.write(f"# TYPE {name}_passed gauge\n{name}_passed {1 if r['passed'] else 0}\n")
    print("Wrote metrics/data_quality.prom")
    spark.stop()

if __name__=="__main__": main()
