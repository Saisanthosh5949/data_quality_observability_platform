from pyspark.sql import functions as F
from src.common.spark_session import get_spark_session

def main():
    spark=get_spark_session("ObservabilityMetrics")
    checks=spark.read.parquet("data/quality/check_results")
    current=(checks.select("metric_name","metric_value","unit","passed","measured_at")
             .withColumn("failure",(~F.col("passed")).cast("int")))
    current.write.mode("overwrite").parquet("data/observability/current_metrics")
    history=spark.read.parquet("data/quality/metric_history")
    summary=history.groupBy("metric_name").agg(
        F.count("*").alias("observations"),F.round(F.avg("metric_value"),3).alias("historical_avg"),
        F.round(F.min("metric_value"),3).alias("historical_min"),
        F.round(F.max("metric_value"),3).alias("historical_max"),
        F.sum((~F.col("passed")).cast("int")).alias("failed_observations"))
    summary.write.mode("overwrite").parquet("data/observability/metric_summary")
    current.show(50,False)
    spark.stop()

if __name__=="__main__": main()
