from datetime import datetime,timezone
from pyspark.sql import functions as F, Row
from pyspark.sql.window import Window
from src.common.spark_session import get_spark_session
from src.quality.rules import apply_row_rules

def main():
    spark=get_spark_session("QualityEngine")
    raw=spark.read.parquet("data/raw/orders")
    total=raw.count()
    duplicate_ids=raw.groupBy("order_id").count().filter("count > 1").count()
    w=Window.partitionBy("order_id").orderBy(F.col("event_timestamp").desc())
    dedup=raw.withColumn("rn",F.row_number().over(w)).filter("rn=1").drop("rn")
    checked=apply_row_rules(dedup)
    valid=checked.filter("is_valid = true")
    invalid=checked.filter("is_valid = false")
    valid_count=valid.count(); invalid_count=invalid.count()
    null_customers=dedup.filter(F.col("customer_id").isNull()).count()
    invalid_qty=dedup.filter(F.col("quantity")<=0).count()
    invalid_price=dedup.filter(F.col("unit_price")<=0).count()
    invalid_status=dedup.filter(~F.col("status").isin("CREATED","PAID","SHIPPED","DELIVERED","CANCELLED")).count()
    max_ts=dedup.agg(F.max("event_timestamp")).first()[0]
    now=datetime.now(timezone.utc).replace(tzinfo=None)
    freshness=(now-max_ts).total_seconds()/60 if max_ts else 999999
    score=max(0.0,100.0-((invalid_count+duplicate_ids)/max(total,1)*100))
    run_id=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    metrics=[
      ("total_records",total,"count",True),
      ("valid_records",valid_count,"count",True),
      ("invalid_records",invalid_count,"count",invalid_count/total < .05),
      ("duplicate_order_ids",duplicate_ids,"count",duplicate_ids/total < .01),
      ("null_customer_ids",null_customers,"count",null_customers/total < .03),
      ("invalid_quantity",invalid_qty,"count",invalid_qty/total < .02),
      ("invalid_price",invalid_price,"count",invalid_price/total < .02),
      ("invalid_status",invalid_status,"count",invalid_status/total < .01),
      ("freshness_minutes",freshness,"minutes",freshness < 120),
      ("quality_score",score,"percent",score >= 95)]
    result=spark.createDataFrame([Row(run_id=run_id,metric_name=n,metric_value=float(v),unit=u,passed=p)
                                  for n,v,u,p in metrics]).withColumn("measured_at",F.current_timestamp())
    valid.write.mode("overwrite").parquet("data/clean/orders")
    invalid.write.mode("overwrite").parquet("data/quarantine/orders")
    result.write.mode("overwrite").parquet("data/quality/check_results")
    result.write.mode("append").parquet("data/quality/metric_history")
    result.select("metric_name","metric_value","unit","passed").show(50,False)
    spark.stop()

if __name__=="__main__": main()
