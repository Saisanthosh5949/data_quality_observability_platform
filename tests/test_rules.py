from src.common.spark_session import get_spark_session
from src.quality.rules import apply_row_rules

def test_negative_quantity_is_invalid():
    spark=get_spark_session("RuleTest")
    df=spark.createDataFrame([("O1","C1","P1",-1,10.0,"PAID")],
        ["order_id","customer_id","product_id","quantity","unit_price","status"])
    from pyspark.sql import functions as F
    df=df.withColumn("event_timestamp",F.current_timestamp())
    row=apply_row_rules(df).first()
    assert row["is_valid"] is False
    assert row["quality_reason"]=="INVALID_QUANTITY"
    spark.stop()
