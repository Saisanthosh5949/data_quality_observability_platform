import argparse
from pyspark.sql import functions as F
from src.common.spark_session import get_spark_session

def main(rows):
    spark=get_spark_session("GenerateOrders")
    statuses=["CREATED","PAID","SHIPPED","DELIVERED","CANCELLED"]
    df=(spark.range(1,rows+1).withColumnRenamed("id","sequence_id")
        .withColumn("order_id",F.format_string("ORD-%012d",F.col("sequence_id")))
        .withColumn("customer_id",F.format_string("CUST-%08d",(F.floor(F.rand(1)*100000)+1).cast("long")))
        .withColumn("product_id",F.format_string("PROD-%07d",(F.floor(F.rand(2)*20000)+1).cast("long")))
        .withColumn("quantity",(F.floor(F.rand(3)*5)+1).cast("int"))
        .withColumn("unit_price",F.round(5+F.rand(4)*500,2))
        .withColumn("status",F.element_at(F.array(*[F.lit(x) for x in statuses]),(F.floor(F.rand(5)*5)+1).cast("int")))
        .withColumn("event_timestamp",F.expr("current_timestamp() - INTERVAL 1 SECOND * CAST(rand(6)*86400 AS INT)"))
        .withColumn("source_system",F.lit("ORDER_SERVICE")))
    df=(df.withColumn("customer_id",F.when(F.rand(10)<.025,F.lit(None)).otherwise(F.col("customer_id")))
        .withColumn("quantity",F.when(F.rand(11)<.01,F.lit(-2)).otherwise(F.col("quantity")))
        .withColumn("unit_price",F.when(F.rand(12)<.008,F.lit(-25.0)).otherwise(F.col("unit_price")))
        .withColumn("status",F.when(F.rand(13)<.006,F.lit("UNKNOWN")).otherwise(F.col("status"))))
    dupes=df.filter(F.col("sequence_id")%500==0)
    out=df.unionByName(dupes)
    out.write.mode("overwrite").parquet("data/raw/orders")
    print(f"Generated {out.count():,} rows including intentional defects")
    spark.stop()

if __name__=="__main__":
    p=argparse.ArgumentParser();p.add_argument("--rows",type=int,default=250000)
    main(p.parse_args().rows)
