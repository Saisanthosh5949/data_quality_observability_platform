from pyspark.sql import functions as F

VALID_STATUSES=["CREATED","PAID","SHIPPED","DELIVERED","CANCELLED"]

def apply_row_rules(df):
    return (df
      .withColumn("rule_customer_required",F.col("customer_id").isNotNull())
      .withColumn("rule_quantity_positive",F.col("quantity")>0)
      .withColumn("rule_price_positive",F.col("unit_price")>0)
      .withColumn("rule_valid_status",F.col("status").isin(VALID_STATUSES))
      .withColumn("rule_timestamp_required",F.col("event_timestamp").isNotNull())
      .withColumn("is_valid",F.col("rule_customer_required") & F.col("rule_quantity_positive")
                  & F.col("rule_price_positive") & F.col("rule_valid_status")
                  & F.col("rule_timestamp_required"))
      .withColumn("quality_reason",F.when(~F.col("rule_customer_required"),"MISSING_CUSTOMER")
          .when(~F.col("rule_quantity_positive"),"INVALID_QUANTITY")
          .when(~F.col("rule_price_positive"),"INVALID_PRICE")
          .when(~F.col("rule_valid_status"),"INVALID_STATUS")
          .when(~F.col("rule_timestamp_required"),"MISSING_TIMESTAMP")))
