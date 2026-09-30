from src.common.spark_session import get_spark_session

def main():
    spark=get_spark_session("QualityReport")
    r=spark.read.parquet("data/quality/check_results")
    print("\n=== DATA QUALITY REPORT ===")
    r.select("metric_name","metric_value","unit","passed").show(100,False)
    print("Failed checks:",r.filter("passed = false").count())
    print("Quarantined records:",spark.read.parquet("data/quarantine/orders").count())
    spark.stop()

if __name__=="__main__": main()
