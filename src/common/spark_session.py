from pyspark.sql import SparkSession

def get_spark_session(app_name):
    spark=(SparkSession.builder.appName(app_name).master("local[*]")
           .config("spark.sql.session.timeZone","UTC")
           .config("spark.sql.shuffle.partitions","16")
           .config("spark.sql.adaptive.enabled","true").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark
