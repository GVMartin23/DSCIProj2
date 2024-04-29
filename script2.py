from pyspark.sql import SparkSession
from pyspark.sql.functions import max

spark = SparkSession.builder.appName("Script2").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

df = spark.read.option("inferSchema", True).option('header',True).csv(r"proj2_data\orders_sample.csv")

df.groupBy("userId").agg(max("daysSincePriorOrder").alias("MaxDays")).show()

spark.stop()