from pyspark.sql import SparkSession
from pyspark.sql.functions import col,to_date

spark = SparkSession.builder.appName("Script1").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

df = spark.read.option('header',True).csv(r"proj2_data\orders_sample.csv")

print(df.where('userId==3').select('orderId').show())

spark.stop()
