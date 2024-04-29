from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Script1").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

df = spark.read.option("inferSchema", True).option('header',True).csv(r"proj2_data\orders_sample.csv")

df.where('userId==3').select('orderId').show()

spark.stop()
