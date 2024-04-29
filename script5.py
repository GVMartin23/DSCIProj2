from pyspark.sql import SparkSession
from pyspark.sql.functions import col,to_date

spark = SparkSession.builder.appName("Script5").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

df = spark.read.option("inferSchema", True).option('header',True).csv(r"proj2_data\orders_sample.csv")

df.where('userId==3').select('orderId').show()

spark.stop()