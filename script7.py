from pyspark.sql import SparkSession
from pyspark.sql.functions import col,to_date

spark = SparkSession.builder.appName("Script7").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

orderMulti = spark.read.option("inferSchema", True).option('header',True).csv(r"proj2_data\product_sample1.csv")
products = spark.read.option("inferSchema", True).option('header',True).csv(r"proj2_data\products1.csv")

orderMulti.join(products, on=["productId"], how="left").groupBy("orderId", "aisleId").count().select('aisleId').groupBy("aisleId").count().show()

spark.stop()