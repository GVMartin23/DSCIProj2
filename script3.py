from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Script3").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

df = spark.read.option("inferSchema", True).option('header',True).csv(r"proj2_data\product_sample1.csv")

df.groupBy("productId").count().where("count>=10").select("productId").show()

spark.stop()