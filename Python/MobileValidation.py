from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when,regexp_extract

spark = SparkSession.builder \
    .master("local[3]") \
    .appName("MobileValidation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read \
    .option("header", "true") \
    .option("delimiter", ",") \
    .csv("D:/mobile/mob.csv")

regular_exp = "^(?:(?:\\+|0{0,2})91(\\s*[\\-]\\s*)?|[0]?)?[6789]\\d{9}$"
validated_df = df \
    .withColumn("isValidMobileNumber", regexp_extract(col("mobile"), regular_exp, 0) != "") \
    .withColumn("isValidMobileNumber", when(col("isValidMobileNumber"), "YES").otherwise("NO"))

validated_df.show()