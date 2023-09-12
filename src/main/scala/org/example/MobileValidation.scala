import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_extract, when}

object MobileValidation {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("MyApp")
      .master("local[*]")
      .getOrCreate()
    val df = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv("D:/mobile/mob.csv")

    val regular_exp = "^(?:(?:\\+|0{0,2})91(\\s*[\\-]\\s*)?|[0]?)?[6789]\\d{9}$"

    val validated_df = df
      .withColumn("isValidMobileNumber", regexp_extract(col("mobile")
        , regular_exp, 0) =!= "")
      .withColumn("isValidMobileNumber", when(col("isValidMobileNumber")
        , "YES").otherwise("NO"))
    validated_df.show()

  }

}
