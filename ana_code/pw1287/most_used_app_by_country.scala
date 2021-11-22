import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window._

val spark = SparkSession
  .builder()
  .appName("Most Used Apps By Country")
  .enableHiveSupport()
  .getOrCreate()

val df = spark.sql("SELECT country, app_id, COUNT(*) AS cnt FROM pw1287.frappe GROUP BY country, app_id ORDER BY cnt DESC")

val w2 = partitionBy("country").orderBy(col("cnt").desc)

val df_result = df.withColumn("row", row_number.over(w2)).where($"row" === 1).drop("row")

df_result.write.mode("overwrite").saveAsTable("pw1287.frequent_app_by_country")



