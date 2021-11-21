import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession
  .builder()
  .appName("Cleaning Meta Information")
  .enableHiveSupport()
  .getOrCreate()

val df = spark.sql("SELECT * FROM pw1287.meta")

val df_cleaned = df.drop("icon", "description", "short_desc")
val df2 = df_cleaned.select(countDistinct("app_id"))
println("Number of distinct apps: " + df2.collect()(0)(0))
val df_cat = df_cleaned.select("category").groupBy("category").count
df_cat.orderBy(col("count").desc).show()
df_cleaned.select(avg("rating").as("avg rating")).show()

df_cleaned.write.mode("overwrite").saveAsTable("pw1287.meta_cleaned")

