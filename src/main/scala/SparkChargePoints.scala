import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object SparkChargePoints {
  val input = "data/input/electric-chargepoints-2017.csv"
  val output = "data/output/chargepoints-2017-analysis"
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkChargePoints")
    .getOrCreate()

  def extract(): DataFrame = {
    // Read & return the data
    spark.read.option("header", "true").csv(input)
  }

  def transform(df: DataFrame): DataFrame = {
    // Group by Charge-point ID column and use aggregation functions avg() and max() to calculate the required metrics
    val df2 = df.groupBy("CPID").agg(
      avg("PluginDuration").alias("AvgDuration"),
      max("PluginDuration").alias("MaxDuration")
    )
    // Rename columns to the specified names
    df2.withColumnRenamed("CPID", "chargepoint_id")
       .withColumnRenamed("AvgDuration", "avg_duration")
       .withColumnRenamed("MaxDuration", "max_duration")
  }

  def load(df: DataFrame): DataFrame = {
    // Save at the output path in parquet format
    df.write.parquet(output)
    df
  }

  def main(args: Array[String]): Unit = {
    load(transform(extract()))
  }
}
