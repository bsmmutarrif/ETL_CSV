package bd.etl.training.data.analysis
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
object LicenseAnalysisDataFrameAPI {
  def main(args: Array[String]): Unit = {
    val config: Config = ConfigFactory.load()
    val outputConfig = config.getConfig("environment.output")
    val licenseCsvPath = outputConfig.getString("license")
    val nearExpirationPath = outputConfig.getString("nearExpirationOutput")
    val ministrySummaryPath = outputConfig.getString("ministrySummaryOutput")
    val spark = SparkSession.builder()
      .appName("License Data Analysis: Spark DataFrame API")
      .master("local[*]")
      .getOrCreate()
    val licenseDF = loadCSV(spark, licenseCsvPath)
    licenseDF.show(false)
    println("Task 1: Licenses Nearing Expiration")
    val nearExpiration = licenseDF.filter( // = WHERE from SQL
      col("STATUS") === "ACTIVE" &&
        datediff(col("EXPIRATION_DATE"), current_date()) <= 30
    )
    nearExpiration.show(false)
    saveToCSV(nearExpiration, nearExpirationPath)
    println("Task 2: Ministry-Level Summary")
    val ministrySummary = licenseDF.groupBy("MINISTRY_ID")
      .agg(
        count("LICENSE_ID").alias("License_Count"),
        sum("INVOICE_AMOUNT").alias("Total_Amount")
      )
    ministrySummary.show(false)
    saveToCSV(ministrySummary, ministrySummaryPath)
  }
  def loadCSV(spark: SparkSession, path: String): DataFrame = {
    spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
  }
  def saveToCSV(df: DataFrame, path: String): Unit = {
    df.write.format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save(path)
  }
}