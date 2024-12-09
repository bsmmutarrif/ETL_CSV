package bd.etl.training.data.analysis
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import scala.io.Source
object LicenseAnalysisSparkSQL {
  def main(args: Array[String]): Unit = {
    // Step 1: Load Configuration
    val config: Config = ConfigFactory.load()
    val outputConfig = config.getConfig("environment.output")
    val licenseCsvPath = outputConfig.getString("license")
    // Step 2: Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("License Analysis: Spark SQL")
      .master("local[*]")
      .getOrCreate()
    // Step 3: Load Data from CSV
    val licenseDF = loadCSV(spark, licenseCsvPath)
      .withColumn("LICENSE_ID", col("LICENSE_ID").cast("int"))
      .withColumn("COMPANY_BIN_ID", col("COMPANY_BIN_ID").cast("int"))
      .withColumn("AMOUNT", col("AMOUNT").cast("int"))
    licenseDF.createOrReplaceTempView("LICENSE")
    // Step 4: Execute SQL Queries
    val nearExpirationQuery = loadSQL("sql/near_expiration.sql")
    val revenueByCompanyQuery = loadSQL("sql/revenue_by_company.sql")
    println("Task 1: Licenses Nearing Expiration")
    val nearExpiration = spark.sql(nearExpirationQuery)
    nearExpiration.show()
    println("Task 2: Total Revenue by Company")
    val revenueByCompany = spark.sql(revenueByCompanyQuery)
    revenueByCompany.show()
  }
  def loadCSV(spark: SparkSession, path: String): DataFrame = {
    spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
  }
  def loadSQL(filePath: String): String = {
    val source = Source.fromResource(filePath)
    try source.getLines().mkString("\n") finally source.close()
  }
}