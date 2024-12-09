package bd.etl.training.data.loading

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.curator.shaded.com.google.common.io.Files
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import java.io.File
import java.nio.file.{Files, Paths}

import java.nio.file.Paths

object LicenseByCompanyETL {
  // Logger for better traceability
  val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    val mockHadoopHome = new File("mock_hadoop_home")
    val binDir = new File(mockHadoopHome, "bin")

    // Create the directories if they don't exist
    if (!mockHadoopHome.exists()) mockHadoopHome.mkdir()
    if (!binDir.exists()) binDir.mkdir()

    // Check if winutils.exe exists and notify if missing
    val winutilsPath = Paths.get(binDir.getAbsolutePath, "winutils.exe")

    System.setProperty("hadoop.home.dir", mockHadoopHome.getAbsolutePath)
    // Load configuration
    val config: Config = ConfigFactory.load()
    // Extract database and output configurations
    val dbConfig = config.getConfig("environment.database")
    val outputConfig = config.getConfig("environment.output")
    val dbUrl = dbConfig.getString("url")
    val dbDriver = dbConfig.getString("driver")
    val dbUser = dbConfig.getString("user")
    val dbPassword = dbConfig.getString("password")
    val license_with_company = dbConfig.getString("queries.license_with_company")
    val logQuery = dbConfig.getString("queries.log")
    val licenseCompanyOutputPath = outputConfig.getString("license_comopany")
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("License Log")
      .master("local[*]")
      .getOrCreate()
    // Load data using reusable function
    val licenseDF = loadData(spark, dbUrl, dbDriver, dbUser,
      dbPassword, license_with_company)
      .withColumn("LICENSE_ID", col("LICENSE_ID").cast("int"))
      .withColumn("COMPANY_BIN_ID",
        col("COMPANY_BIN_ID").cast("int"))
      .withColumn("MINISTRY_ID",
        col("MINISTRY_ID").cast("int"))
      .withColumn("AMOUNT", col("AMOUNT").cast("int"))

    // Verify data loading
    logger.info("Displaying LICENSE data:")
    licenseDF.show(false)
    // Persist data to disk
    saveData(licenseDF, licenseCompanyOutputPath)
  }
  /**
   * Function to load data from a database into a Spark
   DataFrame.
   *
   * @param spark SparkSession
   * @param url Database URL
   * @param driver Database driver
   * @param user Database username
   * @param password Database password
   * @param query Query to fetch data
   * @return DataFrame containing the query results
   */
  def loadData(spark: SparkSession, url: String, driver:
  String, user: String, password: String, query: String):
  DataFrame = {
    spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", password)
      .option("query", query)
      .option("header", "true") // Reads column names from the header
      .option("inferSchema", "true") // Infers data types foreach column
      .load()
  }
  /**
   * Function to save a DataFrame to disk in CSV format.
   *
   * @param df DataFrame to save
   * @param path File path to save the DataFrame
   */
  def saveData(df: DataFrame, path: String): Unit = {
    df.write
      .partitionBy("COMPANY_BIN_ID")
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(path)
  }
}
