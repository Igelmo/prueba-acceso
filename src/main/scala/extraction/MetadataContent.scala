package extraction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.{DataFrame, SparkSession}

object MetadataContent {

  private def generateSparkSession: SparkSession = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    SparkSession
      .builder
      .appName("prueba-acceso")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()
  }

  private val spark: SparkSession = generateSparkSession

  def generateMetadataContent(path: String): DataFrame = {
    spark.read.option("inferSchema", "true").option("multiline", value = true).json(path)
  }

  //Get dataframe of metadata

  def flattenMetadata(metadataContent: DataFrame): DataFrame = {
    import spark.implicits._
    metadataContent
    .withColumn("sources", explode($"dataflows.sources"))
    .withColumn("sinks", explode($"dataflows.sinks"))
    .withColumn("transformations", explode($"dataflows.transformations"))
    .drop("dataflows").dropDuplicates()
  }
}
