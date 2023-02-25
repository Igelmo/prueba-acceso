package load

import extraction.MetadataContent
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{arrays_zip, explode}

object OutputLoad {
  final val instanceOfMetadata = MetadataContent
  val spark = SparkSession.active

  import spark.implicits._

  private def flattenSinks(metadataContentFlattened: DataFrame): DataFrame = {
    metadataContentFlattened
      .withColumn("new", arrays_zip($"sinks.paths", $"sinks.name", $"sinks.format", $"sinks.saveMode"))
      .withColumn("fields", explode($"new"))
      .drop("sources", "sinks", "transformations")
  }

  private def getSinksContent(metadataContentFlattened: DataFrame): List[Row]  = {
    val sinksFlattened = flattenSinks(metadataContentFlattened)
    sinksFlattened
      .withColumn("path", $"fields.0")
      .withColumn("name", $"fields.1")
      .withColumn("format", $"fields.2")
      .withColumn("saveMode", $"fields.3")
      .drop("new", "fields")
      .collect
      .toList
  }

  def writeOutputToFiles(metadataContentFlattened: DataFrame, validate_OK: DataFrame, validate_KO: DataFrame): Unit = {
    val sinksContent = getSinksContent(metadataContentFlattened)
    sinksContent.foreach(row => {
      val path = row.getAs("path").toString.replace("WrappedArray(/", "").init
      val name = row.getAs("name").toString
      val format = row.getAs("format").toString
      val saveMode = row.getAs("saveMode").toString
      if (name == "raw-ok") validate_OK.repartition(2).write.format(format).mode(saveMode).save(path + "/VALIDATE_OK")
      if (name == "raw-ko") validate_KO.repartition(2).write.format(format).mode(saveMode).save(path + "/VALIDATE_KO")
    })
  }
}
