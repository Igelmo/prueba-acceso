package extraction

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{arrays_zip, explode}

object DataSource {
  val spark: SparkSession = SparkSession.active
  import spark.implicits._

  private def flattenSources(metadataContentFlattened: DataFrame): DataFrame = {
    metadataContentFlattened
      .withColumn("new", arrays_zip($"sources.path", $"sources.name", $"sources.format"))
      .withColumn("fields", explode($"new"))
      .drop("sources", "sinks", "transformations")
  }

  private def getSourcesContent(metadataContentFlattened: DataFrame): List[Row] = {
    val sinksFlattened = flattenSources(metadataContentFlattened)
    sinksFlattened
      .withColumn("path", $"fields.0")
      .withColumn("name", $"fields.1")
      .withColumn("format", $"fields.2")
      .drop("new", "fields")
      .collect
      .toList
  }

  def readInputToDataFrames(metadataContentFlattened: DataFrame): DataFrame = {
    val sinksContent = getSourcesContent(metadataContentFlattened)
    sinksContent.map(row => {
      val path = row.getAs("path").toString.replace("WrappedArray(/", "").tail
      val name = row.getAs("name").toString
      val format = row.getAs("format").toString
      if(path.last == '*') spark.read.option("multiline", value = true).option("innerSchema", value = true).format(format).load(path)
      else spark.read.option("multiline", value = true).option("innerSchema", value = true).format(format).load(path+name+"."+format.toLowerCase)
    })reduce(_.union(_))
  }


}
