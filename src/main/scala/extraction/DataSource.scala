package extraction

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.explode

object DataSource {
  final val instanceOfMetadata = MetadataContent
  val spark = instanceOfMetadata.getSparkSession

  import spark.implicits._

  private def getRawSourcePaths(metadataContent: DataFrame): List[Row] =
    metadataContent.select(explode($"sources.path")).collect.toList

  private def getSourcePaths(metadataContent: DataFrame): List[String] =
    getRawSourcePaths(metadataContent).map(path => path.getString(0).tail)

  def readSources(metadataContent: DataFrame): DataFrame = {
    val paths = getSourcePaths(metadataContent)
    spark.read.option("multiline", true).json(paths.head)
  }

}
