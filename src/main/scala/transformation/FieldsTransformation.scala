package transformation

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{current_timestamp, explode, lit}

object FieldsTransformation {
  val spark: SparkSession = SparkSession.active
  import spark.implicits._

  private def getDataTransformations(metadataContentFlattened: DataFrame): List[Row] = {
    val transformFields = metadataContentFlattened
      .withColumn("addFields", explode($"transformations.params.addFields"))
      .drop("sources", "sinks", "transformations")
      .dropDuplicates()

    transformFields
      .withColumn("function", explode($"addFields.function"))
      .withColumn("name", explode($"addFields.name"))
      .drop("addFields")
      .collect
      .toList
  }

  def applyDataTransformations(metadataContentFlattened: DataFrame, validate_OK: DataFrame): DataFrame = {
    val dataTransformations = getDataTransformations(metadataContentFlattened)

    dataTransformations.map(row => {
      val function = row.getAs("function").toString
      val name = row.getAs("name").toString
      if (function == "current_timestamp") validate_OK.withColumn(name, current_timestamp())
      else validate_OK.withColumn(name, lit(function))
    }).reduce(_.union(_))
  }
}
