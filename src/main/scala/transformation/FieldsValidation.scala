package transformation

import extraction.MetadataContent
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{arrays_zip, col, explode, lit}

object FieldsValidation {
  final val instanceOfMetadata = MetadataContent
  val spark: SparkSession = SparkSession.active
  import spark.implicits._

  private def getDataValidations(metadataContentFlattened: DataFrame): List[Row] = {
    val transformValidateFields = metadataContentFlattened
      .withColumn("validations", explode($"transformations.params.validations"))
      .drop("sources", "sinks", "transformations").dropDuplicates()

    val transformValidationFieldsFlattened = transformValidateFields
      .withColumn("new", arrays_zip($"validations.field", $"validations.validations"))
      .withColumn("fields", explode($"new"))
      .drop("validations")

    transformValidationFieldsFlattened
      .withColumn("field", $"fields.0")
      .withColumn("validation", $"fields.1")
      .drop("new", "fields")
      .collect
      .toList
  }

  private def getAffectedRowsByValidations(metadataContentFlattened: DataFrame, sourceContent: DataFrame) = {
    val dataValidations = getDataValidations(metadataContentFlattened)

    dataValidations.map(row => {
      val column = row.getAs("field").toString
      val validation = row
        .getAs("validation")
        .toString
        .replace("WrappedArray(", "")
        .init
      val validationTransformed = validation
        .replace("not", "== ")
        .replace("Empty", "\"\"")

      if (validation == "notNull") sourceContent.filter(col(column).isNull).withColumn("error", lit(s"$column is NULL"))
      else sourceContent.where(s"$column $validationTransformed").withColumn("error", lit(s"$column doesn't meet the requirement of $validation"))
    }).reduce(_.union(_))
  }

  def applyDataValidations(metadataContentFlattened: DataFrame, sourceContent: DataFrame): (DataFrame, DataFrame) = {
    val validate_KO = getAffectedRowsByValidations(metadataContentFlattened, sourceContent)
    val validate_OK = sourceContent.except(validate_KO.select("age","name", "office"))
    (validate_KO, validate_OK)
  }
}
