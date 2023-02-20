import extraction.{DataSource, MetadataContent}
import load.OutputLoad
import org.apache.spark.sql.DataFrame
import transformation.{FieldsTransformation, FieldsValidation}

object Main {

  private final val instanceOfMetadata = MetadataContent
  private final val instanceOfDataSource = DataSource
  private final val instanceOfFieldsValidation = FieldsValidation
  private final val instanceOfFieldsTransformation = FieldsTransformation
  private final val instanceOfOutputLoad = OutputLoad
  def main(args: Array[String]) {

    //---------------------------read of metadata-------------------------------

    val metadataContent: DataFrame = instanceOfMetadata.generateMetadataContent("data/metadata.json")
    val metadataContentFlattened = instanceOfMetadata.flattenMetadata(metadataContent)

    //---------------------------read of source-------------------------------

    val sourceContent = instanceOfDataSource.readSources(metadataContentFlattened)

    //---------------------------transform data-------------------------------

    val validatedDataFrames = instanceOfFieldsValidation.applyDataValidations(metadataContentFlattened, sourceContent)
    val validate_KO = validatedDataFrames._1
    var validate_OK = validatedDataFrames._2

    validate_KO.show(false)
    validate_OK.show(false)

    validate_OK = instanceOfFieldsTransformation.applyDataTransformations(metadataContentFlattened, validate_OK)

    validate_OK.show(false)

    //---------------------------write output-------------------------------

    instanceOfOutputLoad.writeOutputToFiles(metadataContentFlattened, validate_OK, validate_KO)
  }
}