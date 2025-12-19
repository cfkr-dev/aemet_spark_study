package DataExtraction.Core

import DataExtraction.Config.{DataExtractionConf, GlobalConf}
import Utils.ChronoUtils
import Utils.ConsoleLogUtils.Message.{NotificationType, printlnConsoleEnclosedMessage, printlnConsoleMessage}
import Utils.Storage.Core.Storage
import Utils.Storage.JSON.JSONStorageBackend.readJSON
import Utils.Storage.Parquet.AvroParquetStorageBackend.withAvroParquetWriter
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters.{IteratorHasAsScala, SeqHasAsJava}
import scala.util.chaining.scalaUtilChainingOps

/**
 * DataReformatter coordinates the conversion of JSON data (AEMET and IFAPA
 * sources) into Avro/Parquet files following configured schemas.
 *
 * Responsibilities:
 * - Read metadata schemas stored in JSON and build Avro Schemas from them.
 * - Read JSON data files produced by extraction steps and write them as
 *   Parquet using Avro schemas.
 * - Orchestrate reformatting steps for both AEMET and IFAPA sources and
 *   record execution time using an internal chronometer.
 *
 * The main entry point is `reformatData()`, which executes all reformatting
 * steps in sequence and logs progress.
 */
object DataReformatter {
  private val ctsExecution = DataExtractionConf.Constants.execution.dataReformatterConf
  private val ctsLog = DataExtractionConf.Constants.log.dataReformatterConf
  private val ctsStorageAemet = DataExtractionConf.Constants.storage.aemetConf
  private val ctsStorageIfapaAemetFormat = DataExtractionConf.Constants.storage.ifapaAemetFormatConf
  private val ctsStorageDataReformatter = DataExtractionConf.Constants.storage.dataReformatterConf
  private val ctsGlobalUtils = GlobalConf.Constants.utils

  private val chronometer = ChronoUtils.Chronometer()

  private implicit val dataStorage: Storage = GlobalConf.Constants.dataStorage

  /**
   * Build an Avro Schema from a metadata JSON structure.
   *
   * The provided `metadataJSON` must follow the metadata structure defined in
   * configuration; fields are translated to Avro string fields. Fields marked
   * as optional in the metadata become nullable unions of null and string.
   *
   * @param metadataJSON metadata JSON describing the output schema
   * @return Avro Schema usable when writing Parquet via Avro-parquet writer
   */
  private def createParquetSchemaFromMetadata(metadataJSON: ujson.Value): Schema = {
    val fields = metadataJSON(ctsExecution.metadataStructure.schemaDef).arr.map { field =>
      val fieldSchema =
        if (field(ctsExecution.metadataStructure.fieldRequired).bool)
          Schema.create(Schema.Type.STRING)
        else
          Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))

      new Schema.Field(field(ctsExecution.metadataStructure.fieldId).str, fieldSchema, null, null)
    }

    Schema.createRecord("schema", null, null, false).tap(_.setFields(fields.toList.asJava))
  }

  /**
   * Orchestrate the complete reformatting pipeline.
   *
   * Steps executed (in this order):
   * 1. Reformat AEMET stations metadata into Parquet.
   * 2. Reformat AEMET meteorological data into Parquet (chunked by target size).
   * 3. Reformat IFAPA station metadata into Parquet.
   * 4. Reformat IFAPA meteorological data into Parquet.
   *
   * The method logs start/end for each stage and prints overall elapsed time.
   */
  def reformatData(): Unit = {
    chronometer.start()

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.aemetReformatStationsDataStart)
    AemetDataReformatter.reformatStationsData()
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.aemetReformatStationsDataEnd)

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.aemetReformatMeteoDataStart)
    AemetDataReformatter.reformatMeteoData()
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.aemetReformatMeteoDataEnd)

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.ifapaReformatStationsDataStart)
    IfapaDataReformatter.reformatStationsData()
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.ifapaReformatStationsDataEnd)

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.ifapaReformatMeteoDataStart)
    IfapaDataReformatter.reformatMeteoData()
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.ifapaReformatMeteoDataEnd)

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalUtils.chrono.chronoResult.format(chronometer.stop()))
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalUtils.betweenStages.infoText.format(ctsGlobalUtils.betweenStages.millisBetweenStages / 1000))
    Thread.sleep(ctsGlobalUtils.betweenStages.millisBetweenStages)
  }

  /**
   * Object that handles reformatting of AEMET source files into Parquet.
   *
   * Responsibilities:
   * - Chunk large sets of meteorological JSON files into groups approximating
   *   a target size to produce multiple Parquet outputs.
   * - Create Avro schemas from metadata and stream JSON records into Parquet
   *   using a writer provided by the storage backend.
   */
  private object AemetDataReformatter {
    private val targetChunkSize = ctsExecution.jsonFilesTargetChunkSize.toLong * 1024 * 1024

    private def buildChunks(files: Seq[Path]): Seq[Seq[Path]] = {
      files.foldLeft(Vector.empty[Vector[Path]] -> 0L) {
        case ((chunks, size), file) =>
          val fileSize = Files.size(file)

          if (size + fileSize > targetChunkSize && chunks.nonEmpty) {
            (chunks :+ Vector(file), fileSize)
          } else if (chunks.isEmpty) {
            (Vector(Vector(file)), fileSize)
          } else {
            (chunks.init :+ (chunks.last :+ file), size + fileSize)
          }
      }._1
    }

    /**
     * Reformat the AEMET stations JSON file into a Parquet file using the
     * Avro schema derived from the stations metadata JSON.
     */
    def reformatStationsData(): Unit = {
      val stationsFilePath = dataStorage.readDirectoryRecursive(ctsStorageAemet.allStationInfo.dirs.data)
      val stationsFile = Paths.get(stationsFilePath.resolve(ctsStorageAemet.allStationInfo.filenames.data).toString)

      val parquetSchema = createParquetSchemaFromMetadata(
        readJSON(ctsStorageAemet.allStationInfo.filepaths.metadata) match {
          case Left(exception: Exception) => throw exception
          case Right(json: ujson.Value) => json
        }
      )

      val outputPath = ctsStorageDataReformatter.aemet.allStationInfo.filepaths.data

      withAvroParquetWriter(outputPath, parquetSchema) { writer =>
        val arr = ujson.read(stationsFile).arr
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.reformatFile.format(stationsFile.getFileName.toString), encloseHalfLength = 35)
        arr.foreach { value =>
          val record: GenericRecord = new GenericData.Record(parquetSchema)
          value.obj.foreach {
            case (_, ujson.Null) =>
            case (k, v) => record.put(k, v.str)
          }
          writer.write(record)
        }
      } match {
        case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
        case Right(_) => ()
      }

      dataStorage.deleteLocalDirectoryRecursive(stationsFilePath.toString)
    }

    /**
     * Reformat AEMET meteorological JSON files into Parquet files. Files are
     * grouped into chunks based on size and each chunk is written to a separate
     * Parquet output (filename includes the first/last dates of the chunk).
     */
    def reformatMeteoData(): Unit = {
      val meteoFilesPath = dataStorage.readDirectoryRecursive(ctsStorageAemet.allMeteoInfo.dirs.data).toString
      val meteoFiles = Files
        .list(Paths.get(meteoFilesPath))
        .iterator()
        .asScala
        .toSeq
        .sortBy(_.getFileName.toString)

      val parquetSchema = createParquetSchemaFromMetadata(
        readJSON(ctsStorageAemet.allMeteoInfo.filepaths.metadata) match {
          case Left(exception: Exception) => throw exception
          case Right(json: ujson.Value) => json
        }
      )

      buildChunks(meteoFiles).foreach { chunk =>
        val outputPath = ctsStorageDataReformatter.aemet.allMeteoInfo.filepaths.data.format(
          chunk.head.getFileName.toString.split("\\.").head.split(ctsExecution.filenameDelimiterChar).take(3).mkString(ctsExecution.filenameDelimiterChar),
          chunk.last.getFileName.toString.split("\\.").head.split(ctsExecution.filenameDelimiterChar).drop(3).mkString(ctsExecution.filenameDelimiterChar)
        )

        withAvroParquetWriter(outputPath, parquetSchema) { writer =>
          chunk.foreach { path =>
            val arr = ujson.read(path).arr
            printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.reformatFile.format(path.getFileName.toString), encloseHalfLength = 35)
            arr.foreach { value =>
              val record: GenericRecord = new GenericData.Record(parquetSchema)
              value.obj.foreach {
                case (_, ujson.Null) =>
                case (k, v) => record.put(k, v.str)
              }
              writer.write(record)
            }
          }
        } match {
          case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
          case Right(_) => ()
        }
      }

      dataStorage.deleteLocalDirectoryRecursive(meteoFilesPath)
    }
  }

  /**
   * Object that handles reformatting of IFAPA source files into Parquet.
   *
   * Responsibilities:
   * - Read IFAPA-formatted AEMET JSON (single-station) files and write them
   *   to Parquet using Avro schemas derived from IFAPA/AEMET metadata.
   */
  private object IfapaDataReformatter {
    /**
     * Reformat IFAPA single-station info JSON into Parquet using its metadata.
     */
    def reformatStationsData(): Unit = {
      val stationsFilePath = dataStorage.readDirectoryRecursive(ctsStorageIfapaAemetFormat.singleStationInfo.dirs.data)
      val stationsFile = Paths.get(
        stationsFilePath.resolve(
          ctsStorageIfapaAemetFormat.singleStationInfo.filenames.data.format(
            ctsExecution.ifapa.singleStationInfo.stateCode,
            ctsExecution.ifapa.singleStationInfo.stationCode
          )
        ).toString
      )

      val parquetSchema = createParquetSchemaFromMetadata(
        readJSON(ctsStorageIfapaAemetFormat.singleStationInfo.filepaths.metadata) match {
          case Left(exception: Exception) => throw exception
          case Right(json: ujson.Value) => json
        }
      )

      val outputPath = ctsStorageDataReformatter.ifapa.singleStationInfo.filepaths.data.format(
        ctsExecution.ifapa.singleStationInfo.stateCode,
        ctsExecution.ifapa.singleStationInfo.stationCode
      )

      withAvroParquetWriter(outputPath, parquetSchema) { writer =>
        val obj = ujson.read(stationsFile).obj
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.reformatFile.format(stationsFile.getFileName.toString), encloseHalfLength = 35)
        val record: GenericRecord = new GenericData.Record(parquetSchema)
        obj.foreach {
          case (_, ujson.Null) =>
          case (k, v) => record.put(k, v.str)
        }
        writer.write(record)
      } match {
        case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
        case Right(_) => ()
      }

      dataStorage.deleteLocalDirectoryRecursive(stationsFilePath.toString)
    }

    /**
     * Reformat IFAPA single-station meteorological JSON into Parquet using its metadata.
     */
    def reformatMeteoData(): Unit = {
      val meteoFilePath = dataStorage.readDirectoryRecursive(ctsStorageIfapaAemetFormat.singleStationMeteoInfo.dirs.data)
      val meteoFile = Paths.get(
        meteoFilePath.resolve(
          ctsStorageIfapaAemetFormat.singleStationInfo.filenames.data.format(
            ctsExecution.ifapa.singleStationMeteoInfo.startDate,
            ctsExecution.ifapa.singleStationMeteoInfo.endDate,
          )
        ).toString
      )

      val parquetSchema = createParquetSchemaFromMetadata(
        readJSON(ctsStorageIfapaAemetFormat.singleStationMeteoInfo.filepaths.metadata) match {
          case Left(exception: Exception) => throw exception
          case Right(json: ujson.Value) => json
        }
      )

      val outputPath = ctsStorageDataReformatter.ifapa.singleStationMeteoInfo.filepaths.data.format(
        ctsExecution.ifapa.singleStationMeteoInfo.startDate,
        ctsExecution.ifapa.singleStationMeteoInfo.endDate,
      )

      withAvroParquetWriter(outputPath, parquetSchema) { writer =>
        val arr = ujson.read(meteoFile).arr
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.reformatFile.format(meteoFile.getFileName.toString), encloseHalfLength = 35)
        arr.foreach { value =>
          val record: GenericRecord = new GenericData.Record(parquetSchema)
          value.obj.foreach {
            case (_, ujson.Null) =>
            case (k, v) => record.put(k, v.str)
          }
          writer.write(record)
        }
      } match {
        case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
        case Right(_) => ()
      }

      dataStorage.deleteLocalDirectoryRecursive(meteoFilePath.toString)
    }
  }


}
