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

object DataReformatter {
  private val ctsExecution = DataExtractionConf.Constants.execution.dataReformatterConf
  private val ctsLog = DataExtractionConf.Constants.log.dataReformatterConf
  private val ctsStorageAemet = DataExtractionConf.Constants.storage.aemetConf
  private val ctsStorageIfapaAemetFormat = DataExtractionConf.Constants.storage.ifapaAemetFormatConf
  private val ctsStorageDataReformatter = DataExtractionConf.Constants.storage.dataReformatterConf
  private val ctsGlobalUtils = GlobalConf.Constants.utils

  private val chronometer = ChronoUtils.Chronometer()

  private implicit val dataStorage: Storage = GlobalConf.Constants.dataStorage

  def reformatData(): Unit = {
    chronometer.start()

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.aemetReformatStationsDataStart)
    //AemetDataReformatter.reformatStationsData()
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.aemetReformatStationsDataEnd)

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.aemetReformatMeteoDataStart)
    AemetDataReformatter.reformatMeteoData()
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.aemetReformatMeteoDataEnd)

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.ifapaReformatStationsDataStart)
    //IfapaDataReformatter.reformatStationsData()
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.ifapaReformatStationsDataEnd)

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.ifapaReformatMeteoDataStart)
    //IfapaDataReformatter.reformatMeteoData()
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.ifapaReformatMeteoDataEnd)

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalUtils.chrono.chronoResult.format(chronometer.stop()))
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalUtils.betweenStages.infoText.format(ctsGlobalUtils.betweenStages.millisBetweenStages / 1000))
    Thread.sleep(ctsGlobalUtils.betweenStages.millisBetweenStages)
  }

  private object AemetDataReformatter {
    private val targetChunkSize = ctsExecution.jsonFilesTargetChunkSize.toLong * 1024 * 1024

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

    def reformatMeteoData(): Unit = {
      val meteoFilesPath = dataStorage.readDirectoryRecursive(ctsStorageAemet.allMeteoInfo.dirs.data).toString

      val meteoFiles = Files
        .list(Paths.get(meteoFilesPath))
        .iterator()
        .asScala
        .filter(file => file.toString.endsWith(".json"))
        .toSeq

      val parquetSchema = createParquetSchemaFromMetadata(
        readJSON(ctsStorageAemet.allMeteoInfo.filepaths.metadata) match {
          case Left(exception: Exception) => throw exception
          case Right(json: ujson.Value) => json
        }
      )

      buildChunks(meteoFiles).foreach { chunk =>
        val outputPath = ctsStorageDataReformatter.aemet.allMeteoInfo.filepaths.data.format(
          chunk.head.getFileName.toString.stripSuffix(".json").split("_").take(3).mkString("_"),
          chunk.last.getFileName.toString.stripSuffix(".json").split("_").drop(3).mkString("_")
        )

        withAvroParquetWriter(outputPath, parquetSchema) { writer =>
          chunk.foreach { path =>
            val arr = ujson.read(path.toFile).arr
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

  private object IfapaDataReformatter {

  }


}
