package Core.PlotGeneration

import Config.GlobalConf
import Config.PlotGenerationConf
import Config.PlotGenerationConf.Execution.DTO.ClimographDTO
import Utils.ConsoleLogUtils
import Utils.ConsoleLogUtils.Message.{NotificationType, printlnConsoleEnclosedMessage}
import Utils.HTTPUtils.{buildUrl, sendPostRequest}
import Utils.JSONUtils.{readJSON, removeNullKeys}
import sttp.model.Uri
import ujson.Value
import upickle.default.{ReadWriter, writeJs}

object PlotGenerator {
  private val ctsSchemaAutoPlot = GlobalConf.Constants.schema.autoPlotConf
  private val ctsSchemaSpark = GlobalConf.Constants.schema.sparkConf
  private val ctsExecution = PlotGenerationConf.Constants.execution
  private val ctsStorage = PlotGenerationConf.Constants.storage
  private val ctsLogs = PlotGenerationConf.Constants.log
  private val ctsGlobalLogs = PlotGenerator.ctsLogs.globalConf
  private val encloseHalfLength = 35

  object Stations {
    private val ctsExecution = PlotGenerator.ctsExecution.stationsConf

    def generate(): Unit = {
      printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.globalConf.startPlotGeneration.format(
        ctsLogs.stationsConf.studyName
      ))

      // Count evolution
      printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.globalConf.generatingPlot.format(
        ctsLogs.stationsConf.stationCountEvolFromStart
      ), encloseHalfLength = encloseHalfLength)
      generatePlot(buildUrl(ctsExecution.countEvol.uri), ctsExecution.countEvol.body)

      // Count by state
      printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.globalConf.generatingPlot.format(
        ctsLogs.stationsConf.stationCountByState2024
      ), encloseHalfLength = encloseHalfLength)
      generatePlot(buildUrl(ctsExecution.countByState2024.uri), ctsExecution.countByState2024.body)

      // Count by altitude
      printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.globalConf.generatingPlot.format(
        ctsLogs.stationsConf.stationCountByAltitude2024
      ), encloseHalfLength = encloseHalfLength)
      generatePlot(buildUrl(ctsExecution.countByAltitude2024.uri), ctsExecution.countByAltitude2024.body)

      printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.globalConf.endPlotGeneration.format(
        ctsLogs.stationsConf.studyName
      ))
    }
  }

  object Climograph {
    private case class StationInfo(stationName: String, stationId: String, state: String, latitude: String, longitude: String, altitude: Int)
    private case class ClimographInfo(climateType: String, climateSubtype: String, climateLocation: String)
    private case class FormatInfo(stationInfo: StationInfo, climographInfo: ClimographInfo)

    private val ctsExecution = PlotGenerator.ctsExecution.climographConf
    private val ctsStorage = PlotGenerator.ctsStorage.climographConf
    private val ctsLogs = PlotGenerator.ctsLogs.climographConf

    private def climographDTOFormatter(dto: ClimographDTO, formatInfo: FormatInfo): ClimographDTO = {
      dto.copy(
        src = dto.src.copy(
          path = dto.src.path.format(
            formatInfo.climographInfo.climateType,
            formatInfo.climographInfo.climateSubtype,
            formatInfo.climographInfo.climateLocation.replace(" ", "_")
          )
        ),
        dest = dto.dest.copy(
          path = dto.dest.path.format(
            formatInfo.climographInfo.climateType,
            formatInfo.climographInfo.climateSubtype,
            formatInfo.climographInfo.climateLocation.replace(" ", "_")
          )
        ),
        style = dto.style.copy(
          lettering = dto.style.lettering.copy(
            title = dto.style.lettering.title.format(
              formatInfo.stationInfo.stationName,
              formatInfo.stationInfo.stationId,
              formatInfo.stationInfo.state
            ),
            subtitle = dto.style.lettering.subtitle.format(
              formatInfo.climographInfo.climateType,
              formatInfo.climographInfo.climateSubtype,
              formatInfo.stationInfo.latitude,
              formatInfo.stationInfo.longitude,
              formatInfo.stationInfo.altitude
            )
          )
        )
      )
    }

    def generate(): Unit = {
      printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.startPlotGeneration.format(
        ctsLogs.studyName
      ))

      ctsExecution.climateRegistries.foreach(climateGroup => {

        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.climateType.format(
          climateGroup.climateGroupName
        ), encloseHalfLength = encloseHalfLength)

        climateGroup.climates.foreach(climateRegistry => {

          printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.climateSubtype.format(
            climateRegistry.climateName
          ), encloseHalfLength = encloseHalfLength + 5)

          climateRegistry.locations.foreach(location => {

            printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.climateLocation.format(
              location
            ), encloseHalfLength = encloseHalfLength + 10)

            val stationJSON = readJSON(ctsStorage.climograph.dataSrcStation.format(
              climateGroup.climateGroupName,
              climateRegistry.climateName,
              location.replace(" ", "_")
            ), findHeaviest = true) match {
              case Left(exception: Exception) =>
                ConsoleLogUtils.Message.printlnConsoleMessage(NotificationType.Warning, exception.toString)
                return
              case Right(json: Value) => json
            }

            printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.generatingPlot.format(
              ctsLogs.studyName
            ), encloseHalfLength = encloseHalfLength + 10)

            generatePlot(
              buildUrl(ctsExecution.climograph.uri),
              climographDTOFormatter(
                ctsExecution.climograph.body,
                FormatInfo(
                  StationInfo(
                    stationJSON(ctsSchemaSpark.stationsDf.stationName).str,
                    stationJSON(ctsSchemaSpark.stationsDf.stationId).str,
                    stationJSON(ctsSchemaSpark.stationsDf.state).str,
                    stationJSON(ctsSchemaSpark.stationsDf.latDms).str,
                    stationJSON(ctsSchemaSpark.stationsDf.longDms).str,
                    stationJSON(ctsSchemaSpark.stationsDf.altitude).num.toInt,
                  ),
                  ClimographInfo(
                    climateGroup.climateGroupName,
                    climateRegistry.climateName,
                    location
                  )
                )
              )
            )

          })

        })

      })

      printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.endPlotGeneration.format(
        ctsLogs.studyName
      ))
    }

  }



  private def generateRequestBody[T: ReadWriter](dto: T): Value = {
    removeNullKeys(writeJs(dto))
  }

  private def generatePlot[T: ReadWriter](uri: Uri, dto: T): Unit = {
    sendPostRequest(uri, generateRequestBody(dto)) match {
      case Left(exception: Exception) => ConsoleLogUtils.Message.printlnConsoleMessage(NotificationType.Warning, exception.toString)
      case Right(response) => ConsoleLogUtils.Message.printlnConsoleMessage(
        NotificationType.Information,
        ctsLogs.globalConf.dataSaved.format(ujson.read(response.body)(ctsSchemaAutoPlot.response.destPath))
      )
    }
  }

  def generate(): Unit = {
    // STATIONS
    Stations.generate()

    // CLIMOGRAPH
    Climograph.generate()
  }
}


