package PlotGeneration.Core

import PlotGeneration.Config.{GlobalConf, PlotGenerationConf}
import PlotGeneration.Config.PlotGenerationConf.Execution.DTO._
import Utils.ConsoleLogUtils.Message.{NotificationType, printlnConsoleEnclosedMessage}
import Utils.HTTPUtils.{buildUrl, sendPostRequest}
import Utils.JSONUtils.removeNullKeys
import Utils.Storage.Core.Storage
import Utils.Storage.JSON.JSONStorageBackend.readJSON
import Utils.{ChronoUtils, ConsoleLogUtils}
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
  private val ctsGlobalInit = GlobalConf.Constants.init
  private val ctsGlobalUtils = GlobalConf.Constants.utils
  private val chronometer = ChronoUtils.Chronometer()

  private implicit val dataStorage: Storage = GlobalConf.Constants.dataStorage

  ctsGlobalInit.environmentVars.values.autoPlotUrlBase.getOrElse(
    throw new Exception(ctsGlobalUtils.errors.environmentVariableNotFound.format(
      ctsGlobalInit.environmentVars.names.autoPlotUrlBase
    ))
  )

  private object Stations {
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

  private object Climograph {
    private case class StationInfo(stationName: String, stationId: String, state: String, latitude: String, longitude: String, altitude: Int)
    private case class MeteoParamInfo(meteoParam: String, meteoParamAbbrev: String, units: String, colAggMethod: String)
    private case class ClimographInfo(tempParamInfo: MeteoParamInfo, precParamInfo: MeteoParamInfo, climateType: String, climateSubtype: String, climateLocation: String)
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
          ),
          axis = dto.src.axis.copy(
            yTemp = dto.src.axis.yTemp.copy(
              name = dto.src.axis.yTemp.name.format(
                formatInfo.climographInfo.tempParamInfo.meteoParamAbbrev,
                formatInfo.climographInfo.tempParamInfo.colAggMethod
              )
            ),
            yPrec = dto.src.axis.yPrec.copy(
              name = dto.src.axis.yPrec.name.format(
                formatInfo.climographInfo.precParamInfo.meteoParamAbbrev,
                formatInfo.climographInfo.precParamInfo.colAggMethod
              )
            )
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
            ),
            yTempLabel = dto.style.lettering.yTempLabel.format(
              formatInfo.climographInfo.tempParamInfo.meteoParam.capitalize,
              formatInfo.climographInfo.tempParamInfo.units
            ),
            yPrecLabel = dto.style.lettering.yPrecLabel.format(
              formatInfo.climographInfo.precParamInfo.meteoParam.capitalize,
              formatInfo.climographInfo.precParamInfo.units
            )
          ),
          figureTemp = dto.style.figureTemp.copy(
            name = dto.style.figureTemp.name.format(
              formatInfo.climographInfo.tempParamInfo.meteoParam.capitalize
            )
          ),
          figurePrec = dto.style.figurePrec.copy(
            name = dto.style.figurePrec.name.format(
              formatInfo.climographInfo.precParamInfo.meteoParam.capitalize
            )
          ),
        )
      )
    }

    def generate(): Unit = {
      printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.startPlotGeneration.format(
        ctsLogs.studyName
      ))

      ctsExecution.climographValues.climateRecords.foreach(climateGroup => {

        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.climateType.format(
          climateGroup.climateGroupName
        ), encloseHalfLength = encloseHalfLength)

        climateGroup.climates.foreach(climateRecord => {

          printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.climateSubtype.format(
            climateRecord.climateName
          ), encloseHalfLength = encloseHalfLength + 5)

          climateRecord.locations.foreach(location => {

            printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.climateLocation.format(
              location
            ), encloseHalfLength = encloseHalfLength + 10)

            val stationJSON = readJSON(ctsStorage.climograph.dataSrcStation.format(
              climateGroup.climateGroupName,
              climateRecord.climateName,
              location.replace(" ", "_")
            )) match {
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
                    MeteoParamInfo(
                      ctsExecution.climographValues.meteoParams.temperature.studyParamName,
                      ctsExecution.climographValues.meteoParams.temperature.studyParamAbbrev,
                      ctsExecution.climographValues.meteoParams.temperature.studyParamUnit,
                      ctsExecution.climographValues.meteoParams.temperature.colAggMethod,
                    ),
                    MeteoParamInfo(
                      ctsExecution.climographValues.meteoParams.precipitation.studyParamName,
                      ctsExecution.climographValues.meteoParams.precipitation.studyParamAbbrev,
                      ctsExecution.climographValues.meteoParams.precipitation.studyParamUnit,
                      ctsExecution.climographValues.meteoParams.precipitation.colAggMethod,
                    ),
                    climateGroup.climateGroupName,
                    climateRecord.climateName,
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

  private object SingleParamStudies {
    private case class MeteoParamInfo(meteoParam: String, meteoParamAbbrev: String, units: String, colAggMethod: String)
    private case class Top10TemporalInfo(value: String, title: String)
    private case class Top10FormatInfo(meteoParamInfo: MeteoParamInfo, order: String, temporal: Top10TemporalInfo)
    private case class Top5IncFormatInfo(meteoParamInfo: MeteoParamInfo, order: String)
    private case class StationInfo(stationName: String, stationId: String, state: String, stateNoSc: String, latitude: String, longitude: String, altitude: Int)
    private case class EvolFormatInfo(meteoParamInfo: MeteoParamInfo, stationInfo: StationInfo)
    private case class HeatMap2024FormatInfo(meteoParamInfo: MeteoParamInfo, location: String)

    private val ctsExecution = PlotGenerator.ctsExecution.singleParamStudiesConf
    private val ctsStorage = PlotGenerator.ctsStorage.singleParamStudiesConf
    private val ctsLogs = PlotGenerator.ctsLogs.singleParamStudiesConf

    private def barDTOTop10Formatter(dto: BarDTO, formatInfo: Top10FormatInfo): BarDTO = {
      dto.copy(
        src = dto.src.copy(
          path = dto.src.path.format(
            formatInfo.meteoParamInfo.meteoParamAbbrev,
            formatInfo.order,
            formatInfo.temporal.value
          ),
          axis = dto.src.axis.copy(
            y = dto.src.axis.y.copy(
              name = dto.src.axis.y.name.format(
                formatInfo.meteoParamInfo.meteoParamAbbrev,
                formatInfo.meteoParamInfo.colAggMethod
              )
            )
          )
        ),
        dest = dto.dest.copy(
          path = dto.dest.path.format(
            formatInfo.meteoParamInfo.meteoParamAbbrev,
            formatInfo.order,
            formatInfo.temporal.value
          )
        ),
        style = dto.style.copy(
          lettering = dto.style.lettering.copy(
            title = dto.style.lettering.title.format(
              formatInfo.order,
              formatInfo.meteoParamInfo.meteoParam
            ),
            subtitle = dto.style.lettering.subtitle.format(
              formatInfo.temporal.title
            ),
            yLabel = dto.style.lettering.yLabel.format(
              formatInfo.meteoParamInfo.meteoParam.capitalize,
              formatInfo.meteoParamInfo.units
            )
          )
        )
      )
    }

    private def barDTOTop5IncFormatter(dto: BarDTO, formatInfo: Top5IncFormatInfo): BarDTO = {
      dto.copy(
        src = dto.src.copy(
          path = dto.src.path.format(
            formatInfo.meteoParamInfo.meteoParamAbbrev,
            formatInfo.order
          )
        ),
        dest = dto.dest.copy(
          path = dto.dest.path.format(
            formatInfo.meteoParamInfo.meteoParamAbbrev,
            formatInfo.order
          )
        ),
        style = dto.style.copy(
          lettering = dto.style.lettering.copy(
            title = dto.style.lettering.title.format(
              formatInfo.order,
              formatInfo.meteoParamInfo.meteoParam
            ),
            yLabel = dto.style.lettering.yLabel.format(
              formatInfo.meteoParamInfo.meteoParam.capitalize,
              formatInfo.meteoParamInfo.units
            )
          )
        )
      )
    }

    private def linearDTOEvol2024Formatter(dto: LinearDTO, formatInfo: EvolFormatInfo): LinearDTO = {
      dto.copy(
        src = dto.src.copy(
          path = dto.src.path.format(
            formatInfo.meteoParamInfo.meteoParamAbbrev,
            formatInfo.stationInfo.stateNoSc
          ),
          axis = dto.src.axis.copy(
            y = dto.src.axis.y.copy(
              name = dto.src.axis.y.name.format(
                formatInfo.meteoParamInfo.meteoParamAbbrev,
                formatInfo.meteoParamInfo.colAggMethod
              )
            )
          )
        ),
        dest = dto.dest.copy(
          path = dto.dest.path.format(
            formatInfo.meteoParamInfo.meteoParamAbbrev,
            formatInfo.stationInfo.stateNoSc
          )
        ),
        style = dto.style.copy(
          lettering = dto.style.lettering.copy(
            title = dto.style.lettering.title.format(
              formatInfo.stationInfo.stationName,
              formatInfo.stationInfo.stationId,
              formatInfo.stationInfo.state,
              formatInfo.meteoParamInfo.meteoParam,
            ),
            subtitle = dto.style.lettering.subtitle.format(
              formatInfo.stationInfo.latitude,
              formatInfo.stationInfo.longitude,
              formatInfo.stationInfo.altitude
            ),
            yLabel = dto.style.lettering.yLabel.format(
              formatInfo.meteoParamInfo.meteoParam.capitalize,
              formatInfo.meteoParamInfo.units
            )
          ),
          figure = dto.style.figure.copy(
            name = dto.style.figure.name.format(
              formatInfo.meteoParamInfo.meteoParam.capitalize,
            )
          )
        )
      )
    }

    private def linearRegressionDTOEvolYearlyGroupFormatter(dto: LinearRegressionDTO, formatInfo: EvolFormatInfo): LinearRegressionDTO = {
      dto.copy(
        src = dto.src.copy(
          main = dto.src.main.copy(
            path = dto.src.main.path.format(
              formatInfo.meteoParamInfo.meteoParamAbbrev,
              formatInfo.stationInfo.stateNoSc
            ),
            axis = dto.src.main.axis.copy(
              y = dto.src.main.axis.y.copy(
                name = dto.src.main.axis.y.name.format(
                  formatInfo.meteoParamInfo.meteoParamAbbrev,
                  formatInfo.meteoParamInfo.colAggMethod
                )
              )
            )
          ),
          regression = dto.src.regression.copy(
            path = dto.src.regression.path.format(
              formatInfo.meteoParamInfo.meteoParamAbbrev,
              formatInfo.stationInfo.stateNoSc
            )
          )
        ),
        dest = dto.dest.copy(
          path = dto.dest.path.format(
            formatInfo.meteoParamInfo.meteoParamAbbrev,
            formatInfo.stationInfo.stateNoSc
          )
        ),
        style = dto.style.copy(
          lettering = dto.style.lettering.copy(
            title = dto.style.lettering.title.format(
              formatInfo.stationInfo.stationName,
              formatInfo.stationInfo.stationId,
              formatInfo.stationInfo.state,
              formatInfo.meteoParamInfo.meteoParam,
            ),
            subtitle = dto.style.lettering.subtitle.format(
              formatInfo.stationInfo.latitude,
              formatInfo.stationInfo.longitude,
              formatInfo.stationInfo.altitude
            ),
            y1Label = dto.style.lettering.y1Label.format(
              formatInfo.meteoParamInfo.meteoParam.capitalize,
              formatInfo.meteoParamInfo.units
            ),
            y2Label = dto.style.lettering.y2Label.format(
              formatInfo.meteoParamInfo.meteoParam.capitalize,
              formatInfo.meteoParamInfo.units
            )
          ),
          figure1 = dto.style.figure1.copy(
            name = dto.style.figure1.name.format(
              formatInfo.meteoParamInfo.meteoParam.capitalize,
            )
          ),
          figure2 = dto.style.figure2.copy(
            name = dto.style.figure2.name.format(
              formatInfo.meteoParamInfo.meteoParam.capitalize,
            )
          ),
        )
      )
    }

    private def heatMapDTOHeatMap2024Formatter(dto: HeatMapDTO, formatInfo: HeatMap2024FormatInfo): HeatMapDTO = {
      dto.copy(
        src = dto.src.copy(
          path = dto.src.path.format(
            formatInfo.meteoParamInfo.meteoParamAbbrev,
            formatInfo.location
          ),
          names = dto.src.names.copy(
            value = dto.src.names.value.format(
              formatInfo.meteoParamInfo.meteoParamAbbrev,
              formatInfo.meteoParamInfo.colAggMethod
            )
          ),
          location = dto.src.location.format(
            formatInfo.location
          )
        ),
        dest = dto.dest.copy(
          path = dto.dest.path.format(
            formatInfo.meteoParamInfo.meteoParamAbbrev,
            formatInfo.location
          )
        ),
        style = dto.style.copy(
          lettering = dto.style.lettering.copy(
            title = dto.style.lettering.title.format(
              formatInfo.meteoParamInfo.meteoParam.capitalize
            ),
            subtitle = dto.style.lettering.subtitle.format(
              formatInfo.location.replace("_", " ")
            ),
            legendLabel = dto.style.lettering.legendLabel.format(
              formatInfo.meteoParamInfo.meteoParam.capitalize,
              formatInfo.meteoParamInfo.units
            )
          ),
          figure = dto.style.figure.copy(
            name = dto.style.figure.name.format(
              formatInfo.meteoParamInfo.meteoParam
            )
          )
        )
      )
    }

    def generate(): Unit = {
      printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.startPlotGeneration.format(
        ctsLogs.studyName
      ))

      ctsSchemaSpark.meteoParamsValues.toList.foreach(studyParamValue => {

        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.meteoParamStudy.format(
          studyParamValue.studyParamName.capitalize
        ), encloseHalfLength = encloseHalfLength + 5)

        // -- TOP 10 --
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.top10Study.format(
          studyParamValue.studyParamName
        ), encloseHalfLength = encloseHalfLength + 10)

        ctsExecution.top10Values.order.foreach(top10Order => {

          printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.top10Order.format(
            top10Order, studyParamValue.studyParamName
          ), encloseHalfLength = encloseHalfLength + 15)

          ctsExecution.top10Values.temporal.foreach(top10Temporal => {

            printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.top10Temporal.format(
              top10Temporal.title.capitalize
            ), encloseHalfLength = encloseHalfLength + 20)

            generatePlot(
              buildUrl(ctsExecution.top10.uri),
              barDTOTop10Formatter(
                ctsExecution.top10.body,
                Top10FormatInfo(
                  MeteoParamInfo(
                    studyParamValue.studyParamName,
                    studyParamValue.studyParamAbbrev,
                    studyParamValue.studyParamUnit,
                    studyParamValue.colAggMethod
                  ),
                  top10Order,
                  Top10TemporalInfo(
                    top10Temporal.value,
                    top10Temporal.title
                  )
                )
              )
            )

          })
        })

        // -- TOP 5 INC --
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.top5IncStudy.format(
          studyParamValue.studyParamName
        ), encloseHalfLength = encloseHalfLength + 10)

        ctsExecution.top5IncValues.order.foreach(top5IncOrder => {

          printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.top5IncOrder.format(
            top5IncOrder, studyParamValue.studyParamName
          ), encloseHalfLength = encloseHalfLength + 15)

          generatePlot(
            buildUrl(ctsExecution.top5Inc.uri),
            barDTOTop5IncFormatter(
              ctsExecution.top5Inc.body,
              Top5IncFormatInfo(
                MeteoParamInfo(
                  studyParamValue.studyParamName,
                  studyParamValue.studyParamAbbrev,
                  studyParamValue.studyParamUnit,
                  ctsSchemaSpark.groupMethods.avg
                ),
                top5IncOrder
              )
            )
          )
        })

        // -- EVOL --
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.evolStudy.format(
          studyParamValue.studyParamName.capitalize
        ), encloseHalfLength = encloseHalfLength + 10)

        ctsSchemaSpark.stateValues.toList.foreach(stateValue => {

          printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.evolState.format(
            stateValue.stateName.capitalize
          ), encloseHalfLength = encloseHalfLength + 15)

          // -- EVOL 2024 --
          printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.evol2024.format(
            studyParamValue.studyParamName.capitalize
          ), encloseHalfLength = encloseHalfLength + 20)

          val station2024JSON = readJSON(ctsStorage.evol.evol2024.dataSrcStation.format(
            studyParamValue.studyParamAbbrev,
            stateValue.stateNameNoSc
          )) match {
            case Left(exception: Exception) =>
              ConsoleLogUtils.Message.printlnConsoleMessage(NotificationType.Warning, exception.toString)
              return
            case Right(json: Value) => json
          }

          generatePlot(
            buildUrl(ctsExecution.evol2024.uri),
            linearDTOEvol2024Formatter(
              ctsExecution.evol2024.body,
              EvolFormatInfo(
                MeteoParamInfo(
                  studyParamValue.studyParamName,
                  studyParamValue.studyParamAbbrev,
                  studyParamValue.studyParamUnit,
                  studyParamValue.colAggMethod
                ),
                StationInfo(
                  station2024JSON(ctsSchemaSpark.stationsDf.stationName).str,
                  station2024JSON(ctsSchemaSpark.stationsDf.stationId).str,
                  station2024JSON(ctsSchemaSpark.stationsDf.state).str,
                  stateValue.stateNameNoSc,
                  station2024JSON(ctsSchemaSpark.stationsDf.latDms).str,
                  station2024JSON(ctsSchemaSpark.stationsDf.longDms).str,
                  station2024JSON(ctsSchemaSpark.stationsDf.altitude).num.toInt,
                )
              )
            )
          )

          // -- EVOL YEARLY GROUP --
          printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.evolYearlyGroup.format(
            studyParamValue.studyParamName.capitalize
          ), encloseHalfLength = encloseHalfLength + 20)

          val stationGlobalJSON = readJSON(ctsStorage.evol.evolYearlyGroup.dataSrcStation.format(
            studyParamValue.studyParamAbbrev,
            stateValue.stateNameNoSc
          )) match {
            case Left(exception: Exception) =>
              ConsoleLogUtils.Message.printlnConsoleMessage(NotificationType.Warning, exception.toString)
              return
            case Right(json: Value) => json
          }

          generatePlot(
            buildUrl(ctsExecution.evolYearlyGroup.uri),
            linearRegressionDTOEvolYearlyGroupFormatter(
              ctsExecution.evolYearlyGroup.body,
              EvolFormatInfo(
                MeteoParamInfo(
                  studyParamValue.studyParamName,
                  studyParamValue.studyParamAbbrev,
                  studyParamValue.studyParamUnit,
                  studyParamValue.colAggMethod
                ),
                StationInfo(
                  stationGlobalJSON(ctsSchemaSpark.stationsDf.stationName).str,
                  stationGlobalJSON(ctsSchemaSpark.stationsDf.stationId).str,
                  stationGlobalJSON(ctsSchemaSpark.stationsDf.state).str,
                  stateValue.stateNameNoSc,
                  stationGlobalJSON(ctsSchemaSpark.stationsDf.latDms).str,
                  stationGlobalJSON(ctsSchemaSpark.stationsDf.longDms).str,
                  stationGlobalJSON(ctsSchemaSpark.stationsDf.altitude).num.toInt,
                )
              )
            )
          )
        })

        // -- HEAT MAP 2024 --
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.heatMap2024.format(
          studyParamValue.studyParamName.capitalize
        ), encloseHalfLength = encloseHalfLength + 10)

        ctsExecution.heatMap2024Values.foreach(location => {

          printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.heatMap2024Location.format(
            location.replace("_", " ").capitalize
          ), encloseHalfLength = encloseHalfLength + 15)

          generatePlot(
            buildUrl(ctsExecution.heatMap2024.uri),
            heatMapDTOHeatMap2024Formatter(
              ctsExecution.heatMap2024.body,
              HeatMap2024FormatInfo(
                MeteoParamInfo(
                  studyParamValue.studyParamName,
                  studyParamValue.studyParamAbbrev,
                  studyParamValue.studyParamUnit,
                  studyParamValue.colAggMethod
                ),
                location
              )
            )
          )
        })
      })

      printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.endPlotGeneration.format(
        ctsLogs.studyName
      ))
    }
  }

  private object InterestingStudies {
    private case class MeteoParamInfo(meteoParam: String, meteoParamAbbrev: String, units: String, colAggMethod: String)
    private case class StationInfo(stationName: String, stationId: String, state: String, stateNoSc: String, latitude: String, longitude: String, altitude: Int)
    private case class PrecPressEvolFormatInfo(precMeteoInfo: MeteoParamInfo, pressMeteoInfo: MeteoParamInfo, stationInfo: StationInfo)
    private case class Top10StatesFormatInfo(studyName: String, studyAbbrev: String)

    private val ctsExecution = PlotGenerator.ctsExecution.interestingStudiesConf
    private val ctsStorage = PlotGenerator.ctsStorage.interestingStudiesConf
    private val ctsLogs = PlotGenerator.ctsLogs.interestingStudiesConf

    private def doubleLinearDTOEvolPrecPressFormatter(dto: DoubleLinearDTO, formatInfo: PrecPressEvolFormatInfo): DoubleLinearDTO = {
      dto.copy(
        src = dto.src.copy(
          path = dto.src.path.format(
            formatInfo.stationInfo.stateNoSc
          ),
          axis = dto.src.axis.copy(
            y1 = dto.src.axis.y1.copy(
              name = dto.src.axis.y1.name.format(
                formatInfo.precMeteoInfo.meteoParamAbbrev,
                formatInfo.precMeteoInfo.colAggMethod
              )
            ),
            y2 = dto.src.axis.y2.copy(
              name = dto.src.axis.y2.name.format(
                formatInfo.pressMeteoInfo.meteoParamAbbrev,
                formatInfo.pressMeteoInfo.colAggMethod
              )
            )
          )
        ),
        dest = dto.dest.copy(
          path = dto.dest.path.format(
            formatInfo.stationInfo.stateNoSc
          )
        ),
        style = dto.style.copy(
          lettering = dto.style.lettering.copy(
            title = dto.style.lettering.title.format(
              formatInfo.stationInfo.stationName,
              formatInfo.stationInfo.stationId,
              formatInfo.stationInfo.state,
              formatInfo.precMeteoInfo.meteoParam,
              formatInfo.pressMeteoInfo.meteoParam,
            ),
            subtitle = dto.style.lettering.subtitle.format(
              formatInfo.stationInfo.latitude,
              formatInfo.stationInfo.longitude,
              formatInfo.stationInfo.altitude
            ),
            y1Label = dto.style.lettering.y1Label.format(
              formatInfo.precMeteoInfo.meteoParam.capitalize,
              formatInfo.precMeteoInfo.units
            ),
            y2Label = dto.style.lettering.y2Label.format(
              formatInfo.pressMeteoInfo.meteoParam.capitalize,
              formatInfo.pressMeteoInfo.units
            )
          ),
          figure1 = dto.style.figure1.copy(
            name = dto.style.figure1.name.format(
              formatInfo.precMeteoInfo.meteoParam.capitalize,
            )
          ),
          figure2 = dto.style.figure2.copy(
            name = dto.style.figure2.name.format(
              formatInfo.pressMeteoInfo.meteoParam.capitalize,
            )
          )
        )
      )
    }

    private def tableDTOTop10StatesFormatter(dto: TableDTO, formatInfo: Top10StatesFormatInfo): TableDTO = {
      dto.copy(
        src = dto.src.copy(
          path = dto.src.path.format(
            formatInfo.studyAbbrev
          )
        ),
        dest = dto.dest.copy(
          path = dto.dest.path.format(
            formatInfo.studyAbbrev
          )
        ),
        style = dto.style.copy(
          lettering = dto.style.lettering.copy(
            title = dto.style.lettering.title.format(
              formatInfo.studyName
            )
          )
        )
      )
    }

    def generate(): Unit = {
      printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.startPlotGeneration.format(
        ctsLogs.studyName
      ))

      // -- EVOL PREC PRESS --
      printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.evolPrecPressStudy, encloseHalfLength = encloseHalfLength + 5)

      ctsSchemaSpark.stateValues.toList.foreach(stateValue => {

        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.evolPrecPressState.format(
          stateValue.stateName.capitalize
        ), encloseHalfLength = encloseHalfLength + 10)

        // -- EVOL PREC PRESS 2024 --
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.evolPrecPress2024, encloseHalfLength = encloseHalfLength + 15)

        val station2024JSON = readJSON(ctsStorage.evolPrecPress.evol2024.dataSrcStation.format(
          stateValue.stateNameNoSc
        )) match {
          case Left(exception: Exception) =>
            ConsoleLogUtils.Message.printlnConsoleMessage(NotificationType.Warning, exception.toString)
            return
          case Right(json: Value) => json
        }

        generatePlot(
          buildUrl(ctsExecution.evolPrecPress2024.uri),
          doubleLinearDTOEvolPrecPressFormatter(
            ctsExecution.evolPrecPress2024.body,
            PrecPressEvolFormatInfo(
              MeteoParamInfo(
                ctsExecution.evolPrecPressValues.precipitation.studyParamName,
                ctsExecution.evolPrecPressValues.precipitation.studyParamAbbrev,
                ctsExecution.evolPrecPressValues.precipitation.studyParamUnit,
                ctsExecution.evolPrecPressValues.precipitation.colAggMethod
              ),
              MeteoParamInfo(
                ctsExecution.evolPrecPressValues.pressure.studyParamName,
                ctsExecution.evolPrecPressValues.pressure.studyParamAbbrev,
                ctsExecution.evolPrecPressValues.pressure.studyParamUnit,
                ctsExecution.evolPrecPressValues.pressure.colAggMethod
              ),
              StationInfo(
                station2024JSON(ctsSchemaSpark.stationsDf.stationName).str,
                station2024JSON(ctsSchemaSpark.stationsDf.stationId).str,
                station2024JSON(ctsSchemaSpark.stationsDf.state).str,
                stateValue.stateNameNoSc,
                station2024JSON(ctsSchemaSpark.stationsDf.latDms).str,
                station2024JSON(ctsSchemaSpark.stationsDf.longDms).str,
                station2024JSON(ctsSchemaSpark.stationsDf.altitude).num.toInt,
              )
            )
          )
        )

        // -- EVOL PREC PRESS YEARLY GROUP --
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.evolPrecPressYearlyGroup, encloseHalfLength = encloseHalfLength + 15)

        val stationGlobalJSON = readJSON(ctsStorage.evolPrecPress.evolYearlyGroup.dataSrcStation.format(
          stateValue.stateNameNoSc
        )) match {
          case Left(exception: Exception) =>
            ConsoleLogUtils.Message.printlnConsoleMessage(NotificationType.Warning, exception.toString)
            return
          case Right(json: Value) => json
        }

        generatePlot(
          buildUrl(ctsExecution.evolPrecPressYearlyGroup.uri),
          doubleLinearDTOEvolPrecPressFormatter(
            ctsExecution.evolPrecPressYearlyGroup.body,
            PrecPressEvolFormatInfo(
              MeteoParamInfo(
                ctsExecution.evolPrecPressValues.precipitation.studyParamName,
                ctsExecution.evolPrecPressValues.precipitation.studyParamAbbrev,
                ctsExecution.evolPrecPressValues.precipitation.studyParamUnit,
                ctsExecution.evolPrecPressValues.precipitation.colAggMethod
              ),
              MeteoParamInfo(
                ctsExecution.evolPrecPressValues.pressure.studyParamName,
                ctsExecution.evolPrecPressValues.pressure.studyParamAbbrev,
                ctsExecution.evolPrecPressValues.pressure.studyParamUnit,
                ctsExecution.evolPrecPressValues.pressure.colAggMethod
              ),
              StationInfo(
                stationGlobalJSON(ctsSchemaSpark.stationsDf.stationName).str,
                stationGlobalJSON(ctsSchemaSpark.stationsDf.stationId).str,
                stationGlobalJSON(ctsSchemaSpark.stationsDf.state).str,
                stateValue.stateNameNoSc,
                stationGlobalJSON(ctsSchemaSpark.stationsDf.latDms).str,
                stationGlobalJSON(ctsSchemaSpark.stationsDf.longDms).str,
                stationGlobalJSON(ctsSchemaSpark.stationsDf.altitude).num.toInt,
              )
            )
          )
        )

      })

      // -- TOP 10 --
      printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.top10StatesStudy, encloseHalfLength = encloseHalfLength + 5)

      ctsExecution.top10StatesValues.foreach(study => {

        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.top10StatesStudyName.format(
          study.name
        ), encloseHalfLength = encloseHalfLength + 10)

        generatePlot(
          buildUrl(ctsExecution.top10States.uri),
          tableDTOTop10StatesFormatter(
            ctsExecution.top10States.body,
            Top10StatesFormatInfo(
              study.name,
              study.nameAbbrev
            )
          )
        )
      })


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
    chronometer.start()

    // STATIONS
    Stations.generate()

    // CLIMOGRAPH
    Climograph.generate()

    // SINGLE PARAM STUDIES
    SingleParamStudies.generate()

    // INTERESTING STUDIES
    InterestingStudies.generate()

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalUtils.chrono.chronoResult.format(chronometer.stop()))
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalUtils.betweenStages.infoText.format(ctsGlobalUtils.betweenStages.millisBetweenStages / 1000))
    Thread.sleep(ctsGlobalUtils.betweenStages.millisBetweenStages)
  }
}


