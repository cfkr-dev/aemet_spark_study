/**
 * PlotGenerator orchestrates the creation of plots for the project.
 *
 * Responsibilities:
 * - Read precomputed JSON data produced by Spark queries.
 * - Format DTOs for the plotting microservice (AutoPlot) and send HTTP POST
 *   requests to trigger plot generation.
 * - Provide separate workflows for stations, climographs, single-parameter
 *   studies and interesting studies, each encapsulated in a private object.
 *
 * The object is purely procedural: call `generate()` to run the whole
 * generation pipeline (it reports progress via console logging utilities).
 */
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

  /**
   * Stations subplot generator.
   *
   * Produces simple station-related plots such as station count evolution,
   * counts by state and counts by altitude. This object prepares the DTOs
   * from the configuration and forwards them to the plotting backend.
   */
  private object Stations {
    private val ctsExecution = PlotGenerator.ctsExecution.stationsConf

    /**
     * Generate all station-related plots.
     *
     * Behavior:
     * - Log start and end messages.
     * - For each configured plot, build the DTO from configuration and call
     *   `generatePlot` to POST it to the plotting service.
     *
     * @return Unit
     */
    def generate(): Unit = {
      printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.globalConf.startPlotGeneration.format(
        ctsLogs.stationsConf.studyName
      ))

      printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.globalConf.generatingPlot.format(
        ctsLogs.stationsConf.stationCountEvolFromStart
      ), encloseHalfLength = encloseHalfLength)
      generatePlot(buildUrl(ctsExecution.countEvol.uri), ctsExecution.countEvol.body)

      printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.globalConf.generatingPlot.format(
        ctsLogs.stationsConf.stationCountByState2024
      ), encloseHalfLength = encloseHalfLength)
      generatePlot(buildUrl(ctsExecution.countByState2024.uri), ctsExecution.countByState2024.body)

      printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.globalConf.generatingPlot.format(
        ctsLogs.stationsConf.stationCountByAltitude2024
      ), encloseHalfLength = encloseHalfLength)
      generatePlot(buildUrl(ctsExecution.countByAltitude2024.uri), ctsExecution.countByAltitude2024.body)

      printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.globalConf.endPlotGeneration.format(
        ctsLogs.stationsConf.studyName
      ))
    }
  }

  /**
   * Climograph generation helpers and workflow.
   *
   * Reads station JSON metadata and formats climograph DTOs per station
   * and per configured climate group/location. Uses small formatter helpers
   * to adapt the generic DTO templates with station- and climate-specific
   * values.
   */
  private object Climograph {
    /**
     * StationInfo holds basic station metadata used to format plot titles
     * and labels.
     *
     * @param stationName human-readable station name
     * @param stationId station identifier code
     * @param state administrative state name
     * @param latitude latitude as string (DMS or decimal as provided)
     * @param longitude longitude as string (DMS or decimal as provided)
     * @param altitude station altitude as integer meters
     */
    private case class StationInfo(stationName: String, stationId: String, state: String, latitude: String, longitude: String, altitude: Int)

    /**
     * MeteoParamInfo describes a meteorological parameter used in climographs.
     *
     * @param meteoParam parameter long name (e.g. temperature)
     * @param meteoParamAbbrev short abbreviation used in file names
     * @param units units string (e.g. "°C", "mm")
     * @param colAggMethod aggregation method name used in labels
     */
    private case class MeteoParamInfo(meteoParam: String, meteoParamAbbrev: String, units: String, colAggMethod: String)

    /**
     * ClimographInfo groups temperature/precipitation parameter info and the
     * climate group/type metadata used for path formatting.
     *
     * @param tempParamInfo temperature parameter metadata
     * @param precParamInfo precipitation parameter metadata
     * @param climateType climate group name
     * @param climateSubtype climate subtype name
     * @param climateLocation location label used in path formatting
     */
    private case class ClimographInfo(tempParamInfo: MeteoParamInfo, precParamInfo: MeteoParamInfo, climateType: String, climateSubtype: String, climateLocation: String)

    /**
     * FormatInfo aggregates station and climograph metadata used by the
     * DTO formatter.
     *
     * @param stationInfo station metadata
     * @param climographInfo climograph metadata
     */
    private case class FormatInfo(stationInfo: StationInfo, climographInfo: ClimographInfo)

    private val ctsExecution = PlotGenerator.ctsExecution.climographConf
    private val ctsStorage = PlotGenerator.ctsStorage.climographConf
    private val ctsLogs = PlotGenerator.ctsLogs.climographConf

    /**
     * Format a `ClimographDTO` using the provided `FormatInfo`.
     *
     * The function returns a new DTO with paths, axis names and lettering
     * populated from station and climate metadata. It does not perform any
     * IO; it only manipulates the DTO structure.
     *
     * @param dto template DTO read from configuration
     * @param formatInfo metadata used to populate template fields
     * @return formatted `ClimographDTO` ready to be serialized and sent
     */
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

    /**
     * Run the climograph generation workflow.
     *
     * Behavior:
     * - Iterate configured climate groups/subtypes/locations.
     * - Read station metadata from JSON storage.
     * - Format the DTO and POST it to the plotting service via `generatePlot`.
     *
     * @return Unit
     */
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

  /**
   * SingleParamStudies contains formatters and the workflow to generate
   * plots that focus on a single meteorological parameter (top-N, evolutions,
   * heatmaps, etc.).
   */
  private object SingleParamStudies {
    /**
     * MeteoParamInfo describes a parameter used by single-parameter studies.
     *
     * @param meteoParam full parameter name
     * @param meteoParamAbbrev short abbreviation used in file names
     * @param units units string (e.g. "°C", "mm")
     * @param colAggMethod aggregation method used for grouped labels
     */
    private case class MeteoParamInfo(meteoParam: String, meteoParamAbbrev: String, units: String, colAggMethod: String)

    /**
     * Top10TemporalInfo contains temporal metadata for top-10 plots (e.g. "2024").
     *
     * @param value temporal value used in paths
     * @param title human readable title for the temporal slice
     */
    private case class Top10TemporalInfo(value: String, title: String)

    /**
     * Top10FormatInfo aggregates the information required to format a top-10 DTO.
     *
     * @param meteoParamInfo parameter metadata
     * @param order order label (e.g. "highest") used in titles
     * @param temporal temporal metadata
     */
    private case class Top10FormatInfo(meteoParamInfo: MeteoParamInfo, order: String, temporal: Top10TemporalInfo)

    /**
     * Top5IncFormatInfo holds formatting info for top-5 increment plots.
     *
     * @param meteoParamInfo parameter metadata
     * @param order order label for the increment plot
     */
    private case class Top5IncFormatInfo(meteoParamInfo: MeteoParamInfo, order: String)

    /**
     * StationInfo used for evolutions and regression-based plots.
     *
     * @param stationName station human readable name
     * @param stationId station code
     * @param state administrative state name
     * @param stateNoSc state code without special characters for path formatting
     * @param latitude latitude as string (DMS or decimal)
     * @param longitude longitude as string (DMS or decimal)
     * @param altitude altitude in meters
     */
    private case class StationInfo(stationName: String, stationId: String, state: String, stateNoSc: String, latitude: String, longitude: String, altitude: Int)

    /**
     * EvolFormatInfo ties a meteo parameter to a station for evolution plots.
     *
     * @param meteoParamInfo parameter metadata
     * @param stationInfo station metadata
     */
    private case class EvolFormatInfo(meteoParamInfo: MeteoParamInfo, stationInfo: StationInfo)

    /**
     * HeatMap2024FormatInfo contains formatting data for heatmap snapshots.
     *
     * @param meteoParamInfo parameter metadata
     * @param location target location identifier used in path formatting
     */
    private case class HeatMap2024FormatInfo(meteoParamInfo: MeteoParamInfo, location: String)

    private val ctsExecution = PlotGenerator.ctsExecution.singleParamStudiesConf
    private val ctsStorage = PlotGenerator.ctsStorage.singleParamStudiesConf
    private val ctsLogs = PlotGenerator.ctsLogs.singleParamStudiesConf

    /**
     * Format a BarDTO for top-10 plots using Top10FormatInfo.
     *
     * @param dto template BarDTO
     * @param formatInfo formatting metadata
     * @return formatted BarDTO
     */
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

    /**
     * Format a BarDTO for top-5 increment plots.
     *
     * @param dto template BarDTO
     * @param formatInfo formatting metadata for top-5 increments
     * @return formatted BarDTO
     */
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

    /**
     * Format a LinearDTO for evolution plots in 2024 using station metadata.
     *
     * @param dto template LinearDTO
     * @param formatInfo metadata containing parameter and station info
     * @return formatted LinearDTO
     */
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

    /**
     * Format a LinearRegressionDTO for yearly grouped evolution plots.
     *
     * @param dto template LinearRegressionDTO
     * @param formatInfo metadata containing parameter and station info
     * @return formatted LinearRegressionDTO
     */
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

    /**
     * Format a HeatMapDTO for 2024 heatmap generation.
     *
     * @param dto template HeatMapDTO
     * @param formatInfo metadata with parameter and location
     * @return formatted HeatMapDTO
     */
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

    /**
     * Execute the single-parameter studies plotting workflow.
     *
     * Iterates configured parameters and states, reads required JSON
     * metadata, formats DTOs and delegates actual HTTP POSTs to `generatePlot`.
     *
     * @return Unit
     */
    def generate(): Unit = {
      printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.startPlotGeneration.format(
        ctsLogs.studyName
      ))

      ctsSchemaSpark.meteoParamsValues.toList.foreach(studyParamValue => {

        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.meteoParamStudy.format(
          studyParamValue.studyParamName.capitalize
        ), encloseHalfLength = encloseHalfLength + 5)

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

        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.evolStudy.format(
          studyParamValue.studyParamName.capitalize
        ), encloseHalfLength = encloseHalfLength + 10)

        ctsSchemaSpark.stateValues.toList.foreach(stateValue => {

          printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.evolState.format(
            stateValue.stateName.capitalize
          ), encloseHalfLength = encloseHalfLength + 15)

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

  /**
   * InterestingStudies contains workflows that combine multiple parameters
   * (e.g. precipitation and pressure) and top-state tables.
   */
  private object InterestingStudies {
    /**
     * MeteoParamInfo used in interesting studies (precipitation/pressure etc.).
     *
     * @param meteoParam full parameter name
     * @param meteoParamAbbrev short abbreviation used in file names
     * @param units units string (e.g. "°C", "mm")
     * @param colAggMethod aggregation method used for grouped labels
     */
    private case class MeteoParamInfo(meteoParam: String, meteoParamAbbrev: String, units: String, colAggMethod: String)

    /**
     * StationInfo used to build plot titles and labels in interesting studies.
     *
     * @param stationName human-readable station name
     * @param stationId station code
     * @param state administrative state name
     * @param stateNoSc state code without special characters for path formatting
     * @param latitude latitude as string (DMS or decimal)
     * @param longitude longitude as string (DMS or decimal)
     * @param altitude altitude in meters
     */
    private case class StationInfo(stationName: String, stationId: String, state: String, stateNoSc: String, latitude: String, longitude: String, altitude: Int)

    /**
     * PrecPressEvolFormatInfo groups parameter and station info for double-linear plots.
     *
     * @param precMeteoInfo precipitation parameter metadata
     * @param pressMeteoInfo pressure parameter metadata
     * @param stationInfo station metadata used in titles and path formatting
     */
    private case class PrecPressEvolFormatInfo(precMeteoInfo: MeteoParamInfo, pressMeteoInfo: MeteoParamInfo, stationInfo: StationInfo)

    /**
     * Top10StatesFormatInfo contains data to format top-10-by-state tables.
     *
     * @param studyName human readable study name
     * @param studyAbbrev short code used in paths
     */
    private case class Top10StatesFormatInfo(studyName: String, studyAbbrev: String)

    private val ctsExecution = PlotGenerator.ctsExecution.interestingStudiesConf
    private val ctsStorage = PlotGenerator.ctsStorage.interestingStudiesConf
    private val ctsLogs = PlotGenerator.ctsLogs.interestingStudiesConf

    /**
     * Format a DoubleLinearDTO for precipitation/pressure evolution plots.
     *
     * @param dto template DoubleLinearDTO
     * @param formatInfo formatting metadata that includes parameter and station info
     * @return formatted DoubleLinearDTO
     */
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

    /**
     * Format a TableDTO for top-10 states outputs.
     *
     * @param dto template TableDTO
     * @param formatInfo formatting metadata with study name/abbrev
     * @return formatted TableDTO
     */
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

    /**
     * Run the interesting studies plotting workflow.
     *
     * This method reads station JSONs, formats the DTOs and calls
     * `generatePlot` to trigger plot creation for multiple combined-parameter
     * studies and state-level tables.
     *
     * @return Unit
     */
    def generate(): Unit = {
      printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.startPlotGeneration.format(
        ctsLogs.studyName
      ))

      printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.evolPrecPressStudy, encloseHalfLength = encloseHalfLength + 5)

      ctsSchemaSpark.stateValues.toList.foreach(stateValue => {

        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.evolPrecPressState.format(
          stateValue.stateName.capitalize
        ), encloseHalfLength = encloseHalfLength + 10)

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

  /**
   * Serialize the provided DTO to JSON while removing keys with null values.
   *
   * The function is generic and requires an upickle ReadWriter in scope.
   *
   * @tparam T DTO type
   * @param dto data transfer object to serialize
   * @return low-level ujson.Value ready to be sent to the HTTP client
   */
  private def generateRequestBody[T: ReadWriter](dto: T): Value = {
    removeNullKeys(writeJs(dto))
  }

  /**
   * Send a POST request to the plotting service with the generated DTO body.
   *
   * On success it logs the destination path returned by the plotting
   * backend; on failure it logs a warning with the exception message.
   *
   * @param uri HTTP endpoint to post the DTO
   * @param dto DTO value (will be serialized inside)
   * @return Unit
   */
  private def generatePlot[T: ReadWriter](uri: Uri, dto: T): Unit = {
    sendPostRequest(uri, generateRequestBody(dto)) match {
      case Left(exception: Exception) => ConsoleLogUtils.Message.printlnConsoleMessage(NotificationType.Warning, exception.toString)
      case Right(response) => ConsoleLogUtils.Message.printlnConsoleMessage(
        NotificationType.Information,
        ctsLogs.globalConf.dataSaved.format(ujson.read(response.body)(ctsSchemaAutoPlot.response.destPath))
      )
    }
  }

  /**
   * Entry point to generate all configured plots.
   *
   * Behavior:
   * - Starts an internal chronometer.
   * - Executes in sequence: Stations, Climograph, SingleParamStudies,
   *   InterestingStudies.
   * - Logs elapsed time and waits a configured pause before returning.
   *
   * @return Unit
   */
  def generate(): Unit = {
    chronometer.start()

    Stations.generate()
    Climograph.generate()
    SingleParamStudies.generate()
    InterestingStudies.generate()

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalUtils.chrono.chronoResult.format(chronometer.stop()))
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalUtils.betweenStages.infoText.format(ctsGlobalUtils.betweenStages.millisBetweenStages / 1000))
    Thread.sleep(ctsGlobalUtils.betweenStages.millisBetweenStages)
  }
}


