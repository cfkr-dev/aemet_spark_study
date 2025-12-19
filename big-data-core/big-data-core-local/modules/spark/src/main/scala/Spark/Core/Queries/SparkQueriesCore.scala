package Spark.Core.Queries

import Spark.Config.{GlobalConf, SparkConf}
import Spark.Core.Session.SparkSessionCore
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}

case class SparkQueriesCore(sparkSessionCore: SparkSessionCore) {
  import sparkSessionCore.sparkSession.implicits._

  private val ctsSchemaAemetAllStation = GlobalConf.Constants.schema.aemetConf.allStationInfo
  private val ctsSchemaAemetAllMeteoInfo = GlobalConf.Constants.schema.aemetConf.allMeteoInfo
  private val ctsSchemaSparkAllStation = GlobalConf.Constants.schema.sparkConf.stationsDf
  private val ctsSpecialColumns = GlobalConf.Constants.schema.sparkConf.specialColumns
  private val ctsGroupMethods = GlobalConf.Constants.schema.sparkConf.groupMethods
  private val ctsExecutionDataframeConf = SparkConf.Constants.init.execution.dataframeConf

  /**
   * Retrieve station information by station identifier.
   *
   * @param stationId station code to search in the stations DataFrame
   * @return Right(DataFrame) with a single-row DataFrame containing basic station info, or Left(Exception)
   */
  def getStationInfoById(
    stationId: String
  )
  : Either[Exception, DataFrame] = {
    try {
      val allStationsDf: DataFrame = sparkSessionCore.sessionDataframes.allStations.as(ctsExecutionDataframeConf.allStationsDf.aliasName)

      Right(
        allStationsDf
          .filter(col(ctsSchemaAemetAllStation.indicativo) === stationId)
          .select(
            col(ctsSchemaAemetAllStation.indicativo).alias(ctsSchemaSparkAllStation.stationId),
            col(ctsSchemaAemetAllStation.nombre).alias(ctsSchemaSparkAllStation.stationName),
            col(ctsSchemaAemetAllStation.provincia).alias(ctsSchemaSparkAllStation.state),
            col(ctsSchemaAemetAllStation.latitud).alias(ctsSchemaSparkAllStation.latDms),
            col(ctsSchemaAemetAllStation.longitud).alias(ctsSchemaSparkAllStation.longDms),
            col(ctsSchemaAemetAllStation.altitud).alias(ctsSchemaSparkAllStation.altitude)
          )
      )
    } catch {
      case exception: Exception => Left(exception)
    }
  }

  /**
   * Count distinct stations grouped by a column over a time lapse.
   *
   * @param column tuple containing a Column expression and the output column name
   * @param startDate start of the interval (YYYY-MM-DD or year for yearly queries)
   * @param endDate optional end date (if omitted a single-year aggregation is applied)
   * @return Right(DataFrame) with counts or Left(Exception) on failure
   */
  def getStationCountByColumnInLapse(
    column: (Column, String),
    startDate: String,
    endDate: Option[String] = None,
  ): Either[Exception, DataFrame] = {
    try {
      val meteoDf: DataFrame = sparkSessionCore.sessionDataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName)

      Right(
        meteoDf.filter(
            endDate match {
              case Some(end) => col(ctsSchemaAemetAllMeteoInfo.fecha).between(lit(startDate), lit(end))
              case None => year(col(ctsSchemaAemetAllMeteoInfo.fecha)) === startDate.toInt
            }
          ).groupBy(column._1.as(column._2))
          .agg(countDistinct(col(ctsSchemaAemetAllMeteoInfo.indicativo)).as(ctsSpecialColumns.count))
          .select(
            col(column._2),
            col(ctsSpecialColumns.count)
          )
          .orderBy(column._2)
      )
    } catch {
      case exception: Exception => Left(exception)
    }
  }

  /**
   * Count stations falling into parameter intervals for a given lapse.
   *
   * For each interval the function computes min/max bounds if they are
   * specified as infinities in configuration, then counts stations whose
   * parameter lies in the interval.
   *
   * @param paramIntervals list of (columnName, min, max)
   * @param startDate start date or year
   * @param endDate optional end date
   * @return Right(DataFrame) with rows per interval or Left(Exception)
   */
  def getStationsCountByParamIntervalsInALapse(
    paramIntervals: List[(String, Double, Double)],
    startDate: String,
    endDate: Option[String] = None
  ): Either[Exception, DataFrame] = {
    try {
      val meteoDf: DataFrame = sparkSessionCore.sessionDataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName)

      val param = col(paramIntervals.head._1)

      val filteredDf = meteoDf.filter(
        endDate match {
          case Some(end) => col(ctsSchemaAemetAllMeteoInfo.fecha).between(lit(startDate), lit(end))
          case None => year(col(ctsSchemaAemetAllMeteoInfo.fecha)) === startDate.toInt
        }
      ).select(
        col(ctsSchemaAemetAllMeteoInfo.indicativo),
        col(paramIntervals.head._1).cast(DoubleType)
      )

      Right(
        paramIntervals.zipWithIndex.map {case ((_param, _min, _max), idx) =>
          val actualMin = if (_min.isNegInfinity) filteredDf.agg(min(_param)).collect()(0)(0) else _min
          val actualMax = if (_max.isPosInfinity) filteredDf.agg(max(_param)).collect()(0)(0) else _max

          filteredDf.filter(
            if (idx == 0)
              param >= actualMin && param <= actualMax
            else
              param > actualMin && param <= actualMax
          ).agg(
            lit(actualMin).as(ctsSpecialColumns.minValue),
            lit(actualMax).as(ctsSpecialColumns.maxValue),
            countDistinct(ctsSchemaAemetAllMeteoInfo.indicativo).as(ctsSpecialColumns.count)
          )
        }.reduce(_.union(_)).orderBy(ctsSpecialColumns.minValue)
      )
    } catch {
      case exception: Exception => Left(exception)
    }
  }

  /**
   * Compute monthly averages for temperature and monthly sum of precipitation
   * for a station in a given year.
   *
   * @param stationId station code
   * @param studyParams tuple of (temperatureColumnName, precipitationColumnName)
   * @param observationYear year to observe
   * @return Right(DataFrame) with month, avg(temp) and sum(precip) columns
   */
  def getStationMonthlyAvgTempAndSumPrecInAYear(
    stationId: String,
    studyParams: (String, String),
    observationYear: Int
  ): Either[Exception, DataFrame] = {
    try {
      val meteoDf: DataFrame = sparkSessionCore.sessionDataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName)

      Right(
        meteoDf
          .filter(
            col(ctsSchemaAemetAllMeteoInfo.tMed).isNotNull &&
              col(ctsSchemaAemetAllMeteoInfo.prec).isNotNull &&
              col(ctsSchemaAemetAllMeteoInfo.indicativo) === stationId &&
              year(col(ctsSchemaAemetAllMeteoInfo.fecha)) === observationYear
          ).groupBy(
            month(col(ctsSchemaAemetAllMeteoInfo.fecha)).as(ctsSpecialColumns.month)
          ).agg(
            round(avg(col(ctsSchemaAemetAllMeteoInfo.tMed)), 1).as(ctsSpecialColumns.colMonthlyGrouped.format(studyParams._1, ctsGroupMethods.avg)),
            round(sum(col(ctsSchemaAemetAllMeteoInfo.prec)), 1).as(ctsSpecialColumns.colMonthlyGrouped.format(studyParams._2, ctsGroupMethods.sum))
          ).orderBy(col(ctsSpecialColumns.month))
      )
    } catch {
      case exception: Exception => Left(exception)
    }
  }

  /**
   * Compute top-N stations for a climate parameter within a given lapse.
   *
   * @param climateParam column name to aggregate
   * @param aggMethodName aggregation method (avg/sum/min/max)
   * @param paramNameToShow short name to use in result column
   * @param startDate start of lapse (or year)
   * @param endDate optional end date
   * @param topN number of rows to return
   * @param highest whether to return the highest (true) or lowest (false)
   * @return Right(DataFrame) with top-N stations or Left(Exception)
   */
  def getTopNClimateParamInALapse(
    climateParam: String,
    aggMethodName: String,
    paramNameToShow: String,
    startDate: String,
    endDate: Option[String] = None,
    topN: Int = 10,
    highest: Boolean = true
  ): Either[Exception, DataFrame] = {
    try {
      val meteoDf: DataFrame = sparkSessionCore.sessionDataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName)
      val stationDf: DataFrame = sparkSessionCore.sessionDataframes.allStations.as(ctsExecutionDataframeConf.allStationsDf.aliasName)

      val aggMethod: Column => Column = aggMethodName.toLowerCase match {
        case ctsGroupMethods.avg => avg
        case ctsGroupMethods.sum => sum
        case ctsGroupMethods.min => min
        case ctsGroupMethods.max => max
        case other => throw new IllegalArgumentException(other)
      }

      Right(
        meteoDf.filter(endDate match {
            case Some(endDate) => col(ctsSchemaAemetAllMeteoInfo.fecha).between(lit(startDate), lit(endDate))
            case None => year(col(ctsSchemaAemetAllMeteoInfo.fecha)) === startDate.toInt
          }).filter(col(climateParam).isNotNull)
          .groupBy(col(ctsSchemaAemetAllMeteoInfo.indicativo))
          .agg(aggMethod(col(climateParam)).as(ctsSpecialColumns.colGrouped.format(climateParam, aggMethodName)))
          .join(stationDf, Seq(ctsSchemaAemetAllStation.indicativo), "inner")
          .select(
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.indicativo
            )).alias(ctsSchemaSparkAllStation.stationId),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.nombre
            )).alias(ctsSchemaSparkAllStation.stationName),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.provincia
            )).alias(ctsSchemaSparkAllStation.state),
            round(col(ctsSpecialColumns.colGrouped.format(
              climateParam, aggMethodName
            )), 1).alias(ctsSpecialColumns.colGrouped.format(paramNameToShow, aggMethodName)),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.latitud
            )).alias(ctsSchemaSparkAllStation.latDms),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.longitud
            )).alias(ctsSchemaSparkAllStation.longDms),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.altitud
            )).alias(ctsSchemaSparkAllStation.altitude),
          ).orderBy(
            if (highest)
              col(ctsSpecialColumns.colGrouped.format(paramNameToShow, aggMethodName)).desc
            else
              col(ctsSpecialColumns.colGrouped.format(paramNameToShow, aggMethodName)).asc
          )
          .limit(topN)
          .withColumn(ctsSpecialColumns.top, monotonically_increasing_id() + 1)
      )
    } catch {
      case exception: Exception => Left(exception)
    }
  }

  /**
   * Compute top-N stations (or states) matching a set of climate parameter
   * conditions within a lapse.
   *
   * @param climateParams list of (paramName, min, max) conditions
   * @param startDate start of lapse
   * @param endDate optional end date
   * @param groupByState whether the aggregation is by state or by station
   * @param topN max rows to return
   * @param highest whether to return highest or lowest results
   * @return Right(DataFrame) with top-N results, Left(Exception) on failure
   */
  def getTopNClimateConditionsInALapse(
    climateParams: List[(String, Double, Double)],
    startDate: String,
    endDate: Option[String] = None,
    groupByState: Boolean = false,
    topN: Int = 10,
    highest: Boolean = true
  ): Either[Exception, DataFrame] = {
    try {
      val meteoDf: DataFrame = sparkSessionCore.sessionDataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName)
      val stationDf: DataFrame = sparkSessionCore.sessionDataframes.allStations.as(ctsExecutionDataframeConf.allStationsDf.aliasName)

      val filteredDf = meteoDf.filter(endDate match {
          case Some(end) => col(ctsSchemaAemetAllMeteoInfo.fecha).between(lit(startDate), lit(end))
          case None => year(col(ctsSchemaAemetAllMeteoInfo.fecha)) === startDate.toInt
        })
        .filter(climateParams.map { case (paramName, minValue, maxValue) =>
          col(paramName).isNotNull && col(paramName).between(minValue, maxValue)
        }.reduce(_ && _))

      Right(
        (if (groupByState) {
          filteredDf
            .groupBy(ctsSchemaAemetAllMeteoInfo.provincia)
            .agg(count(ctsSchemaAemetAllMeteoInfo.fecha).alias(ctsSpecialColumns.daysWithConds))
            .orderBy(
              if (highest)
                col(ctsSpecialColumns.daysWithConds).desc
              else
                col(ctsSpecialColumns.daysWithConds).asc
            )
            .withColumn(ctsSpecialColumns.top, monotonically_increasing_id() + 1)
            .select(
              col(ctsSchemaAemetAllMeteoInfo.provincia).alias(ctsSchemaSparkAllStation.state),
              col(ctsSpecialColumns.top)
            )
        } else {
          filteredDf
            .groupBy(ctsSchemaAemetAllMeteoInfo.indicativo)
            .agg(countDistinct(ctsSchemaAemetAllMeteoInfo.fecha).alias(ctsSpecialColumns.daysWithConds))
            .join(stationDf, Seq(ctsSchemaAemetAllStation.indicativo), "inner")
            .orderBy(
              if (highest)
                col(ctsSpecialColumns.daysWithConds).desc
              else
                col(ctsSpecialColumns.daysWithConds).asc
            )
            .withColumn(ctsSpecialColumns.top, monotonically_increasing_id() + 1)
            .select(
              col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
                ctsSchemaAemetAllStation.indicativo
              )).alias(ctsSchemaSparkAllStation.stationId),
              col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
                ctsSchemaAemetAllStation.nombre
              )).alias(ctsSchemaSparkAllStation.stationName),
              col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
                ctsSchemaAemetAllStation.provincia
              )).alias(ctsSchemaSparkAllStation.state),
              col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
                ctsSchemaAemetAllStation.latitud
              )).alias(ctsSchemaSparkAllStation.latDms),
              col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
                ctsSchemaAemetAllStation.longitud
              )).alias(ctsSchemaSparkAllStation.longDms),
              col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
                ctsSchemaAemetAllStation.altitud
              )).alias(ctsSchemaSparkAllStation.altitude),
              col(ctsSpecialColumns.top)
            )
        })
          .orderBy(col(ctsSpecialColumns.top).asc)
          .limit(topN)
      )
    } catch {
      case exception: Exception => Left(exception)
    }
  }

  /**
   * Retrieve daily climate parameters for a station in a lapse and return
   * a DataFrame with dates and the requested parameters aggregated per day.
   *
   * @param stationId station code
   * @param climateParams sequence of (columnName, displayName)
   * @param aggMethodNames aggregation method names for each param
   * @param startDate start of lapse
   * @param endDate optional end date
   * @return Right(DataFrame) with daily series or Left(Exception)
   */
  def getClimateParamInALapseById(
    stationId: String,
    climateParams: Seq[(String, String)],
    aggMethodNames: Seq[String],
    startDate: String,
    endDate: Option[String] = None
  ): Either[Exception, DataFrame] = {
    try {
      require(climateParams.length == aggMethodNames.length)

      val meteoDf: DataFrame = sparkSessionCore.sessionDataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName)

      Right(
        climateParams.foldLeft(
            meteoDf.filter(endDate match {
              case Some(endDate) => col(ctsSchemaAemetAllMeteoInfo.fecha).between(lit(startDate), lit(endDate))
              case None => year(col(ctsSchemaAemetAllMeteoInfo.fecha)) === startDate.toInt
            })
          ) { (acc, climateParam) =>
            acc.filter(col(climateParam._1).isNotNull)
          }
          .filter(col(ctsSchemaAemetAllMeteoInfo.indicativo) === stationId)
          .select(
            Seq(col(ctsSchemaAemetAllMeteoInfo.fecha).alias(ctsSpecialColumns.date)) ++
              climateParams.zip(aggMethodNames).map(param => round(col(param._1._1), 1).alias(
                ctsSpecialColumns.colDailyGrouped.format(param._1._2, param._2)
              )): _*
          )
          .orderBy(ctsSpecialColumns.date)
      )
    } catch {
      case exception: Exception => Left(exception)
    }
  }

  /**
   * Aggregate yearly groups for a station according to provided parameters
   * and aggregation methods.
   *
   * @param stationId station code
   * @param climateParams sequence of (colName, displayName)
   * @param aggMethodNames aggregation methods corresponding to each param
   * @param startDate start year or date
   * @param endDate optional end year or date
   * @return Right(DataFrame) grouped by year or Left(Exception)
   */
  def getClimateYearlyGroupById(
    stationId: String,
    climateParams: Seq[(String, String)],
    aggMethodNames: Seq[String],
    startDate: String,
    endDate: Option[String] = None
  ): Either[Exception, DataFrame] = {
    try {
      require(climateParams.length == aggMethodNames.length)

      val meteoDf = sparkSessionCore.sessionDataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName)

      val filteredDf = climateParams.foldLeft(
        meteoDf
          .filter(endDate match {
            case Some(end) => col(ctsSchemaAemetAllMeteoInfo.fecha).between(lit(startDate), lit(end))
            case None => year(col(ctsSchemaAemetAllMeteoInfo.fecha)) === startDate.toInt
          })
          .filter(col(ctsSchemaAemetAllMeteoInfo.indicativo) === stationId)
          .withColumn(ctsSpecialColumns.year, year(col(ctsSchemaAemetAllMeteoInfo.fecha)))
      ) { (acc, climateParam) =>
        acc.filter(col(climateParam._1).isNotNull)
      }

      val aggColumns = climateParams.zip(aggMethodNames).map {
        case ((colName, paramName), op) =>
          val baseCol = col(colName)
          val aggCol = op.toLowerCase match {
            case ctsGroupMethods.avg => round(avg(baseCol), 1)
            case ctsGroupMethods.sum => round(sum(baseCol), 1)
            case ctsGroupMethods.min => round(min(baseCol), 1)
            case ctsGroupMethods.max => round(max(baseCol), 1)
            case other => throw new IllegalArgumentException(other)
          }
          aggCol.alias(ctsSpecialColumns.colYearlyGrouped.format(paramName, op))
      }

      val resultDf = filteredDf
        .groupBy(ctsSpecialColumns.year)
        .agg(aggColumns.head, aggColumns.tail: _*)
        .orderBy(ctsSpecialColumns.year)

      Right(resultDf)

    } catch {
      case ex: Exception => Left(ex)
    }
  }

  /**
   * Compute averages for all stations filtered by optional set of states.
   *
   * @param climateParam column to aggregate
   * @param aggMethodName aggregation method (avg/sum/min/max)
   * @param paramNameToShow display name used in result columns
   * @param startDate start of lapse
   * @param endDate optional end date
   * @param states optional list of state names to filter
   * @return Right(DataFrame) with aggregated station averages or Left(Exception)
   */
  def getAllStationsByStatesAvgClimateParamInALapse(
    climateParam: String,
    aggMethodName: String,
    paramNameToShow: String,
    startDate: String,
    endDate: Option[String] = None,
    states: Option[Seq[String]] = None
  ): Either[Exception, DataFrame] = {
    try {
      val meteoDf: DataFrame = sparkSessionCore.sessionDataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName)
      val stationDf: DataFrame = sparkSessionCore.sessionDataframes.allStations.as(ctsExecutionDataframeConf.allStationsDf.aliasName)

      val aggMethod: Column => Column = aggMethodName.toLowerCase match {
        case ctsGroupMethods.avg => avg
        case ctsGroupMethods.sum => sum
        case ctsGroupMethods.min => min
        case ctsGroupMethods.max => max
        case other => throw new IllegalArgumentException(other)
      }

      Right(
        meteoDf.filter(endDate match {
            case Some(end) => col(ctsSchemaAemetAllMeteoInfo.fecha).between(lit(startDate), lit(end))
            case None => year(col(ctsSchemaAemetAllMeteoInfo.fecha)) === startDate.toInt
          }).filter(states match {
            case Some(stateList) => col(ctsSchemaAemetAllMeteoInfo.provincia).isin(stateList: _*)
            case None => lit(true)
          }).filter(col(climateParam).isNotNull)
          .groupBy(col(ctsSchemaAemetAllMeteoInfo.indicativo))
          .agg(aggMethod(col(climateParam)).as(ctsSpecialColumns.colGrouped.format(climateParam, aggMethodName)))
          .join(stationDf, Seq(ctsSchemaAemetAllStation.indicativo), "inner")
          .select(
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.indicativo
            )).alias(ctsSchemaSparkAllStation.stationId),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.nombre
            )).alias(ctsSchemaSparkAllStation.stationName),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.provincia
            )).alias(ctsSchemaSparkAllStation.state),
            round(
              col(ctsSpecialColumns.colGrouped.format(climateParam, aggMethodName)), 1
            ).alias(ctsSpecialColumns.colGrouped.format(paramNameToShow, aggMethodName)),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.latitud
            )).alias(ctsSchemaSparkAllStation.latDms),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.longitud
            )).alias(ctsSchemaSparkAllStation.longDms),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaSparkAllStation.latDec
            )).alias(ctsSchemaSparkAllStation.latDec),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaSparkAllStation.longDec
            )).alias(ctsSchemaSparkAllStation.longDec),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.altitud
            )).alias(ctsSchemaSparkAllStation.altitude),
          )
      )
    } catch {
      case exception: Exception => Left(exception)
    }
  }

  /**
   * Compute a linear regression (beta1, beta0) for a climate parameter for
   * a station over a year range and return the model parameters as a small
   * DataFrame.
   *
   * @param stationId station code
   * @param climateParam parameter column name
   * @param aggMethodName aggregation method for yearly grouping
   * @param startYear start year
   * @param endYear optional end year
   * @return Right(DataFrame(stationId, beta1, beta0)) or Left(Exception)
   */
  def getStationClimateParamRegressionModelInALapse(
    stationId: String,
    climateParam: String,
    aggMethodName: String,
    startYear: String,
    endYear: Option[String] = None
  ): Either[Exception, DataFrame] = {
    try {
      val meteoDf: DataFrame = sparkSessionCore.sessionDataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName)

      val aggMethod: Column => Column = aggMethodName.toLowerCase match {
        case ctsGroupMethods.avg => avg
        case ctsGroupMethods.sum => sum
        case ctsGroupMethods.min => min
        case ctsGroupMethods.max => max
        case other => throw new IllegalArgumentException(other)
      }

      val filteredDF = meteoDf
        .filter(col(ctsSchemaAemetAllMeteoInfo.indicativo) === stationId)
        .filter(endYear match {
          case Some(end) => col(ctsSchemaAemetAllMeteoInfo.fecha).between(lit(startYear), lit(end))
          case None => year(col(ctsSchemaAemetAllMeteoInfo.fecha)) === startYear
        })
        .filter(col(climateParam).isNotNull)
        .withColumn(ctsSpecialColumns.year, year(col(ctsSchemaAemetAllMeteoInfo.fecha)))
        .groupBy(ctsSpecialColumns.year)
        .agg(aggMethod(col(climateParam)).as(ctsSpecialColumns.climateParamGrouped.format(ctsGroupMethods.avg)))
        .select(
          col(ctsSpecialColumns.year).alias(ctsSpecialColumns.x),
          col(ctsSpecialColumns.climateParamGrouped.format(ctsGroupMethods.avg)).alias(ctsSpecialColumns.y)
        )

      val (meanX, meanY) = filteredDF.agg(
        avg(ctsSpecialColumns.x),
        avg(ctsSpecialColumns.y)
      ).as[(Double, Double)].first() match {
        case (mx, my) => (mx, my)
      }

      val (beta1, beta0) = filteredDF.withColumn(
          ctsSpecialColumns.xDiff, col(ctsSpecialColumns.x) - meanX
        )
        .withColumn(
          ctsSpecialColumns.yDiff, col(ctsSpecialColumns.y) - meanY
        )
        .agg(
          sum(
            col(ctsSpecialColumns.xDiff) * col(ctsSpecialColumns.yDiff)
          ).as(ctsSpecialColumns.num),
          sum(
            col(ctsSpecialColumns.xDiff) * col(ctsSpecialColumns.xDiff)
          ).as(ctsSpecialColumns.den)
        ).as[(Double, Double)].first() match {
        case (num, den) =>
          val b1 = num / den
          val b0 = meanY - b1 * meanX
          (b1, b0)
      }

      Right(Seq((stationId, beta1, beta0)).toDF(
        ctsSchemaSparkAllStation.stationId,
        ctsSpecialColumns.beta1,
        ctsSpecialColumns.beta0
      ))
    } catch {
      case exception: Exception => Left(exception)
    }
  }

  /**
   * Compute the top-N stations with the highest (or lowest) increment
   * of a climate parameter between two years using precomputed regression models.
   *
   * @param stationIds list of station codes to consider
   * @param regressionModels DataFrame containing regression model params
   * @param climateParam climate parameter column
   * @param paramNameToShow display name used in outputs
   * @param aggMethodName aggregation method used for yearly groupings
   * @param startYear start year for increment calculation
   * @param endYear end year for increment calculation
   * @param highest true to return highest increments, false for lowest
   * @param topN number of rows to return
   * @return Right(DataFrame) with top-N stations and increment metrics or Left(Exception)
   */
  def getTopNClimateParamIncrementInAYearLapse(
    stationIds: Seq[String],
    regressionModels: DataFrame,
    climateParam: String,
    paramNameToShow: String,
    aggMethodName: String,
    startYear: Int,
    endYear: Int,
    highest: Boolean = true,
    topN: Int = 5
  ): Either[Exception, DataFrame] = {
    try {
      val meteoDf: DataFrame = sparkSessionCore.sessionDataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName)
      val stationDf: DataFrame = sparkSessionCore.sessionDataframes.allStations.as(ctsExecutionDataframeConf.allStationsDf.aliasName)

      val aggMethod: Column => Column = aggMethodName.toLowerCase match {
        case ctsGroupMethods.avg => avg
        case ctsGroupMethods.sum => sum
        case ctsGroupMethods.min => min
        case ctsGroupMethods.max => max
        case other => throw new IllegalArgumentException(other)
      }

      Right(
        regressionModels
          .filter(col(ctsSchemaSparkAllStation.stationId).isin(stationIds: _*))
          .withColumn(
            ctsSpecialColumns.inc,
            (col(ctsSpecialColumns.beta1) * lit(endYear) + col(ctsSpecialColumns.beta0)) - (col(ctsSpecialColumns.beta1) * lit(startYear) + col(ctsSpecialColumns.beta0))
          )
          .select(col(ctsSchemaSparkAllStation.stationId).as(ctsSchemaAemetAllMeteoInfo.indicativo), col(ctsSpecialColumns.inc))
          .join(
            meteoDf
              .filter(col(ctsSchemaAemetAllMeteoInfo.indicativo).isin(stationIds: _*))
              .filter(col(climateParam).isNotNull)
              .withColumn(ctsSpecialColumns.year, year(col(ctsSchemaAemetAllMeteoInfo.fecha)))
              .filter(col(ctsSpecialColumns.year).between(startYear, endYear))
              .groupBy(ctsSchemaAemetAllMeteoInfo.indicativo, ctsSpecialColumns.year)
              .agg(aggMethod(col(climateParam)).as(ctsSpecialColumns.colYearlyGrouped.format(climateParam, aggMethodName)))
              .groupBy(ctsSchemaAemetAllMeteoInfo.indicativo)
              .agg(avg(col(ctsSpecialColumns.colYearlyGrouped.format(climateParam, aggMethodName))).as(ctsSpecialColumns.globalColYearlyAvg.format(climateParam))),
            Seq(ctsSchemaAemetAllMeteoInfo.indicativo),
            "inner"
          )
          .join(stationDf, Seq(ctsSchemaAemetAllStation.indicativo), "inner")
          .withColumn(ctsSpecialColumns.incPerc, col(ctsSpecialColumns.inc) / col(ctsSpecialColumns.globalColYearlyAvg.format(climateParam, aggMethodName)) * 100)
          .select(
            round(col(ctsSpecialColumns.inc), 1).alias(ctsSpecialColumns.inc),
            round(col(ctsSpecialColumns.incPerc), 1).alias(ctsSpecialColumns.incPerc),
            round(col(ctsSpecialColumns.globalColYearlyAvg.format(climateParam, aggMethodName)), 1).alias(ctsSpecialColumns.globalColYearlyAvg.format(paramNameToShow, aggMethodName)),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.indicativo
            )).alias(ctsSchemaSparkAllStation.stationId),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.nombre
            )).alias(ctsSchemaSparkAllStation.stationName),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.provincia
            )).alias(ctsSchemaSparkAllStation.state),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.latitud
            )).alias(ctsSchemaSparkAllStation.latDms),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.longitud
            )).alias(ctsSchemaSparkAllStation.longDms),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.altitud
            )).alias(ctsSchemaSparkAllStation.altitude),
          )
          .orderBy(
            if (highest)
              col(ctsSpecialColumns.inc).desc
            else
              col(ctsSpecialColumns.inc).asc
          )
          .limit(topN)
          .withColumn(ctsSpecialColumns.top, monotonically_increasing_id() + 1)
      )
    } catch {
      case exception: Exception => Left(exception)
    }
  }

}
