package Core

//import InitApp.ec

//import Config.ConstantsV2.Spark.Queries.Temperature.tempEvolFromStartForEachState.stationRegistries
import Core.DataExtraction.Aemet.AemetAPIClient
import Core.DataExtraction.Ifapa.IfapaAPIClient
import Core.DataExtraction.Ifapa.IfapaToAemetConverter
import Core.Spark.SparkManager.SparkQueries
import org.apache.spark.sql.{DataFrame, SparkSession}

object InitApp extends App {

  //AemetAPIClient.aemetDataExtraction()
  //IfapaAPIClient.ifapaDataExtraction()
//  IfapaToAemetConverter.ifapaToAemetConversion()

//  SparkQueries.Aemet.stationMonthlyAvgTempAndPrecInAYear("7002Y", 2024)
//  SparkQueries.Aemet.stationInfo("7002Y")
//  SparkQueries.Aemet.stationMonthlyAvgTempAndPrecInAYear("C249I", 2024)
//  SparkQueries.Aemet.stationInfo("C249I")
//  SparkQueries.Aemet.stationMonthlyAvgTempAndPrecInAYear("2462", 2024)
//  SparkQueries.Aemet.stationInfo("2462")
//  SparkQueries.Aemet.stationMonthlyAvgTempAndPrecInAYear("9839V", 2024)
//  SparkQueries.Aemet.stationInfo("9839V")
//  SparkQueries.Aemet.stationMonthlyAvgTempAndPrecInAYear("4", 2024)
//  SparkQueries.Aemet.stationInfo("4")

  //SparkQueries.Climograph.execute()

  //SparkQueries.test("2024", topN = 10, mayores = false).show()
  //SparkQueries.test("2024", topN = 10, mayores = true).show()
  //SparkQueries.test("1973-01-01", Some("2024-12-31"), topN = 5, mayores = true).show()
  //SparkQueries.test("1973-01-01", Some("2024-12-31"), topN = 5, mayores = false).show()
  //SparkQueries.extraerTemperaturaDiaria("9677", 2024).show(365)
//  SparkQueries.getTopNClimateParamInALapse("tmed", "2024", endDate = None, topN = 10) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("tmed", "2024", endDate = None, topN = 10, highest = false) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("prec", "2024", endDate = None, topN = 10) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("prec", "2024", endDate = None, topN = 10, highest = false) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("velmedia", "2024", endDate = None, topN = 10) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("velmedia", "2024", endDate = None, topN = 10, highest = false) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("presmax", "2024", endDate = None, topN = 10) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("presmax", "2024", endDate = None, topN = 10, highest = false) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("sol", "2024", endDate = None, topN = 10) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("sol", "2024", endDate = None, topN = 10, highest = false) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("hrmedia", "2024", endDate = None, topN = 10) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("hrmedia", "2024", endDate = None, topN = 10, highest = false) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//
//
//  SparkQueries.getTopNClimateParamInALapse("tmed", "2000-01-01", Some("2024-12-31"), topN = 10) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("tmed", "2000-01-01", Some("2024-12-31"), topN = 10, highest = false) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("prec", "2000-01-01", Some("2024-12-31"), topN = 10) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("prec", "2000-01-01", Some("2024-12-31"), topN = 10, highest = false) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("velmedia", "2000-01-01", Some("2024-12-31"), topN = 10) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("velmedia", "2000-01-01", Some("2024-12-31"), topN = 10, highest = false) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("presmax", "2000-01-01", Some("2024-12-31"), topN = 10) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("presmax", "2000-01-01", Some("2024-12-31"), topN = 10, highest = false) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("sol", "2000-01-01", Some("2024-12-31"), topN = 10) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("sol", "2000-01-01", Some("2024-12-31"), topN = 10, highest = false) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("hrmedia", "2000-01-01", Some("2024-12-31"), topN = 10) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("hrmedia", "2000-01-01", Some("2024-12-31"), topN = 10, highest = false) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//
//
//  SparkQueries.getTopNClimateParamInALapse("tmed", "1973-01-01", Some("2024-12-31"), topN = 10) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("tmed", "1973-01-01", Some("2024-12-31"), topN = 10, highest = false) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("prec", "1973-01-01", Some("2024-12-31"), topN = 10) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("prec", "1973-01-01", Some("2024-12-31"), topN = 10, highest = false) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("velmedia", "1973-01-01", Some("2024-12-31"), topN = 10) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("velmedia", "1973-01-01", Some("2024-12-31"), topN = 10, highest = false) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("presmax", "1973-01-01", Some("2024-12-31"), topN = 10) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("presmax", "1973-01-01", Some("2024-12-31"), topN = 10, highest = false) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("sol", "1973-01-01", Some("2024-12-31"), topN = 10) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("sol", "1973-01-01", Some("2024-12-31"), topN = 10, highest = false) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("hrmedia", "1973-01-01", Some("2024-12-31"), topN = 10) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }
//  SparkQueries.getTopNClimateParamInALapse("hrmedia", "1973-01-01", Some("2024-12-31"), topN = 10, highest = false) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }



//  SparkQueries.getAllStationsInfoByAvgClimateParamInALapse("tmed", "2024") match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }

  //SparkQueries.test().show(999, truncate = false)

  //SparkQueries.test().show(999, truncate = false)


  // Uso de la función para obtener los períodos de operatividad para una provincia específica
//  val provincia = "A CORUÑA" // Cambia la provincia que quieras
//  val result = SparkQueries.getOperativePeriodsForProvince(provincia)
//
//  // Mostrar el resultado
//  result.show(999, truncate = false)

//  // Llamada a la función para obtener todas las estaciones operativas entre 1973-01-01 y 2024-12-31
//  val operativeStations = SparkQueries.getStationsOperativeBetweenDatesForAllProvincesWithYearCut("1973-01-01", "2024-12-31")
//  // Mostrar el resultado
//  operativeStations.show(999, truncate = false)

  //val estacionesMasLongevas = SparkQueries.getLongestOperativeStationsPerProvince("tmed", 3)
  //estacionesMasLongevas.show(999, truncate = false)

//  SparkQueries.getClimateParamInALapseById("1387","tmed", "1973-01-01", Some("2024-12-31")) match {
//    case Right(value) => value.show()
//    case Left(_) => ()
//  }

  //SparkQueries.test()

  //SparkQueries.trainLinearModelForProvincesAndRank()
  //SparkQueries.trainAndRankTemperatureModels()
  //val indicativos = stationRegistries.map(e => e.stationId) // cambia esto por los indicativos que quieras
  //SparkQueries.getTopNClimateParamIncrementInAYearLapse(indicativos, "prec", 1973, 2024, highest = false).show()

//  SparkQueries.getAllStationsByStatesAvgClimateParamInALapse("tmed", "2024", None, Some(List("SANTA CRUZ DE TENERIFE", "LAS PALMAS"))) match {
//    case Right(value) => {
//      println(value.count())
//      value.show(value.count().toInt)
//    }
//    case Left(exception) => throw exception
//  }
//
//  SparkQueries.getAllStationsByStatesAvgClimateParamInALapse("tmed", "2024", None, None) match {
//    case Right(value) => {
//      println(value.count())
//      value.show(value.count().toInt)
//    }
//    case Left(exception) => throw exception
//  }

  SparkQueries.Climograph.execute()
  SparkQueries.Temperature.execute()

























  //AemetAPIClient.AllStationsData.saveAllStations()
  //AemetAPIClient.AllStationsMeteorologicalDataBetweenDates.saveAllStationsMeteorologicalDataBetweenDates()
//  IfapaAPIClient.StationMeteorologicalDataBetweenDates.saveStationMeteorologicalDataBetweenDates()
//  IfapaAPIClient.StationData.saveStationData()

//  val spark = SparkSession.builder().appName("AEMET Spark Study").master("local[*]").getOrCreate()
//  spark.sparkContext.setLogLevel("ERROR")
//
//  val schema = StructType(List(
//    StructField("fecha", StringType, false),
//    StructField("indicativo", StringType, false),
//    StructField("nombre", StringType, false),
//    StructField("provincia", StringType, false),
//    StructField("altitud", StringType, false),
//    StructField("tmed", StringType, true),
//    StructField("prec", StringType, true),
//    StructField("tmin", StringType, true),
//    StructField("horatmin", StringType, true),
//    StructField("tmax", StringType, true),
//    StructField("horatmax", StringType, true),
//    StructField("dir", StringType, true),
//    StructField("velmedia", StringType, true),
//    StructField("racha", StringType, true),
//    StructField("horaracha", StringType, true),
//    StructField("sol", StringType, true),
//    StructField("presmax", StringType, true),
//    StructField("horapresmax", StringType, true),
//    StructField("presmin", StringType, true),
//    StructField("horapresmin", StringType, true),
//    StructField("hrmedia", StringType, true),
//    StructField("hrmax", StringType, true),
//    StructField("horahrmax", StringType, true),
//    StructField("hrmin", StringType, true),
//    StructField("horahrmin", StringType, true)
//  ))
//
//  println(Config.AemetPaths.AllStationsMeteorologicalDataBetweenDates.jsonData)
//
//  val df = spark.read.format("json").option("multiline", value = true).schema(schema).json(Config.AemetPaths.AllStationsMeteorologicalDataBetweenDates.jsonData)
//
//  val dfCast = df.withColumn("horapresmin", col("horapresmin").cast(FloatType))
//
//  dfCast.createOrReplaceTempView("stations")
//
//  val result = spark.sql("""
//  SELECT *
//  FROM stations
//  WHERE nombre = 'GÜÍMAR' AND tmed = '19,6'
//""")
//
//  result.show(result.count().toInt, false)
//
//  val result_2 = spark.sql(s"""
//  SELECT DISTINCT(nombre), provincia
//  FROM stations
//  WHERE sol IS NOT NULL
//  """)
//
//  result_2.show(result_2.count().toInt, false)
//  println(result_2.count().toInt)
//
//  val result_3 = df
//    .select(col("nombre"), col("provincia"))
//    .filter(col("sol").isNotNull)
//    .dropDuplicates("nombre")
//    .orderBy("provincia")
//
//  // Mostrar el resultado en la consola
//  result_3.show(result_3.count().toInt, truncate = false)
//  println(result_3.count().toInt)

//  SparkQueries.Aemet.stationMonthlyAvgTempAndPrecInAYear("7002Y", 2024)
//  SparkQueries.Aemet.stationInfo("7002Y")
//  SparkQueries.Aemet.stationMonthlyAvgTempAndPrecInAYear("C249I", 2024)
//  SparkQueries.Aemet.stationInfo("C249I")
//  SparkQueries.Aemet.stationMonthlyAvgTempAndPrecInAYear("2462", 2024)
//  SparkQueries.Aemet.stationInfo("2462")
//  SparkQueries.Aemet.stationMonthlyAvgTempAndPrecInAYear("9839V", 2024)
//  SparkQueries.Aemet.stationInfo("9839V")
//  SparkQueries.Ifapa.tabernasAlmeriaStationMonthlyAvgTempAndPrecIn2024()

//  val aemetAllMeteoInfoMetadataKeys = ConstantsV2.RemoteRequest.AemetAPI.Params.Global.Metadata.SchemaJSONKeys
//  val json0: ujson.Value = ujson.read("""{
//                                        |  "unidad_generadora": "Servicio del Banco Nacional de Datos Climatológicos",
//                                        |  "periodicidad": "1 vez al día, con un retardo de 4 días",
//                                        |  "descripcion": "Climatologías diarias",
//                                        |  "formato": "application/json",
//                                        |  "copyright": "© AEMET. Autorizado el uso de la información y su reproducción citando a AEMET como autora de la misma.",
//                                        |  "notaLegal": "https://www.aemet.es/es/nota_legal",
//                                        |  "campos": [
//                                        |    {
//                                        |      "id": "fecha",
//                                        |      "descripcion": "fecha del dia (AAAA-MM-DD)",
//                                        |      "tipo_datos": "string",
//                                        |      "requerido": true
//                                        |    },
//                                        |    {
//                                        |      "id": "indicativo",
//                                        |      "descripcion": "indicativo climatológico",
//                                        |      "tipo_datos": "string",
//                                        |      "requerido": true
//                                        |    },
//                                        |    {
//                                        |      "id": "nombre",
//                                        |      "descripcion": "nombre (ubicación) de la estación",
//                                        |      "tipo_datos": "string",
//                                        |      "requerido": true
//                                        |    },
//                                        |    {
//                                        |      "id": "provincia",
//                                        |      "descripcion": "provincia de la estación",
//                                        |      "tipo_datos": "string",
//                                        |      "requerido": true
//                                        |    },
//                                        |    {
//                                        |      "id": "altitud",
//                                        |      "descripcion": "altitud de la estación en m sobre el nivel del mar",
//                                        |      "tipo_datos": "float",
//                                        |      "unidad": "m",
//                                        |      "requerido": true
//                                        |    },
//                                        |    {
//                                        |      "id": "tmed",
//                                        |      "descripcion": "Temperatura media diaria",
//                                        |      "tipo_datos": "float",
//                                        |      "unidad": "°C",
//                                        |      "requerido": false
//                                        |    },
//                                        |    {
//                                        |      "id": "prec",
//                                        |      "descripcion": "Precipitación diaria de 07 a 07",
//                                        |      "tipo_datos": "float",
//                                        |      "unidad": "mm (Ip = inferior a 0,1 mm) (Acum = Precipitación acumulada)",
//                                        |      "requerido": false
//                                        |    },
//                                        |    {
//                                        |      "id": "tmin",
//                                        |      "descripcion": "Temperatura Mínima del día",
//                                        |      "tipo_datos": "float",
//                                        |      "unidad": "°C",
//                                        |      "requerido": false
//                                        |    },
//                                        |    {
//                                        |      "id": "horatmin",
//                                        |      "descripcion": "Hora y minuto de la temperatura mínima",
//                                        |      "tipo_datos": "string",
//                                        |      "unidad": "UTC",
//                                        |      "requerido": false
//                                        |    },
//                                        |    {
//                                        |      "id": "tmax",
//                                        |      "descripcion": "Temperatura Máxima del día",
//                                        |      "tipo_datos": "float",
//                                        |      "unidad": "°C",
//                                        |      "requerido": false
//                                        |    },
//                                        |    {
//                                        |      "id": "horatmax",
//                                        |      "descripcion": "Hora y minuto de la temperatura máxima",
//                                        |      "tipo_datos": "string",
//                                        |      "unidad": "UTC",
//                                        |      "requerido": false
//                                        |    },
//                                        |    {
//                                        |      "id": "dir",
//                                        |      "descripcion": "Dirección de la racha máxima",
//                                        |      "tipo_datos": "float",
//                                        |      "unidad": "decenas de grado (99 = dirección variable)(88 = sin dato)",
//                                        |      "requerido": false
//                                        |    },
//                                        |    {
//                                        |      "id": "velmedia",
//                                        |      "descripcion": "Velocidad media del viento",
//                                        |      "tipo_datos": "float",
//                                        |      "unidad": "m/s",
//                                        |      "requerido": false
//                                        |    },
//                                        |    {
//                                        |      "id": "racha",
//                                        |      "descripcion": "Racha máxima del viento",
//                                        |      "tipo_datos": "float",
//                                        |      "unidad": "m/s",
//                                        |      "requerido": false
//                                        |    },
//                                        |    {
//                                        |      "id": "horaracha",
//                                        |      "descripcion": "Hora y minuto de la racha máxima",
//                                        |      "tipo_datos": "string",
//                                        |      "unidad": "UTC",
//                                        |      "requerido": false
//                                        |    },
//                                        |    {
//                                        |      "id": "sol",
//                                        |      "descripcion": "Insolación",
//                                        |      "tipo_datos": "float",
//                                        |      "unidad": "horas",
//                                        |      "requerido": false
//                                        |    },
//                                        |    {
//                                        |      "id": "presmax",
//                                        |      "descripcion": "Presión máxima al nivel de referencia de la estación",
//                                        |      "tipo_datos": "float",
//                                        |      "unidad": "hPa",
//                                        |      "requerido": false
//                                        |    },
//                                        |    {
//                                        |      "id": "horapresmax",
//                                        |      "descripcion": "Hora de la presión máxima (redondeada a la hora entera más próxima)",
//                                        |      "tipo_datos": "string",
//                                        |      "unidad": "UTC",
//                                        |      "requerido": false
//                                        |    },
//                                        |    {
//                                        |      "id": "presmin",
//                                        |      "descripcion": "Presión mínima al nivel de referencia de la estación",
//                                        |      "tipo_datos": "float",
//                                        |      "unidad": "hPa",
//                                        |      "requerido": false
//                                        |    },
//                                        |    {
//                                        |      "id": "horapresmin",
//                                        |      "descripcion": "Hora de la presión mínima (redondeada a la hora entera más próxima)",
//                                        |      "tipo_datos": "string",
//                                        |      "unidad": "UTC",
//                                        |      "requerido": false
//                                        |    },
//                                        |    {
//                                        |      "id": "hrmedia",
//                                        |      "descripcion": "Humedad relativa media diaria",
//                                        |      "tipo_datos": "float",
//                                        |      "unidad": "%",
//                                        |      "requerido": false
//                                        |    },
//                                        |    {
//                                        |      "id": "hrmax",
//                                        |      "descripcion": "Humedad relativa máxima diaria",
//                                        |      "tipo_datos": "float",
//                                        |      "unidad": "%",
//                                        |      "requerido": false
//                                        |    },
//                                        |    {
//                                        |      "id": "horahrmax",
//                                        |      "descripcion": "Hora de la humedad relativa máxima diaria",
//                                        |      "tipo_datos": "string",
//                                        |      "unidad": "UTC",
//                                        |      "requerido": false
//                                        |    },
//                                        |    {
//                                        |      "id": "hrmin",
//                                        |      "descripcion": "Humedad relativa mínima diaria",
//                                        |      "tipo_datos": "float",
//                                        |      "unidad": "%",
//                                        |      "requerido": false
//                                        |    },
//                                        |    {
//                                        |      "id": "horahrmin",
//                                        |      "descripcion": "Hora de la humedad relativa mínima diaria",
//                                        |      "tipo_datos": "string",
//                                        |      "unidad": "UTC",
//                                        |      "requerido": false
//                                        |    }
//                                        |  ]
//                                        |}""".stripMargin)
//  val json1: ujson.Value = ujson.read("""{
//                                        |    "fecha": "2024-01-01",
//                                        |    "dia": 1,
//                                        |    "tempmedia": 9.53,
//                                        |    "tempmax": 17.93,
//                                        |    "hormintempmax": "14:06",
//                                        |    "tempmin": 2.963,
//                                        |    "hormintempmin": "23:50",
//                                        |    "humedadmedia": 70.3,
//                                        |    "humedadmax": 96.9,
//                                        |    "horminhummax": "23:58",
//                                        |    "humedadmin": 37.68,
//                                        |    "horminhummin": "13:40",
//                                        |    "velviento": 0.883,
//                                        |    "dirviento": 85.6,
//                                        |    "velvientomax": 4.675,
//                                        |    "horminvelmax": "15:20",
//                                        |    "dirvientovelmax": 110.1,
//                                        |    "radiacion": 9.92,
//                                        |    "precipitacion": 0,
//                                        |    "bateria": 12.45,
//                                        |    "fechautlmod": "2024-01-03T09:00:00.000+0100",
//                                        |    "et0": null
//                                        |  }""".stripMargin)
//  val json2: ujson.Value = ujson.read("""{
//                                        |  "provincia": {
//                                        |    "id": 4,
//                                        |    "nombre": "Almería"
//                                        |  },
//                                        |  "codigoestacion": "4",
//                                        |  "nombre": "Tabernas",
//                                        |  "bajoplastico": false,
//                                        |  "activa": true,
//                                        |  "visible": true,
//                                        |  "longitud": "021808000W",
//                                        |  "latitud": "370528000N",
//                                        |  "altitud": 502,
//                                        |  "xutm": 561998,
//                                        |  "yutm": 4105230,
//                                        |  "huso": 30
//                                        |}""".stripMargin)
//
//  val result = JSONUtils.buildJSONFromSchemaAndData(
//    IfapaToAemetConverter.generateEmptyAemetJSONFromMetadata(json0),
//    IfapaToAemetConverter.IfapaStationMeteoInfo.generateAemetMeteoInfoJSONKeysToIfapaJSONValues(json1, json2)
//  )
//
//  val resultConverted = JSONUtils.transformJSONValues(result, IfapaToAemetConverter.IfapaStationMeteoInfo.generateAemetMeteoInfoFormatters())
//
//  // Mostrar el resultado
//  println(ujson.write(result, indent = 2))
//  println(ujson.write(resultConverted, indent = 2))
//
//  val json3: ujson.Value = ujson.read("""{
//                                        |  "unidad_generadora": "Servicio del Banco de Datos Nacional de Climatología",
//                                        |  "periodicidad": "1 vez al día",
//                                        |  "descripcion": "Inventario de estaciones para el apartado Valores Climatología",
//                                        |  "formato": "application/json",
//                                        |  "copyright": "© AEMET. Autorizado el uso de la información y su reproducción citando a AEMET como autora de la misma.",
//                                        |  "notaLegal": "https://www.aemet.es/es/nota_legal",
//                                        |  "campos": [
//                                        |    {
//                                        |      "id": "latitud",
//                                        |      "descripcion": "latitud de la estación",
//                                        |      "tipo_datos": "string",
//                                        |      "requerido": true
//                                        |    },
//                                        |    {
//                                        |      "id": "provincia",
//                                        |      "descripcion": "provincia donde reside la estación",
//                                        |      "tipo_datos": "string",
//                                        |      "requerido": true
//                                        |    },
//                                        |    {
//                                        |      "id": "indicativo",
//                                        |      "descripcion": "indicativo climatológico de la estación",
//                                        |      "tipo_datos": "string",
//                                        |      "requerido": true
//                                        |    },
//                                        |    {
//                                        |      "id": "altitud",
//                                        |      "descripcion": "altitud de la estación ",
//                                        |      "tipo_datos": "string",
//                                        |      "requerido": true
//                                        |    },
//                                        |    {
//                                        |      "id": "nombre",
//                                        |      "descripcion": "ubicación de la estación",
//                                        |      "tipo_datos": "string",
//                                        |      "requerido": true
//                                        |    },
//                                        |    {
//                                        |      "id": "indsinop",
//                                        |      "descripcion": "Indicativo sinóptico",
//                                        |      "tipo_datos": "string",
//                                        |      "requerido": true
//                                        |    },
//                                        |    {
//                                        |      "id": "longitud",
//                                        |      "descripcion": "longitud de la estación",
//                                        |      "tipo_datos": "string",
//                                        |      "requerido": true
//                                        |    }
//                                        |  ]
//                                        |}""".stripMargin)
//  val json4: ujson.Value = ujson.read("""{
//                                        |  "provincia": {
//                                        |    "id": 4,
//                                        |    "nombre": "Almería"
//                                        |  },
//                                        |  "codigoestacion": "4",
//                                        |  "nombre": "Tabernas",
//                                        |  "bajoplastico": false,
//                                        |  "activa": true,
//                                        |  "visible": true,
//                                        |  "longitud": "021808000W",
//                                        |  "latitud": "370528000N",
//                                        |  "altitud": 502,
//                                        |  "xutm": 561998,
//                                        |  "yutm": 4105230,
//                                        |  "huso": 30
//                                        |}""".stripMargin)
//
//  val result2 = JSONUtils.buildJSONFromSchemaAndData(
//    IfapaToAemetConverter.generateEmptyAemetJSONFromMetadata(json3),
//    IfapaToAemetConverter.IfapaStationInfo.generateAemetStationInfoJSONKeysToIfapaJSONValues(json4)
//  )
//
//  val resultConverted2 = JSONUtils.transformJSONValues(result2, IfapaToAemetConverter.IfapaStationInfo.generateAemetStationInfoFormatters())
//
//  // Mostrar el resultado
//  println(ujson.write(result2, indent = 2))
//  println(ujson.write(resultConverted2, indent = 2))
































  //  import org.apache.spark.sql.SparkSession
//  import scala.concurrent.{Future, ExecutionContext, Await}
//  import scala.concurrent.duration._
//  import ExecutionContext.Implicits.global
//
//  val spark = SparkSession.builder()
//    .appName("HeavyQueryTest")
//    .master("local[*]") // Usa todos los núcleos disponibles
//    .getOrCreate()
//
//  // Aumentamos el rango de datos a 10 millones
//  val df = spark.range(1, 10000000)
//
//  // Añadir más columnas para aumentar la complejidad
//  val dfWithValues = df.withColumn("value", (df("id") % 100).cast("int"))
//    .withColumn("value2", (df("id") % 200).cast("int"))
//    .withColumn("value3", (df("id") % 50).cast("int"))
//
//  // Realizar una consulta mucho más costosa
//  println("===== Ejecutando versión concurrente (con consulta aún más pesada) =====")
//
//  // Iniciar temporizador para la versión concurrente
//  val startConcurrent = System.nanoTime()
//
//  // Ejecutar múltiples consultas pesadas en paralelo, cada una dentro de su propio Future
//  val future1 = Future {
//    val heavyQuery1 = dfWithValues.groupBy("value", "value2", "value3")
//      .agg(
//        org.apache.spark.sql.functions.avg("id").alias("avg_id"),
//        org.apache.spark.sql.functions.max("id").alias("max_id"),
//        org.apache.spark.sql.functions.min("id").alias("min_id"),
//        org.apache.spark.sql.functions.sum("id").alias("sum_id"),
//        org.apache.spark.sql.functions.count("id").alias("count_id")
//      )
//    val result1 = heavyQuery1.collect() // Ejecuta la consulta en el primer conjunto de datos
//    println(s"Consulta 1 completada con ${result1.length} filas")
//    result1
//  }
//
//  val future2 = Future {
//    val heavyQuery2 = dfWithValues.groupBy("value", "value2", "value3")
//      .agg(
//        org.apache.spark.sql.functions.avg("id").alias("avg_id"),
//        org.apache.spark.sql.functions.max("id").alias("max_id"),
//        org.apache.spark.sql.functions.min("id").alias("min_id"),
//        org.apache.spark.sql.functions.sum("id").alias("sum_id"),
//        org.apache.spark.sql.functions.count("id").alias("count_id")
//      )
//    val result2 = heavyQuery2.collect() // Ejecuta la consulta en el segundo conjunto de datos
//    println(s"Consulta 2 completada con ${result2.length} filas")
//    result2
//  }
//
//  val future3 = Future {
//    val heavyQuery3 = dfWithValues.groupBy("value", "value2", "value3")
//      .agg(
//        org.apache.spark.sql.functions.avg("id").alias("avg_id"),
//        org.apache.spark.sql.functions.max("id").alias("max_id"),
//        org.apache.spark.sql.functions.min("id").alias("min_id"),
//        org.apache.spark.sql.functions.sum("id").alias("sum_id"),
//        org.apache.spark.sql.functions.count("id").alias("count_id")
//      )
//    val result3 = heavyQuery3.collect() // Ejecuta la consulta en el tercer conjunto de datos
//    println(s"Consulta 3 completada con ${result3.length} filas")
//    result3
//  }
//
//  // Esperar a que todas las operaciones terminen
//  val resultsConcurrent = Await.result(Future.sequence(Seq(future1, future2, future3)), 20.minutes)
//
//  // Medir el tiempo final para la versión concurrente
//  val endConcurrent = System.nanoTime()
//  val timeConcurrent = (endConcurrent - startConcurrent) / 1e9 // Convertir a segundos
//
//  println(s"Resultados finales (concurrente): ${resultsConcurrent.mkString(", ")}")
//  println(f"Tiempo de ejecución concurrente: $timeConcurrent%.2f segundos\n")
//
//  println("===== Ejecutando versión secuencial (con consulta aún más pesada) =====")
//
//  // Iniciar temporizador para la versión secuencial
//  val startSequential = System.nanoTime()
//
//  // Ejecutar las mismas consultas secuenciales
//  val result1Seq = {
//    val heavyQuery1 = dfWithValues.groupBy("value", "value2", "value3")
//      .agg(
//        org.apache.spark.sql.functions.avg("id").alias("avg_id"),
//        org.apache.spark.sql.functions.max("id").alias("max_id"),
//        org.apache.spark.sql.functions.min("id").alias("min_id"),
//        org.apache.spark.sql.functions.sum("id").alias("sum_id"),
//        org.apache.spark.sql.functions.count("id").alias("count_id")
//      )
//    heavyQuery1.collect()
//  }
//  println(s"Consulta 1 completada con ${result1Seq.length} filas")
//
//  val result2Seq = {
//    val heavyQuery2 = dfWithValues.groupBy("value", "value2", "value3")
//      .agg(
//        org.apache.spark.sql.functions.avg("id").alias("avg_id"),
//        org.apache.spark.sql.functions.max("id").alias("max_id"),
//        org.apache.spark.sql.functions.min("id").alias("min_id"),
//        org.apache.spark.sql.functions.sum("id").alias("sum_id"),
//        org.apache.spark.sql.functions.count("id").alias("count_id")
//      )
//    heavyQuery2.collect()
//  }
//  println(s"Consulta 2 completada con ${result2Seq.length} filas")
//
//  val result3Seq = {
//    val heavyQuery3 = dfWithValues.groupBy("value", "value2", "value3")
//      .agg(
//        org.apache.spark.sql.functions.avg("id").alias("avg_id"),
//        org.apache.spark.sql.functions.max("id").alias("max_id"),
//        org.apache.spark.sql.functions.min("id").alias("min_id"),
//        org.apache.spark.sql.functions.sum("id").alias("sum_id"),
//        org.apache.spark.sql.functions.count("id").alias("count_id")
//      )
//    heavyQuery3.collect()
//  }
//  println(s"Consulta 3 completada con ${result3Seq.length} filas")
//
//  // Medir el tiempo final para la versión secuencial
//  val endSequential = System.nanoTime()
//  val timeSequential = (endSequential - startSequential) / 1e9 // Convertir a segundos
//
//  println(s"Resultados finales (secuencial): $result1Seq, $result2Seq, $result3Seq")
//  println(f"Tiempo de ejecución secuencial: $timeSequential%.2f segundos")
//
//  // Cerrar la sesión de Spark
//  spark.stop()















  // Función para dividir el rango en 5 tramos
//  def splitDateRange(startDateTime: LocalDateTime, endDateTime: LocalDateTime): Seq[(LocalDateTime, LocalDateTime)] = {
//    val totalSeconds = ChronoUnit.SECONDS.between(startDateTime, endDateTime) // Total de segundos entre fechas
//    val numberOfTramos = 5 // Queremos dividirlo en 5 tramos
//
//    // Calcular el número de segundos que debe tener cada tramo
//    val tramoDurationInSeconds = totalSeconds / numberOfTramos
//
//    // Generamos las 4 fechas frontera (las 4 fechas intermedias)
//    val borders = (1 until numberOfTramos).map { i =>
//      startDateTime.plusSeconds(tramoDurationInSeconds * i).toLocalDate.atStartOfDay() // Capar a las 00:00:00
//    }
//
//    // Crear los tramos
//    var currentStartDateTime = startDateTime
//    var dateRanges = Seq[(LocalDateTime, LocalDateTime)]()
//
//    // Generamos tramos en base a las fechas frontera
//    borders.foreach { border =>
//      val endOfTramo = border.minusSeconds(1)  // Fin de tramo: un segundo antes de la frontera (23:59:59)
//      dateRanges = dateRanges :+ (currentStartDateTime, endOfTramo)
//      currentStartDateTime = border  // La siguiente fecha empieza a la hora exacta de la frontera
//    }
//
//    // El último tramo va desde la última frontera hasta la fecha de fin
//    dateRanges = dateRanges :+ (currentStartDateTime, endDateTime)
//
//    dateRanges
//  }
//
//  // Función para imprimir el resultado
//  val startDateTime = LocalDateTime.of(1973, 1, 1, 0, 0, 0)  // Fecha de inicio
//  val endDateTime = LocalDateTime.of(2024, 12, 31, 23, 59, 59)   // Fecha final
//
//  val dateRanges = splitDateRange(startDateTime, endDateTime)
//
//  // Mostrar los tramos generados con la duración en segundos de cada uno
//  dateRanges.foreach { case (start, end) =>
//    val durationInSeconds = ChronoUnit.SECONDS.between(start, end)
//    println(s"Tramo: $start a $end | Duración: $durationInSeconds segundos")
//  }




//  // Crear un pool de 3 hilos
//  val threadPool = Executors.newFixedThreadPool(3)
//  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(threadPool)
//
//  // Cola de tareas compartida
//  val taskQueue = new LinkedBlockingQueue[() => Unit]()
//
//  // Generar 100 tareas con tiempos aleatorios entre 100ms y 2500ms
//  val tasks = (1 to 100).map { i =>
//    val sleepTime = Random.between(100, 2501) // Tiempo aleatorio entre 100 y 2500 ms
//    () => {
//      println(s"[${Thread.currentThread().getName}] Tarea $i ejecutada (duración: ${sleepTime}ms)")
//      Thread.sleep(sleepTime)
//    }
//  }
//
//  // Agregar tareas a la cola
//  tasks.foreach(taskQueue.put)
//
//  // Función para que los hilos trabajen
//  def worker(): Unit = {
//    while (!taskQueue.isEmpty) {
//      val task = taskQueue.poll()  // Tomar tarea de la cola
//      if (task != null) task()      // Ejecutar tarea
//    }
//  }
//
//  // Lanzar los workers en hilos secundarios
//  (1 to 3).foreach(_ => Future(worker())(ec))
//
//  // El `main` también trabaja cuando está libre
//  worker()
//
//  // Esperar a que todos terminen antes de cerrar el pool
//  threadPool.shutdown()



















//  // Función para crear un ZonedDateTime a partir de una fecha en formato string
//  def getDateZonedDateTime(date: String): ZonedDateTime = {
//    ZonedDateTime.parse(date)
//  }
//
//  // Formateador para imprimir la fecha en el formato UTC
//  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'UTC'")
//
//  // Función para dividir el rango en 5 tramos
//  def splitDateRange(startDateTime: ZonedDateTime, endDateTime: ZonedDateTime): Seq[(ZonedDateTime, ZonedDateTime)] = {
//    val totalSeconds = Duration.between(startDateTime, endDateTime).getSeconds // Total de segundos entre fechas
//    val numberOfTramos = 5 // Queremos dividirlo en 5 tramos
//
//    // Calcular el número de segundos que debe tener cada tramo
//    val tramoDurationInSeconds = totalSeconds / numberOfTramos
//
//    // Generamos las 4 fechas frontera (las 4 fechas intermedias)
//    val borders = (1 until numberOfTramos).map { i =>
//      startDateTime.plusSeconds(tramoDurationInSeconds * i).withZoneSameInstant(java.time.ZoneOffset.UTC).toLocalDate.atStartOfDay().atZone(java.time.ZoneOffset.UTC)
//    }
//
//    // Crear los tramos
//    var currentStartDateTime = startDateTime
//    var dateRanges = Seq[(ZonedDateTime, ZonedDateTime)]()
//
//    // Generamos tramos en base a las fechas frontera
//    borders.foreach { border =>
//      val endOfTramo = border.minusSeconds(1)  // Fin de tramo: un segundo antes de la frontera (23:59:59)
//      dateRanges = dateRanges :+ (currentStartDateTime, endOfTramo)
//      currentStartDateTime = border  // La siguiente fecha empieza a la hora exacta de la frontera
//    }
//
//    // El último tramo va desde la última frontera hasta la fecha de fin
//    dateRanges = dateRanges :+ (currentStartDateTime, endDateTime)
//
//    dateRanges
//  }
//
//  // Lógica de procesamiento de cada tramo (simulación de un procesamiento con `sleep`)
//  def processTramo(start: ZonedDateTime, end: ZonedDateTime): String = {
//    // Obtener el nombre del hilo que está ejecutando el tramo
//    val threadName = Thread.currentThread().getName
//    println(s"Ejecutando tramo: ${formatter.format(start)} a ${formatter.format(end)} en el hilo: $threadName")
//
//    // Simulación de procesamiento con un sleep de 2 segundos por tramo
//    Thread.sleep(1000) // Simula un procesamiento que dura 2 segundos
//
//    s"Finalizado tramo: ${formatter.format(start)} a ${formatter.format(end)} en el hilo: $threadName"
//  }
//
//  // Función principal
//  val semaphore = new Semaphore(0)
//  val startDateTime = getDateZonedDateTime("1973-01-01T00:00:00Z")  // Fecha de inicio
//  val endDateTime = getDateZonedDateTime("2024-12-31T23:59:59Z")   // Fecha final
//
//  val dateRanges = splitDateRange(startDateTime, endDateTime)
//
//  // Opción para elegir el tipo de Executor (global o personalizado)
//  val useGlobalExecutor = true // Cambia esto a false para usar el executor personalizado
//
//  // Crear el ExecutionContext según la elección
//  val customExecutionContext: ExecutionContext = if (useGlobalExecutor) {
//    // Usar el Executor global (predeterminado en Scala)
//    ExecutionContext.global
//  } else {
//    // Usar un Executor personalizado
//    val numThreads = 5 // Número de hilos, puedes ajustarlo según el tamaño de los tramos
//    val executorService = Executors.newFixedThreadPool(numThreads)
//    ExecutionContext.fromExecutorService(executorService)
//  }
//
//  println(Thread.currentThread().getName)
//
//  // Crear un futuro para cada tramo y procesarlos de forma concurrente usando el ExecutionContext seleccionado
//  val futures = dateRanges.map { case (start, end) =>
//    Future {
//      // Llamada al procesamiento de cada tramo
//      val result = processTramo(start, end)
//      semaphore.acquire()
//      println(result) // Mostrar resultado cuando el procesamiento termine
//    }(customExecutionContext) // Usar el ExecutionContext seleccionado
//  }
//
//  // Esperar a que todos los futuros terminen
//  val results = Future.sequence(futures)
//
//  // Imprimir los resultados cuando todos los futuros terminen
//  results.onComplete {
//    case Success(res) =>
//      println(Thread.currentThread().getName)
//      println("Todos los tramos han sido procesados.")
//      if (!useGlobalExecutor) {
//        // Cerrar el executor personalizado después de que todos los tramos han sido procesados
//
//
//        customExecutionContext.asInstanceOf[ExecutionContextExecutorService].shutdown()
//      }
//    case Failure(exception) =>
//      println(Thread.currentThread().getName)
//      println(s"Hubo un error en el procesamiento: ${exception.toString}")
//      if (!useGlobalExecutor) {
//        // Asegurarse de cerrar el executor si ocurre un error
//        customExecutionContext.asInstanceOf[ExecutionContextExecutorService].shutdown()
//      }
//  }
//
//  println(Thread.currentThread().getName)
//  Thread.sleep(5000)
//  println("Hilo principal")
//  semaphore.release()
//
//  // Para esperar que todos los resultados se impriman
//  Await.result(results, 10.minutes)  // Puedes ajustar el tiempo de espera según sea necesario






}
