

object AemetBigDataSparkStudyApp extends App {
  AemetAPIClient.AllStationsData.saveAllStations()
  AemetAPIClient.AllStationsMeteorologicalDataBetweenDates.saveAllStationsMeteorologicalDataBetweenDates()
}
