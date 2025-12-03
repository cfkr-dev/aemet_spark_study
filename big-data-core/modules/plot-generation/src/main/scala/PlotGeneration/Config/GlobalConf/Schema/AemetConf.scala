package PlotGeneration.Config.GlobalConf.Schema

case class AemetAllMeteoInfo(
  fecha: String,
  indicativo: String,
  nombre: String,
  provincia: String,
  altitud: String,
  tMed: String,
  prec: String,
  tMin: String,
  horaTMin: String,
  tMax: String,
  horaTMax: String,
  dir: String,
  velMedia: String,
  racha: String,
  horaRacha: String,
  sol: String,
  presMax: String,
  horaPresMax: String,
  presMin: String,
  horaPresMin: String,
  hrMedia: String,
  hrMax: String,
  horaHrMax: String,
  hrMin: String,
  horaHrMin: String
)

case class AemetAllStationInfo(
  latitud: String,
  provincia: String,
  indicativo: String,
  altitud: String,
  nombre: String,
  indsinop: String,
  longitud: String
)

case class AemetConf(allMeteoInfo: AemetAllMeteoInfo, allStationInfo: AemetAllStationInfo)
