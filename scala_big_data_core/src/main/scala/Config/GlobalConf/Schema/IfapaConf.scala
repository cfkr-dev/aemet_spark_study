package Config.GlobalConf.Schema

case class IfapaSingleStationMeteoInfo(
  bateria: String,
  dia: String,
  dirViento: String,
  dirVientoVelMax: String,
  et0: String,
  fecha: String,
  fechaUtlMod: String,
  horMinHumMax: String,
  horMinHumMin: String,
  horMinTempMax: String,
  horMinTempMin: String,
  horMinVelMax: String,
  humedadMax: String,
  humedadMedia: String,
  humedadMin: String,
  precipitacion: String,
  radiacion: String,
  tempMax: String,
  tempMedia: String,
  tempMin: String,
  velViento: String,
  velVientoMax: String
)

case class IfapaSingleStationInfo(
  activa: String,
  altitud: String,
  bajoPlastico: String,
  codigoEstacion: String,
  huso: String,
  latitud: String,
  longitud: String,
  nombre: String,
  provincia: String,
  visible: String,
  xutm: String,
  yutm: String
)

case class IfapaSingleStateInfo(
  id: String,
  nombre: String
)

case class IfapaConf(
  singleStationMeteoInfo: IfapaSingleStationMeteoInfo,
  singleStationInfo: IfapaSingleStationInfo,
  singleStateInfo: IfapaSingleStateInfo
)
