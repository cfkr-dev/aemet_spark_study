package Config.PlotGenerationConf.Execution.DTO

import upickle.default.{ReadWriter, macroRW}
import upickle.implicits.key

case class HeatMapDTOSrcNamesComponent(
  @key("longitude") longitude: String,
  @key("latitude") latitude: String,
  @key("value") value: String
)
object HeatMapDTOSrcNamesComponent { implicit val rw: ReadWriter[HeatMapDTOSrcNamesComponent] = macroRW }

case class HeatMapDTOSrc(
  @key("path") path: String,
  @key("names") names: HeatMapDTOSrcNamesComponent,
  @key("location") location: String
)
object HeatMapDTOSrc { implicit val rw: ReadWriter[HeatMapDTOSrc] = macroRW }

case class HeatMapDTODest(
  @key("path") path: String,
  @key("filename") filename: String,
  @key("export_png") exportPng: Boolean
)
object HeatMapDTODest { implicit val rw: ReadWriter[HeatMapDTODest] = macroRW }

case class HeatMapDTOLetteringPointInfoComponentBuildComponent(
  @key("text_before") textBefore: String,
  @key("name") name: String,
  @key("text_after") textAfter: String
)
object HeatMapDTOLetteringPointInfoComponentBuildComponent { implicit val rw: ReadWriter[HeatMapDTOLetteringPointInfoComponentBuildComponent] = macroRW }

case class HeatMapDTOLetteringPointInfoComponent(
  @key("label") label: String,
  @key("build") build: List[HeatMapDTOLetteringPointInfoComponentBuildComponent]
)
object HeatMapDTOLetteringPointInfoComponent { implicit val rw: ReadWriter[HeatMapDTOLetteringPointInfoComponent] = macroRW }

case class HeatMapDTOLettering(
  @key("title") title: String,
  @key("subtitle") subtitle: String,
  @key("long_label") longLabel: String,
  @key("lat_label") latLabel: String,
  @key("legend_label") legendLabel: String,
  @key("point_info") pointInfo: Option[List[HeatMapDTOLetteringPointInfoComponent]]
)
object HeatMapDTOLettering { implicit val rw: ReadWriter[HeatMapDTOLettering] = macroRW }

case class HeatMapDTOFigure(
  @key("name") name: String,
  @key("color") color: String,
  @key("color_opacity") colorOpacity: Float,
  @key("point_size") pointSize: Float
)
object HeatMapDTOFigure { implicit val rw: ReadWriter[HeatMapDTOFigure] = macroRW }

case class HeatMapDTOMargin(
  @key("left") left: Float,
  @key("right") right: Float,
  @key("top") top: Float,
  @key("bottom") bottom: Float
)
object HeatMapDTOMargin { implicit val rw: ReadWriter[HeatMapDTOMargin] = macroRW }

case class HeatMapDTOStyle(
  @key("lettering") lettering: HeatMapDTOLettering,
  @key("figure") figure: HeatMapDTOFigure,
  @key("margin") margin: Option[HeatMapDTOMargin]
)
object HeatMapDTOStyle { implicit val rw: ReadWriter[HeatMapDTOStyle] = macroRW }

case class HeatMapDTO (
  @key("src") src: HeatMapDTOSrc, 
  @key("dest") dest: HeatMapDTODest, 
  @key("style") style: HeatMapDTOStyle
)
object HeatMapDTO { implicit val rw: ReadWriter[HeatMapDTO] = macroRW }
