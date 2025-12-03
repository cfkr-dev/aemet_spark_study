package Config.PlotGenerationConf.Execution.DTO

import upickle.default.{ReadWriter, macroRW}
import upickle.implicits.key

case class ClimographDTOAxisComponent(
  @key("name") name: String,
  @key("format") format: Option[String]
)
object ClimographDTOAxisComponent { implicit val rw: ReadWriter[ClimographDTOAxisComponent] = macroRW }

case class ClimographDTOAxis(
  @key("x") x: ClimographDTOAxisComponent,
  @key("y_temp") yTemp: ClimographDTOAxisComponent,
  @key("y_prec") yPrec: ClimographDTOAxisComponent
)
object ClimographDTOAxis { implicit val rw: ReadWriter[ClimographDTOAxis] = macroRW }

case class ClimographDTOSrc(
  @key("path") path: String,
  @key("axis") axis: ClimographDTOAxis
)
object ClimographDTOSrc { implicit val rw: ReadWriter[ClimographDTOSrc] = macroRW }

case class ClimographDTODest(
  @key("path") path: String,
  @key("filename") filename: String,
  @key("export_png") exportPng: Boolean
)
object ClimographDTODest { implicit val rw: ReadWriter[ClimographDTODest] = macroRW }

case class ClimographDTOLettering(
  @key("title") title: String,
  @key("subtitle") subtitle: String,
  @key("x_label") xLabel: String,
  @key("y_temp_label") yTempLabel: String,
  @key("y_prec_label") yPrecLabel: String
)
object ClimographDTOLettering { implicit val rw: ReadWriter[ClimographDTOLettering] = macroRW }

case class ClimographDTOFigure(
  @key("name") name: String,
  @key("color") color: String
)
object ClimographDTOFigure { implicit val rw: ReadWriter[ClimographDTOFigure] = macroRW }

case class ClimographDTOMargin(
  @key("left") left: Float,
  @key("right") right: Float,
  @key("top") top: Float,
  @key("bottom") bottom: Float
)
object ClimographDTOMargin { implicit val rw: ReadWriter[ClimographDTOMargin] = macroRW }

case class ClimographDTOLegend(
  @key("y_offset") yOffset: Float
)
object ClimographDTOLegend { implicit val rw: ReadWriter[ClimographDTOLegend] = macroRW }

case class ClimographDTOStyle(
  @key("lettering") lettering: ClimographDTOLettering,
  @key("figure_temp") figureTemp: ClimographDTOFigure,
  @key("figure_prec") figurePrec: ClimographDTOFigure,
  @key("margin") margin: Option[ClimographDTOMargin],
  @key("legend") legend: Option[ClimographDTOLegend]
)
object ClimographDTOStyle { implicit val rw: ReadWriter[ClimographDTOStyle] = macroRW }

case class ClimographDTO (
  @key("src") src: ClimographDTOSrc, 
  @key("dest") dest: ClimographDTODest, 
  @key("style") style: ClimographDTOStyle
)
object ClimographDTO { implicit val rw: ReadWriter[ClimographDTO] = macroRW }
