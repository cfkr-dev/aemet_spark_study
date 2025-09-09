package Config.PlotGenerationConf.Execution.DTO

import upickle.default.{ReadWriter, macroRW}
import upickle.implicits.key

case class DoubleLinearDTOAxisComponent(
  @key("name") name: String,
  @key("format") format: Option[String]
)
object DoubleLinearDTOAxisComponent { implicit val rw: ReadWriter[DoubleLinearDTOAxisComponent] = macroRW }

case class DoubleLinearDTOAxis(
  @key("x") x: DoubleLinearDTOAxisComponent,
  @key("y1") y1: DoubleLinearDTOAxisComponent,
  @key("y2") y2: DoubleLinearDTOAxisComponent
)
object DoubleLinearDTOAxis { implicit val rw: ReadWriter[DoubleLinearDTOAxis] = macroRW }

case class DoubleLinearDTOSrc(
  @key("path") path: String,
  @key("axis") axis: DoubleLinearDTOAxis
)
object DoubleLinearDTOSrc { implicit val rw: ReadWriter[DoubleLinearDTOSrc] = macroRW }

case class DoubleLinearDTODest(
  @key("path") path: String,
  @key("filename") filename: String,
  @key("export_png") exportPng: Boolean
)
object DoubleLinearDTODest { implicit val rw: ReadWriter[DoubleLinearDTODest] = macroRW }

case class DoubleLinearDTOLettering(
  @key("title") title: String,
  @key("subtitle") subtitle: String,
  @key("x_label") xLabel: String,
  @key("y1_label") y1Label: String,
  @key("y2_label") y2Label: String
)
object DoubleLinearDTOLettering { implicit val rw: ReadWriter[DoubleLinearDTOLettering] = macroRW }

case class DoubleLinearDTOFigure(
  @key("name") name: String,
  @key("color") color: String
)
object DoubleLinearDTOFigure { implicit val rw: ReadWriter[DoubleLinearDTOFigure] = macroRW }

case class DoubleLinearDTOMargin(
  @key("left") left: Float,
  @key("right") right: Float,
  @key("top") top: Float,
  @key("bottom") bottom: Float
)
object DoubleLinearDTOMargin { implicit val rw: ReadWriter[DoubleLinearDTOMargin] = macroRW }

case class DoubleLinearDTOLegend(
  @key("y_offset") yOffset: Float
)
object DoubleLinearDTOLegend { implicit val rw: ReadWriter[DoubleLinearDTOLegend] = macroRW }

case class DoubleLinearDTOStyle(
  @key("lettering") lettering: DoubleLinearDTOLettering,
  @key("figure_1") figure1: DoubleLinearDTOFigure,
  @key("figure_2") figure2: DoubleLinearDTOFigure,
  @key("margin") margin: Option[DoubleLinearDTOMargin],
  @key("legend") legend: Option[DoubleLinearDTOLegend]
)
object DoubleLinearDTOStyle { implicit val rw: ReadWriter[DoubleLinearDTOStyle] = macroRW }

case class DoubleLinearDTO (
  @key("src") src: DoubleLinearDTOSrc, 
  @key("dest") dest: DoubleLinearDTODest, 
  @key("style") style: DoubleLinearDTOStyle
)
object DoubleLinearDTO { implicit val rw: ReadWriter[DoubleLinearDTO] = macroRW }
