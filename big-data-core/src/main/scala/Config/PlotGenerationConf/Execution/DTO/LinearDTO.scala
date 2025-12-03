package Config.PlotGenerationConf.Execution.DTO

import upickle.default.{ReadWriter, macroRW}
import upickle.implicits.key

case class LinearDTOAxisComponent(
  @key("name") name: String,
  @key("format") format: Option[String]
)
object LinearDTOAxisComponent { implicit val rw: ReadWriter[LinearDTOAxisComponent] = macroRW }

case class LinearDTOAxis(
  @key("x") x: LinearDTOAxisComponent,
  @key("y") y: LinearDTOAxisComponent
)
object LinearDTOAxis { implicit val rw: ReadWriter[LinearDTOAxis] = macroRW }

case class LinearDTOSrc(
  @key("path") path: String,
  @key("axis") axis: LinearDTOAxis
)
object LinearDTOSrc { implicit val rw: ReadWriter[LinearDTOSrc] = macroRW }

case class LinearDTODest(
  @key("path") path: String,
  @key("filename") filename: String,
  @key("export_png") exportPng: Boolean
)
object LinearDTODest { implicit val rw: ReadWriter[LinearDTODest] = macroRW }

case class LinearDTOLettering(
  @key("title") title: String,
  @key("subtitle") subtitle: String,
  @key("x_label") xLabel: String,
  @key("y_label") yLabel: String
)
object LinearDTOLettering { implicit val rw: ReadWriter[LinearDTOLettering] = macroRW }

case class LinearDTOFigure(
  @key("name") name: String,
  @key("color") color: String
)
object LinearDTOFigure { implicit val rw: ReadWriter[LinearDTOFigure] = macroRW }

case class LinearDTOMargin(
  @key("left") left: Float,
  @key("right") right: Float,
  @key("top") top: Float,
  @key("bottom") bottom: Float
)
object LinearDTOMargin { implicit val rw: ReadWriter[LinearDTOMargin] = macroRW }

case class LinearDTOLegend(
  @key("y_offset") yOffset: Float
)
object LinearDTOLegend { implicit val rw: ReadWriter[LinearDTOLegend] = macroRW }

case class LinearDTOStyle(
  @key("lettering") lettering: LinearDTOLettering,
  @key("figure") figure: LinearDTOFigure,
  @key("margin") margin: Option[LinearDTOMargin],
  @key("legend") legend: Option[LinearDTOLegend]
)
object LinearDTOStyle { implicit val rw: ReadWriter[LinearDTOStyle] = macroRW }

case class LinearDTO (
  @key("src") src: LinearDTOSrc,
  @key("dest") dest: LinearDTODest,
  @key("style") style: LinearDTOStyle
)
object LinearDTO { implicit val rw: ReadWriter[LinearDTO] = macroRW }