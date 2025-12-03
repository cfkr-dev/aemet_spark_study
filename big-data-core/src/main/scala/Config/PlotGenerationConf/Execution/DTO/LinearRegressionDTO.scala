package Config.PlotGenerationConf.Execution.DTO

import upickle.default.{ReadWriter, macroRW}
import upickle.implicits.key

case class LinearRegressionDTOAxisComponent(
  @key("name") name: String,
  @key("format") format: Option[String]
)
object LinearRegressionDTOAxisComponent { implicit val rw: ReadWriter[LinearRegressionDTOAxisComponent] = macroRW }

case class LinearRegressionDTOAxis(
  @key("x") x: LinearRegressionDTOAxisComponent,
  @key("y") y: LinearRegressionDTOAxisComponent
)
object LinearRegressionDTOAxis { implicit val rw: ReadWriter[LinearRegressionDTOAxis] = macroRW }

case class LinearRegressionDTOSrcMainComponent(
  @key("path") path: String,
  @key("axis") axis: LinearRegressionDTOAxis
)
object LinearRegressionDTOSrcMainComponent { implicit val rw: ReadWriter[LinearRegressionDTOSrcMainComponent] = macroRW }

case class LinearRegressionDTOSrcRegressionComponentNamesComponent(
  @key("slope") slope: String,
  @key("intercept") intercept: String
)
object LinearRegressionDTOSrcRegressionComponentNamesComponent { implicit val rw: ReadWriter[LinearRegressionDTOSrcRegressionComponentNamesComponent] = macroRW }

case class LinearRegressionDTOSrcRegressionComponent(
  @key("path") path: String,
  @key("names") names: LinearRegressionDTOSrcRegressionComponentNamesComponent
)
object LinearRegressionDTOSrcRegressionComponent { implicit val rw: ReadWriter[LinearRegressionDTOSrcRegressionComponent] = macroRW }

case class LinearRegressionDTOSrc(
  @key("main") main: LinearRegressionDTOSrcMainComponent,
  @key("regression") regression: LinearRegressionDTOSrcRegressionComponent
)
object LinearRegressionDTOSrc { implicit val rw: ReadWriter[LinearRegressionDTOSrc] = macroRW }

case class LinearRegressionDTODest(
  @key("path") path: String,
  @key("filename") filename: String,
  @key("export_png") exportPng: Boolean
)
object LinearRegressionDTODest { implicit val rw: ReadWriter[LinearRegressionDTODest] = macroRW }

case class LinearRegressionDTOLettering(
  @key("title") title: String,
  @key("subtitle") subtitle: String,
  @key("x_label") xLabel: String,
  @key("y_1_label") y1Label: String,
  @key("y_2_label") y2Label: String
)
object LinearRegressionDTOLettering { implicit val rw: ReadWriter[LinearRegressionDTOLettering] = macroRW }

case class LinearRegressionDTOFigure(
  @key("name") name: String,
  @key("color") color: String
)
object LinearRegressionDTOFigure { implicit val rw: ReadWriter[LinearRegressionDTOFigure] = macroRW }

case class LinearRegressionDTOMargin(
  @key("left") left: Float,
  @key("right") right: Float,
  @key("top") top: Float,
  @key("bottom") bottom: Float
)
object LinearRegressionDTOMargin { implicit val rw: ReadWriter[LinearRegressionDTOMargin] = macroRW }

case class LinearRegressionDTOLegend(
  @key("y_offset") yOffset: Float
)
object LinearRegressionDTOLegend { implicit val rw: ReadWriter[LinearRegressionDTOLegend] = macroRW }

case class LinearRegressionDTOStyle(
  @key("lettering") lettering: LinearRegressionDTOLettering,
  @key("figure_1") figure1: LinearRegressionDTOFigure,
  @key("figure_2") figure2: LinearRegressionDTOFigure,
  @key("margin") margin: Option[LinearRegressionDTOMargin],
  @key("legend") legend: Option[LinearRegressionDTOLegend]
)
object LinearRegressionDTOStyle { implicit val rw: ReadWriter[LinearRegressionDTOStyle] = macroRW }

case class LinearRegressionDTO (
  @key("src") src: LinearRegressionDTOSrc, 
  @key("dest") dest: LinearRegressionDTODest, 
  @key("style") style: LinearRegressionDTOStyle
)
object LinearRegressionDTO { implicit val rw: ReadWriter[LinearRegressionDTO] = macroRW }
