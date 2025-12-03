package Config.PlotGenerationConf.Execution.DTO

import upickle.default.{ReadWriter, macroRW}
import upickle.implicits.key

case class BarDTOAxisComponent(
  @key("name") name: String,
  @key("format") format: Option[String]
)
object BarDTOAxisComponent { implicit val rw: ReadWriter[BarDTOAxisComponent] = macroRW }

case class BarDTOAxis(
  @key("x") x: BarDTOAxisComponent,
  @key("y") y: BarDTOAxisComponent
)
object BarDTOAxis { implicit val rw: ReadWriter[BarDTOAxis] = macroRW }

case class BarDTOSrc(
  @key("path") path: String,
  @key("axis") axis: BarDTOAxis
)
object BarDTOSrc { implicit val rw: ReadWriter[BarDTOSrc] = macroRW }

case class BarDTODest(
  @key("path") path: String,
  @key("filename") filename: String,
  @key("export_png") exportPng: Boolean
)
object BarDTODest { implicit val rw: ReadWriter[BarDTODest] = macroRW }

case class BarDTOLetteringInsideInfoComponentBuildComponent(
  @key("text_before") textBefore: String,
  @key("name") name: String,
  @key("text_after") textAfter: String
)
object BarDTOLetteringInsideInfoComponentBuildComponent { implicit val rw: ReadWriter[BarDTOLetteringInsideInfoComponentBuildComponent] = macroRW }

case class BarDTOLetteringInsideInfoComponent(
  @key("label") label: String,
  @key("build") build: List[BarDTOLetteringInsideInfoComponentBuildComponent]
)
object BarDTOLetteringInsideInfoComponent { implicit val rw: ReadWriter[BarDTOLetteringInsideInfoComponent] = macroRW }

case class BarDTOLettering(
  @key("title") title: String,
  @key("subtitle") subtitle: String,
  @key("x_label") xLabel: String,
  @key("y_label") yLabel: String,
  @key("inside_info") insideInfo: Option[List[BarDTOLetteringInsideInfoComponent]]
)
object BarDTOLettering { implicit val rw: ReadWriter[BarDTOLettering] = macroRW }

case class BarDTOFigure(
  @key("inverted_horizontal_axis") invertedHorizontalAxis: Boolean,
  @key("threshold_limit_max_min") thresholdLimitMaxMin: Float,
  @key("threshold_perc_limit_outside_text") thresholdPercLimitOutsideText: Float,
  @key("range_margin_perc") rangeMarginPerc: Float,
  @key("color") color: String
)
object BarDTOFigure { implicit val rw: ReadWriter[BarDTOFigure] = macroRW }

case class BarDTOMargin(
  @key("left") left: Float,
  @key("right") right: Float,
  @key("top") top: Float,
  @key("bottom") bottom: Float
)
object BarDTOMargin { implicit val rw: ReadWriter[BarDTOMargin] = macroRW }

case class BarDTOStyle(
  @key("lettering") lettering: BarDTOLettering,
  @key("figure") figure: BarDTOFigure,
  @key("margin") margin: Option[BarDTOMargin]
)
object BarDTOStyle { implicit val rw: ReadWriter[BarDTOStyle] = macroRW }

case class BarDTO (
  @key("src") src: BarDTOSrc,
  @key("dest") dest: BarDTODest,
  @key("style") style: BarDTOStyle
)
object BarDTO { implicit val rw: ReadWriter[BarDTO] = macroRW }
