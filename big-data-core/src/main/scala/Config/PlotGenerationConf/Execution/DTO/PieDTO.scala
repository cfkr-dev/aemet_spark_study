package Config.PlotGenerationConf.Execution.DTO

import upickle.default.{ReadWriter, macroRW}
import upickle.implicits.key


case class PieDTOSrcNamesComponent(
  @key("lower_bound") lowerBound: String,
  @key("upper_bound") upperBound: String,
  @key("value") value: String
)
object PieDTOSrcNamesComponent { implicit val rw: ReadWriter[PieDTOSrcNamesComponent] = macroRW }

case class PieDTOSrc(
  @key("path") path: String,
  @key("names") names: PieDTOSrcNamesComponent
)
object PieDTOSrc { implicit val rw: ReadWriter[PieDTOSrc] = macroRW }

case class PieDTODest(
  @key("path") path: String,
  @key("filename") filename: String,
  @key("export_png") exportPng: Boolean
)
object PieDTODest { implicit val rw: ReadWriter[PieDTODest] = macroRW }

case class PieDTOLettering(
  @key("title") title: String,
  @key("subtitle") subtitle: String
)
object PieDTOLettering { implicit val rw: ReadWriter[PieDTOLettering] = macroRW }

case class PieDTOMargin(
  @key("left") left: Float,
  @key("right") right: Float,
  @key("top") top: Float,
  @key("bottom") bottom: Float
)
object PieDTOMargin { implicit val rw: ReadWriter[PieDTOMargin] = macroRW }

case class PieDTOStyle(
  @key("lettering") lettering: PieDTOLettering,
  @key("margin") margin: Option[PieDTOMargin],
  @key("show_legend") showLegend: Boolean
)
object PieDTOStyle { implicit val rw: ReadWriter[PieDTOStyle] = macroRW }

case class PieDTO (
  @key("src") src: PieDTOSrc, 
  @key("dest") dest: PieDTODest, 
  @key("style") style: PieDTOStyle
)
object PieDTO { implicit val rw: ReadWriter[PieDTO] = macroRW }
