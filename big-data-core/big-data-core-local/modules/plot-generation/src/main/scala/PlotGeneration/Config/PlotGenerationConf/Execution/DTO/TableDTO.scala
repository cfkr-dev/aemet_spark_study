package PlotGeneration.Config.PlotGenerationConf.Execution.DTO

import upickle.default.{ReadWriter, macroRW}
import upickle.implicits.key


case class TableDTOSrc(
  @key("path") path: String,
  @key("col_names") colNames: List[String]
)
object TableDTOSrc { implicit val rw: ReadWriter[TableDTOSrc] = macroRW }

case class TableDTODest(
  @key("path") path: String,
  @key("filename") filename: String,
  @key("export_png") exportPng: Boolean
)
object TableDTODest { implicit val rw: ReadWriter[TableDTODest] = macroRW }

case class TableDTOLettering(
  @key("title") title: String,
  @key("subtitle") subtitle: String,
  @key("headers") headers: List[String]
)
object TableDTOLettering { implicit val rw: ReadWriter[TableDTOLettering] = macroRW }

case class TableDTOFigureComponent(
  @key("align") align: String,
  @key("color") color: String
)
object TableDTOFigureComponent { implicit val rw: ReadWriter[TableDTOFigureComponent] = macroRW }

case class TableDTOFigure(
  @key("headers") headers: TableDTOFigureComponent,
  @key("cells") cells: TableDTOFigureComponent
)
object TableDTOFigure { implicit val rw: ReadWriter[TableDTOFigure] = macroRW }

case class TableDTOMargin(
  @key("left") left: Float,
  @key("right") right: Float,
  @key("top") top: Float,
  @key("bottom") bottom: Float
)
object TableDTOMargin { implicit val rw: ReadWriter[TableDTOMargin] = macroRW }

case class TableDTOStyle(
  @key("lettering") lettering: TableDTOLettering,
  @key("figure") figure: TableDTOFigure,
  @key("margin") margin: Option[TableDTOMargin]
)
object TableDTOStyle { implicit val rw: ReadWriter[TableDTOStyle] = macroRW }

case class TableDTO(
  @key("src") src: TableDTOSrc, 
  @key("dest") dest: TableDTODest, 
  @key("style") style: TableDTOStyle
)
object TableDTO { implicit val rw: ReadWriter[TableDTO] = macroRW }
