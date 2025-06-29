package Config.PlotGenerationConf.Execution.DTO


case class AxisComponent(
  name: String,
  format: Option[String]
)

case class Axis(
  x: AxisComponent,
  y: AxisComponent
)

case class Src(
  path: String,
  axis: Axis
)

case class Dest(
  path: String,
  filename: String,
  exportPng: Boolean
)

case class Lettering(
  title: String,
  subtitle: String,
  xLabel: String,
  yLabel: String
)

case class Figure(
  name: String,
  color: String
)

case class Margin(
  left: Float,
  right: Float,
  top: Float,
  bottom: Float
)

case class Legend(
  yOffset: Float
)

case class Style(
  lettering: Lettering,
  figure: Figure,
  margin: Option[Margin],
  legend: Option[Legend]
)

case class LinearDTO (src: Src, dest: Dest, style: Style)