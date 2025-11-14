package Config.PlotGenerationConf.Execution

import Config.PlotGenerationConf.Execution.DTO.BarDTO

case class StudyParamsValues(
  studyParamName: String,
  studyParamAbbrev: String,
  studyParamUnit: String,
  colAggMethod: String
)

case class Top10ValuesTemporal(
  value: String,
  title: String
)

case class Top10Values(
  order: List[String],
  temporal: List[Top10ValuesTemporal]
)

case class Top10(
  uri: String,
  body: BarDTO
)

case class Top5IncValues(
  order: List[String]
)

case class Top5Inc(
  uri: String,
  body: BarDTO
)

case class SingleParamStudiesConf(
  studyParamsValues: List[StudyParamsValues],
  top10Values: Top10Values,
  top10: Top10,
  top5IncValues: Top5IncValues,
  top5Inc: Top5Inc
)