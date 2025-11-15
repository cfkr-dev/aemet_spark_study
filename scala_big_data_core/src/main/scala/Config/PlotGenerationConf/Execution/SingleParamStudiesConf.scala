package Config.PlotGenerationConf.Execution

import Config.PlotGenerationConf.Execution.DTO.{BarDTO, LinearDTO}

case class StudyParamsValues(
  studyParamName: String,
  studyParamAbbrev: String,
  studyParamUnit: String,
  colAggMethod: String
)

case class StateValues(
  stateName: String,
  stateNameNoSc: String
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

case class Evol2024(
  uri: String,
  body: LinearDTO
)

case class SingleParamStudiesConf(
  studyParamsValues: List[StudyParamsValues],
  stateValues: List[StateValues],
  top10Values: Top10Values,
  top10: Top10,
  top5IncValues: Top5IncValues,
  top5Inc: Top5Inc,
  evol2024: Evol2024
)