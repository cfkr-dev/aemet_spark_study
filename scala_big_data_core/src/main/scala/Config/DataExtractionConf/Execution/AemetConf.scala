package Config.DataExtractionConf.Execution


case class AemetAllMeteoInfo(startDate: String, endDate: String)


case class AemetApiResources(allMeteoInfo: AemetAllMeteoInfo)


case class AemetLastSavedDates(lastEndDate: String)


case class AemetRequest(apiKey: String)


case class AemetResponse(stateNumber: String, metadata: String, data: String)


case class AemetMetadata(schemaDef: String, fieldId: String, fieldRequired: String)


case class AemetReqResp(
  lastSavedDates: AemetLastSavedDates,
  request: AemetRequest,
  response: AemetResponse,
  metadata: AemetMetadata
)


case class AemetConf(apiResources: AemetApiResources, reqResp: AemetReqResp)
