package DataExtraction.Config.DataExtractionConf.Execution


case class IfapaSingleStationMeteoInfo(startDate: String, startDateAltFormat: String, endDate: String, endDateAltFormat: String, stateCode: String, stationCode: String)

case class IfapaSingleStationInfo(stateCode: String, stationCode: String)

case class IfapaSingleStateInfo(stateCode: String)

case class IfapaApiResources(singleStationMeteoInfo: IfapaSingleStationMeteoInfo, singleStationInfo: IfapaSingleStationInfo, singleStateInfo: IfapaSingleStateInfo)


case class IfapaResponse(metadata: String)

case class IfapaMetadata(singleStationMeteoInfo: String, singleStationInfo: String, fieldProperties: String)

case class IfapaReqResp(response: IfapaResponse, metadata: IfapaMetadata)


case class IfapaConf(apiResources: IfapaApiResources, reqResp: IfapaReqResp)
