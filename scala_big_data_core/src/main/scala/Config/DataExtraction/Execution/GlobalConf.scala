package Config.DataExtraction.Execution


case class DelayTimes(requestSimple: Int, requestMetadata: Int)


case class HttpHeaders(acceptJson: (String, String), userAgent: (String, String))


case class GlobalConf(delayTimes: DelayTimes, httpHeaders: HttpHeaders)
