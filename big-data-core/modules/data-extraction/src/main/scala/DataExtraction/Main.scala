package DataExtraction

import DataExtraction.Core.Aemet.AemetAPIClient
import DataExtraction.Core.DataReformatter
import DataExtraction.Core.Ifapa.{IfapaAPIClient, IfapaToAemetConverter}

/**
 * Main application entry point for the DataExtraction module.
 *
 * This object extends App and orchestrates the full extraction and
 * transformation pipeline by invoking the following steps in sequence:
 *
 * 1. `AemetAPIClient.aemetDataExtraction()` - fetches metadata and
 *    meteorological data from the AEMET API and persists it to the
 *    configured storage backend.
 * 2. `IfapaAPIClient.ifapaDataExtraction()` - fetches data from IFAPA and
 *    persists it.
 * 3. `IfapaToAemetConverter.ifapaToAemetConversion()` - converts IFAPA data
 *    to the AEMET-compatible format.
 * 4. `DataReformatter.reformatData()` - performs final reformatting and
 *    normalization of persisted data for downstream consumption.
 *
 * Notes:
 * - The steps are executed sequentially and synchronously; any exception
 *   thrown by a step will stop subsequent steps (exceptions are not caught
 *   here). Consider wrapping calls if you need more resilient behavior.
 * - This object is designed for batch execution (e.g., scheduled runs or
 *   manual invocation). For long-running or concurrent usage, adapt the
 *   orchestration strategy accordingly.
 */
object Main extends App {
  AemetAPIClient.aemetDataExtraction()
  IfapaAPIClient.ifapaDataExtraction()
  IfapaToAemetConverter.ifapaToAemetConversion()
  DataReformatter.reformatData()
}
