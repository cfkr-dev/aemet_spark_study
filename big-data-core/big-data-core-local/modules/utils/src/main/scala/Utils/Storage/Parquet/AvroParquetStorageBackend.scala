package Utils.Storage.Parquet

import Utils.Storage.Core.Storage
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopOutputFile

import java.nio.file.{Path, Paths}
import java.util.UUID
import scala.util.Using

/**
 * Helper to create a temporary Avro Parquet file using a provided `Schema` and
 * a write action that receives a `ParquetWriter[GenericRecord]`.
 *
 * The resulting temporary file is written to the configured `Storage` using
 * the implicit `Storage` instance.
 */
object AvroParquetStorageBackend {
  /**
   * Create a Parquet file using the given `schema` and `writeAction`, then
   * store it in the configured `Storage` under `path`.
   *
   * @param path destination storage path
   * @param schema Avro `Schema` for the Parquet file
   * @param writeAction function that receives a `ParquetWriter[GenericRecord]` and writes records
   * @param storage implicit `Storage` backend used to persist the temporary file
   * @return `Right(path)` on success or `Left(Exception)` on failure
   */
  def withAvroParquetWriter(
    path: String,
    schema: Schema
  )(writeAction: ParquetWriter[GenericRecord] => Unit)(implicit storage: Storage): Either[Exception, String] = {

    try {
      val tmpDir: Path =
        Paths.get(System.getProperty("java.io.tmpdir"))

      val tmpFile: Path =
        tmpDir.resolve(s"parquetwriter_${UUID.randomUUID()}.parquet")

      val outputFile = HadoopOutputFile.fromPath(
        new org.apache.hadoop.fs.Path(tmpFile.toAbsolutePath.toString),
        new Configuration()
      )

      Using.resource(
        AvroParquetWriter.builder[GenericRecord](outputFile)
          .withSchema(schema)
          .withCompressionCodec(CompressionCodecName.SNAPPY)
          .build()
      ) { writer =>
        writeAction(writer)
      }

      storage.write(path, tmpFile)
      Right(path)
    } catch {
      case ex: Exception => Left(ex)
    }
  }
}