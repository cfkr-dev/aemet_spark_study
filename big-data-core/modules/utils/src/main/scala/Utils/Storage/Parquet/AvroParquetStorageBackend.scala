package Utils.Storage.Parquet

import Utils.Storage.Core.Storage
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopOutputFile

import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import scala.util.Using

object AvroParquetStorageBackend {
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