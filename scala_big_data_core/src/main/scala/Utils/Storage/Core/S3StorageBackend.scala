package Utils.Storage.Core

import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region

import java.io.File
import java.nio.file.{Path, Paths}
import java.net.URI

object S3StorageBackend {

  private val tempDir: Path = Paths.get(System.getProperty("java.io.tmpdir"))

  private def createDummyClient(endpoint: String): S3Client = {
    S3Client.builder()
      .region(Region.US_EAST_1)
      .endpointOverride(URI.create(endpoint))
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))
      )
      .forcePathStyle(true)
      .build()
  }

  def read(bucket: String, key: String, endpoint: Option[String] = None): Path = {
    val client: S3Client = endpoint match {
      case Some(uri) => createDummyClient(uri)
      case None => S3Client.create()
    }
    val tmp = tempDir.resolve(new File(key).getName)

    val req = GetObjectRequest.builder()
      .bucket(bucket)
      .key(key)
      .build()

    try {
      client.getObject(req, tmp)
    } catch {
      case exception: Exception => throw new S3OperationException(bucket, key, exception)
    }
    tmp
  }

  def write(bucket: String, key: String, localPath: Path, endpoint: Option[String] = None): Unit = {
    val client: S3Client = endpoint match {
      case Some(uri) => createDummyClient(uri)
      case None => S3Client.create()
    }
    val req = PutObjectRequest.builder()
      .bucket(bucket)
      .key(key)
      .build()

    try {
      client.putObject(req, localPath)
    } catch {
      case exception: Exception => throw new S3OperationException(bucket, key, exception)
    }
  }
}