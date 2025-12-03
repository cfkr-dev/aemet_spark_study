package Utils.Storage.Core

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._

import java.net.URI
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.util.UUID
import scala.jdk.CollectionConverters.CollectionHasAsScala

object S3StorageBackend {

  private val tempDirBase: Path = Paths.get(System.getProperty("java.io.tmpdir"))

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

  private def tempPathForKey(key: String): Path = {
    val extension = {
      val name = new java.io.File(key).getName
      val dotIndex = name.lastIndexOf(".")
      if (dotIndex != -1) name.substring(dotIndex) else ""
    }

    val fileName = s"s3reader-${UUID.randomUUID()}$extension"

    tempDirBase.resolve(fileName)
  }

  def read(bucket: String, key: String, endpoint: Option[String] = None): Path = {
    val client: S3Client = endpoint match {
      case Some(uri) => createDummyClient(uri)
      case None => S3Client.create()
    }

    val tmp = tempPathForKey(key)

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

  def readDirectoryRecursive(
    bucket: String,
    key: String,
    endpoint: Option[String] = None,
    includeDirs: Seq[String]
  ): Path = {

    // Crear cliente S3
    val client = endpoint match {
      case Some(uri) => S3StorageBackend.createDummyClient(uri)
      case None     => S3Client.create()
    }

    // Normalizar key y crear tempDir raíz
    val normalizedKey = key.stripSuffix("/")
    val rootName = Paths.get(normalizedKey).getFileName.toString
    val tempDirBasePath = Files.createTempDirectory(tempDirBase, null)
    val tempDir = tempDirBasePath.resolve(rootName)
    Files.createDirectories(tempDir)

    // Normalizar includeDirs → relativo a la raíz del key
    val includePaths: Set[String] = includeDirs.map { p =>
      val normalized = p.replace('\\', '/').stripPrefix("/").stripSuffix("/")
      val parts = normalized.split("/").toList

      parts match {
        case head :: tail if head == rootName =>
          tail.mkString("/") // solo la ruta dentro de la raíz
        case _ =>
          throw new IllegalArgumentException(
            s"includeDir must start with root directory '$rootName': $p"
          )
      }
    }.toSet

    // Función recursiva para listar y descargar
    def listAndDownload(continuationToken: Option[String]): Unit = {
      val reqBuilder = ListObjectsV2Request.builder()
        .bucket(bucket)
        .prefix(normalizedKey)

      continuationToken.foreach(reqBuilder.continuationToken)
      val resp = client.listObjectsV2(reqBuilder.build())

      resp.contents().asScala.foreach { obj =>
        // Calcular ruta relativa respecto a la raíz del key
        val relative = obj.key()
          .stripPrefix(normalizedKey)
          .stripPrefix("/")
          .replace("/", java.io.File.separator)

        if (relative.nonEmpty) {
          val includeThis =
            includePaths.isEmpty ||
              includePaths.exists(dir => relative.replace("\\", "/").startsWith(dir))

          if (includeThis) {
            val targetPath = tempDir.resolve(relative)
            Option(targetPath.getParent).foreach(Files.createDirectories(_))

            val tmpFile = S3StorageBackend.read(bucket, obj.key(), endpoint)
            Files.copy(tmpFile, targetPath, StandardCopyOption.REPLACE_EXISTING)
          }
        }
      }

      Option(resp.nextContinuationToken()).foreach(token => listAndDownload(Some(token)))
    }

    listAndDownload(None)
    tempDir
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