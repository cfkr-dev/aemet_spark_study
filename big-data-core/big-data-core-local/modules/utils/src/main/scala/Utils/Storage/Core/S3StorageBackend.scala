package Utils.Storage.Core

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._

import java.net.URI
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.util.UUID
import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
 * S3 storage backend utilities. Supports reading single objects, recursive
 * directory-like downloads and writing objects. When `endpoint` is provided
 * a dummy client with static credentials is created (useful for localstack).
 */
object S3StorageBackend {

  private val tempDirBase: Path = Paths.get(System.getProperty("java.io.tmpdir"))

  /**
   * Create a lightweight S3 client pointing to a custom `endpoint` (used for testing/localstack).
   *
   * @param endpoint endpoint URL string
   * @return configured `S3Client`
   */
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

  /**
   * Create a temporary path for a given S3 `key`, preserving file extension.
   *
   * @param key S3 object key
   * @return temporary `Path` on the local filesystem
   */
  private def tempPathForKey(key: String): Path = {
    val extension = {
      val name = new java.io.File(key).getName
      val dotIndex = name.lastIndexOf(".")
      if (dotIndex != -1) name.substring(dotIndex) else ""
    }

    val fileName = s"s3reader-${UUID.randomUUID()}$extension"

    tempDirBase.resolve(fileName)
  }

  /**
   * Read an object from S3 into a temporary `Path` and return it.
   *
   * @param bucket S3 bucket name
   * @param key S3 object key
   * @param endpoint optional custom endpoint (localstack/test)
   * @return temporary `Path` where the downloaded object is stored
   * @throws S3OperationException on download errors
   */
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

  /**
   * Recursively download a subset of an S3 "directory" (prefix) to a temporary directory.
   * The `includeDirs` elements must start with the root folder name and are used to
   * filter which keys are downloaded.
   *
   * @param bucket S3 bucket name
   * @param key prefix representing the root key to download
   * @param endpoint optional custom endpoint
   * @param includeDirs sequence of include paths (must start with the root name)
   * @return temporary `Path` containing the downloaded subset
   */
  def readDirectoryRecursive(
    bucket: String,
    key: String,
    endpoint: Option[String] = None,
    includeDirs: Seq[String]
  ): Path = {

    val client = endpoint match {
      case Some(uri) => S3StorageBackend.createDummyClient(uri)
      case None     => S3Client.create()
    }

    val normalizedKey = key.stripSuffix("/")
    val rootName = Paths.get(normalizedKey).getFileName.toString
    val tempDirBasePath = Files.createTempDirectory(tempDirBase, null)
    val tempDir = tempDirBasePath.resolve(rootName)
    Files.createDirectories(tempDir)

    val includePaths: Set[String] = includeDirs.map { p =>
      val normalized = p.replace('\\', '/').stripPrefix("/").stripSuffix("/")
      val parts = normalized.split("/").toList

      parts match {
        case head :: tail if head == rootName =>
          tail.mkString("/")
        case _ =>
          throw new IllegalArgumentException(
            s"includeDir must start with root directory '$rootName': $p"
          )
      }
    }.toSet

    /**
     * Recursive helper that lists objects using `ListObjectsV2` and downloads
     * each matching object into the temporary directory.
     *
     * @param continuationToken optional continuation token for paged listing
     */
    def listAndDownload(continuationToken: Option[String]): Unit = {
      val reqBuilder = ListObjectsV2Request.builder()
        .bucket(bucket)
        .prefix(normalizedKey)

      continuationToken.foreach(reqBuilder.continuationToken)
      val resp = client.listObjectsV2(reqBuilder.build())

      resp.contents().asScala.foreach { obj =>
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

  /**
   * Write a local file to S3 under the provided `key`.
   *
   * @param bucket S3 bucket name
   * @param key S3 object key
   * @param localPath local file `Path` to upload
   * @param endpoint optional custom endpoint
   */
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