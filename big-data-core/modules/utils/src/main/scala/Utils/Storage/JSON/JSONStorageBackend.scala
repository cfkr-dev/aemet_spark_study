package Utils.Storage.JSON

import Utils.Storage.Core.Storage
import ujson.Value

import java.nio.file.{Files, Path}

/**
 * Simple JSON storage helpers that delegate to an implicit `Storage` backend.
 *
 * Methods return `Either[Exception, T]` to propagate IO errors without throwing
 * directly (the underlying `Storage` may still throw for local operations).
 */
object JSONStorageBackend {

  /**
   * Read a JSON file from the configured `Storage` and parse it into `ujson.Value`.
   *
   * @param path storage path to read
   * @param storage implicit `Storage` implementation
   * @return `Right(Value)` on success or `Left(Exception)` on failure
   */
  def readJSON(path: String)(implicit storage: Storage): Either[Exception, Value] = {
    try {
      val tmp: Path = storage.read(path)
      val content = Files.readString(tmp)
      Right(ujson.read(content))
    } catch {
      case ex: Exception => Left(ex)
    }
  }

  /**
   * Write a `ujson.Value` to the configured `Storage` at `path`.
   *
   * @param path destination storage path
   * @param json `ujson.Value` to write
   * @param storage implicit `Storage` implementation
   * @return `Right(path)` on success or `Left(Exception)` on failure
   */
  def writeJSON(path: String, json: Value)(implicit storage: Storage): Either[Exception, String] = {
    try {
      val tmp: Path = Files.createTempFile("jsonwriter", ".json")
      Files.writeString(tmp, ujson.write(json, indent = 2))
      storage.write(path, tmp)
      Right(path)
    } catch {
      case ex: Exception => Left(ex)
    }
  }

  /**
   * Copy a JSON file from `srcPath` to `destPath` using the configured `Storage`.
   *
   * @param srcPath source storage path
   * @param destPath destination storage path
   * @param storage implicit `Storage` implementation
   * @return `Right(destPath)` on success or `Left(Exception)` on failure
   */
  def copyJSON(srcPath: String, destPath: String)(implicit storage: Storage): Either[Exception, String] = {
    try {
      storage.copy(srcPath, destPath)
      Right(destPath)
    } catch {
      case ex: Exception => Left(ex)
    }
  }


}
