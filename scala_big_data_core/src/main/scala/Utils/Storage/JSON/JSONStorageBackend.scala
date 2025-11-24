package Utils.Storage.JSON

import Utils.Storage.Core.Storage
import ujson.Value

import java.nio.file.{Files, Path}

object JSONStorageBackend {

  def readJSON(path: String)(implicit storage: Storage): Either[Exception, Value] = {
    try {
      val tmp: Path = storage.read(path)
      val content = Files.readString(tmp)
      Right(ujson.read(content))
    } catch {
      case ex: Exception => Left(ex)
    }
  }

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

  def copyJSON(srcPath: String, destPath: String)(implicit storage: Storage): Either[Exception, String] = {
    try {
      storage.copy(srcPath, destPath)
      Right(destPath)
    } catch {
      case ex: Exception => Left(ex)
    }
  }



}
