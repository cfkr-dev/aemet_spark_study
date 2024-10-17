import sttp.model.Uri
import fansi.{Color, _}

object EnhancedConsoleLog {
  object Method {
    def printlnGet(uri: Uri): Unit = println(Color.Yellow("GET").overlay(Bold.On) ++ " => " ++ Color.LightMagenta(uri.toString()).overlay(Underlined.On))
  }

  object Response {
    def println200OK(uri: Uri): Unit = println(Color.LightMagenta(uri.toString()).overlay(Underlined.On) ++ " => " ++ Color.Green("200 OK").overlay(Bold.On))
    def println404NotFound(uri: Uri): Unit = println(Color.LightMagenta(uri.toString()).overlay(Underlined.On) ++ " => " ++ Color.Red("404 NOT FOUND").overlay(Bold.On))
    def println500ServerError(uri: Uri): Unit = println(Color.LightMagenta(uri.toString()).overlay(Underlined.On) ++ " => " ++ Color.Red("500 INTERNAL SERVER ERROR").overlay(Bold.On))
  }
}
