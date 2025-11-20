package Utils

object ChronoUtils {
  def executeAndAwaitIfTimeNotExceedMinimum(minimumMillis: Long)(execute: => Unit): Unit = {
    val startTime = System.nanoTime()

    execute

    val endTime = System.nanoTime()

    val lapse = (endTime - startTime) / 1e6

    if (lapse < minimumMillis) {
      Thread.sleep((minimumMillis - lapse).toLong)
    }
  }

  def await(millis: Long): Unit = {
    Thread.sleep(millis)
  }

  case class Chronometer(private var startTime: Long = 0L,
                         private var endTime: Long = 0L,
                         private var running: Boolean = false) {

    def start(): Unit = {
      if (!running) {
        startTime = System.nanoTime()
        running = true
      }
    }

    def stop(): String = {
      if (running) {
        endTime = System.nanoTime()
        running = false
        formatElapsed(endTime - startTime)
      } else {
        "00:00:00.000"
      }
    }

    private def formatElapsed(elapsedNano: Long): String = {
      val totalMillis = elapsedNano / 1_000_000
      val hours = totalMillis / 3_600_000
      val minutes = (totalMillis % 3_600_000) / 60_000
      val seconds = (totalMillis % 60_000) / 1000
      val millis = totalMillis % 1000
      f"$hours%02d:$minutes%02d:$seconds%02d.$millis%03d"
    }
  }
}
