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
}
