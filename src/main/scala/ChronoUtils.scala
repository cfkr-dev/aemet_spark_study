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

  def awaitAndExecute(millis: Long)(execute: => Unit): Unit = {
    Thread.sleep(Constants.minimumMillisBetweenRequestMetadata)

    execute
  }
}
