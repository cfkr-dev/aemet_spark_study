package Utils

/**
 * Utility methods for simple timing and delays.
 *
 * Provides helpers to execute blocks with a minimum elapsed time and a
 * lightweight `Chronometer` for measuring and formatting elapsed time.
 *
 * Example:
 * {{{
 *   val chrono = ChronoUtils.Chronometer()
 *   chrono.start()
 *   // do work
 *   val elapsed = chrono.stop() // returns formatted `String` like "00:01:23.456"
 * }}}
 */
object ChronoUtils {
  /**
   * Execute the given block of code, ensuring that at least the specified
   * minimum time in milliseconds has elapsed. If the code block completes
   * quickly, the current thread will be put to sleep for the remaining time.
   *
   * @param minimumMillis the minimum elapsed time in milliseconds
   * @param execute the code block to execute
   */
  def executeAndAwaitIfTimeNotExceedMinimum(minimumMillis: Long)(execute: => Unit): Unit = {
    val startTime = System.nanoTime()

    execute

    val endTime = System.nanoTime()

    val lapse = (endTime - startTime) / 1e6

    if (lapse < minimumMillis) {
      Thread.sleep((minimumMillis - lapse).toLong)
    }
  }

  /**
   * Sleep for the given number of milliseconds.
   *
   * @param millis number of milliseconds to sleep
   */
  def await(millis: Long): Unit = {
    Thread.sleep(millis)
  }

  /**
   * Simple chronometer for measuring elapsed time.
   *
   * Usage:
   * {{{
   *   val c = Chronometer()
   *   c.start()
   *   // work
   *   val result: String = c.stop()
   * }}}
   *
   * @param startTime internal start time in nanoseconds (used when constructed manually)
   * @param endTime internal end time in nanoseconds (used when constructed manually)
   * @param running whether the chronometer is currently running
   */
  case class Chronometer(private var startTime: Long = 0L,
                         private var endTime: Long = 0L,
                         private var running: Boolean = false) {

    /**
     * Start the chronometer. Subsequent calls while running are ignored.
     */
    def start(): Unit = {
      if (!running) {
        startTime = System.nanoTime()
        running = true
      }
    }

    /**
     * Stop the chronometer and return a formatted elapsed time string.
     *
     * @return formatted elapsed time as `String` in `HH:MM:SS.mmm` format
     */
    def stop(): String = {
      if (running) {
        endTime = System.nanoTime()
        running = false
        formatElapsed(endTime - startTime)
      } else {
        "00:00:00.000"
      }
    }

    /**
     * Format a duration given in nanoseconds into a human-readable
     * `HH:MM:SS.mmm` string.
     *
     * @param elapsedNano elapsed time in nanoseconds
     * @return formatted time string as `String` (e.g. "00:01:23.456")
     */
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
