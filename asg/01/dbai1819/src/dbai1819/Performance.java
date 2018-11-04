package dbai1819;

/**
 * This class contains methods for performance measurements.
 *
 * @author Christian Gruen, DBIS, University of Konstanz
 */
public final class Performance {
  /** Performance timer, using nano seconds. */
  private long time = System.nanoTime();

  /**
   * Returns the measured execution time in nanoseconds.
   * @return execution time
   */
  public long getTime() {
    final long time2 = System.nanoTime();
    final long diff = time2 - time;
    time = time2;
    return diff;
  }

  /**
   * Returns the measured execution time in milliseconds and reinitializes
   * the timer.
   * @return execution time
   */
  public String getTimer() {
    return getTimer(1);
  }

  /**
   * Returns the measured execution time in milliseconds, divided by the
   * number of runs, and reinitializes the timer.
   * @param runs number of runs
   * @return execution time
   */
  public String getTimer(final int runs) {
    return getTimer(getTime(), runs);
  }

  /**
   * Returns a string with the measured execution time in milliseconds.
   * @param time measured time in nanoseconds
   * @param runs number of runs
   * @return execution time
   */
  public static String getTimer(final long time, final int runs) {
    return time / runs / 10000 / 100d + " ms" + (runs > 1 ? " (avg)" : "");
  }

  /**
   * Returns a formatted representation of the current memory consumption.
   * @return memory consumption
   */
  public static String getMem() {
    final Runtime rt = Runtime.getRuntime();
    final long mem = rt.totalMemory() - rt.freeMemory();
    return format(mem);
  }

  /**
   * Formats a file size according to the binary size orders (KB, MB, ...).
   * @param size file size
   * @return formatted size value
   */
  public static String format(final long size) {
    if(size > 1L << 30) return (size + (1L << 29) >> 30) + " GB";
    if(size > 1L << 20) return (size + (1L << 19) >> 20) + " MB";
    if(size > 1L << 10) return (size + (1L <<  9) >> 10) + " KB";
    return size + " Bytes";
  }

  /**
   * Performs some garbage collection.
   * GC behavior in Java is a pretty complex task. Still, garbage collection
   * can be forced by calling it several times.
   * @param n number of times to execute garbage collection
   */
  public static void gc(final int n) {
    for(int i = 0; i < n; ++i) System.gc();
  }

  @Override
  public String toString() {
    return getTimer();
  }
}
