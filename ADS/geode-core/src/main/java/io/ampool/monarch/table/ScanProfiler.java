package io.ampool.monarch.table;

import java.util.Map;

import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

/**
 * A wrapper class to instrument various phases of scan. It holds a map of string to long that help
 * measure execution time in specific code-blocks. Unique identifier can be used to mark and measure
 * respective times. In the end it can print the summary of all such recorded sections.
 * <p>
 */
public class ScanProfiler extends Scan {
  private Object2LongOpenHashMap<String> profileData = new Object2LongOpenHashMap<>(10);

  /**
   * Create instrumented scan object.
   *
   * @param scan the scanner object
   */
  public ScanProfiler(final Scan scan) {
    super(scan);
  }

  /**
   * Create instrumented scan object with reference to an existing profile-map.
   *
   * @param scan the scanner object
   * @param profileData the profile-data map
   */
  public ScanProfiler(final Scan scan, final Object2LongOpenHashMap<String> profileData) {
    super(scan);
    this.profileData = profileData;
  }

  /**
   * Get the profiling summary.
   *
   * @return the map of all recorded
   */
  public Object2LongOpenHashMap<String> getProfileData() {
    return profileData;
  }

  /**
   * Increment the measured time for the specified id.
   *
   * @param name the unique id respective to the code-block
   * @param value the time spent in each execution
   */
  public void addMetric(final String name, final long value) {
    profileData.addTo(name, value);
  }

  /**
   * Print summary of time spent in all recorded code-blocks.
   */
  public void printSummary() {
    System.out.println("Time spent in seconds:");
    for (Map.Entry<String, Long> entry : this.profileData.entrySet()) {
      if (entry.getKey().contains("Count")) {
        System.out.printf("Op# %12s= %8s\n", entry.getKey(), (entry.getValue()));
      } else
        System.out.printf("Op# %12s= %8s\n", entry.getKey(),
            (entry.getValue() / 1_000_000) / 1000.0);
    }
  }
}
