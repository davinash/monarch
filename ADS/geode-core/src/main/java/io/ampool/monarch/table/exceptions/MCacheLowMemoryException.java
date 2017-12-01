package io.ampool.monarch.table.exceptions;


import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import org.apache.geode.GemFireException;

/**
 * An exception indicating MTable cache is low in memory.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MCacheLowMemoryException extends MException {

  public MCacheLowMemoryException(String s, GemFireException ge) {
    super(s, ge);
  }

  public MCacheLowMemoryException(String s) {
    super(s);
  }

  public MCacheLowMemoryException(Exception e) {
    super(e);
  }

}
