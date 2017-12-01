package io.ampool.monarch.table.exceptions;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;

/**
 * An exception indicating problem occured during executing filters.
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public class MFilterException extends MException {

  private static final long serialVersionUID = -1516686150339626543L;

  /**
   * Create a new instance of MFilterException with a detail message
   * 
   * @param message the detail message
   */
  public MFilterException(String message) {
    super(message);
  }
}
