package org.apache.geode.internal.cache;

import java.io.Serializable;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;

/**
 * 
 * @author ashishtadose
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class MTableCliDataOpsArg implements Serializable {

  private static final long serialVersionUID = -186601124597330750L;
  private String tableName;
  private String key;
  private String startKey;
  private String stopKey;

  private enum ScanStatus {
    PUT, GET, SCAN
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getStartKey() {
    return startKey;
  }

  public void setStartKey(String startKey) {
    this.startKey = startKey;
  }

  public String getStopKey() {
    return stopKey;
  }

  public void setStopKey(String stopKey) {
    this.stopKey = stopKey;
  }

}
