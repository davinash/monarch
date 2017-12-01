package io.ampool.monarch.table.coprocessor;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.Set;

class TrxTransactionState implements Serializable {
  private Set<String> perBucketCallStack;

  public TrxTransactionState() {
    perBucketCallStack = new LinkedHashSet();
  }


  public void updateTrxState(String perBucketCall) {
    this.perBucketCallStack.add(perBucketCall);
  }

  public Set<String> getPerBucketCallStack() {
    return perBucketCallStack;
  }

  @Override
  public String toString() {
    String callStackAsStr = "_";
    for (String call : perBucketCallStack) {
      callStackAsStr += call;
    }
    return callStackAsStr;
  }

}
