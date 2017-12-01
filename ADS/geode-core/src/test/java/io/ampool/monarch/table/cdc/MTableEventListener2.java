package io.ampool.monarch.table.cdc;

import io.ampool.monarch.table.CDCEvent;
import io.ampool.monarch.table.MAsyncEventListener;

import java.util.List;

/**
 * Created by root on 23/8/16.
 */
public class MTableEventListener2 implements MAsyncEventListener {
  @Override
  public boolean processEvents(List<CDCEvent> events) {
    System.out.println("MTableEventListener2.processEvents");
    for (CDCEvent event : events) {
      System.out
          .println("MTableEventListener2.processEvents table = " + event.getMTable().getName());
      System.out.println("MTableEventListener2.processEvents key = " + event.getKey());
      System.out.println(
          "MTableEventListener2.processEvents tableDescriptor = " + event.getMTableDescriptor());
      System.out.println("MTableEventListener2.processEvents operation = " + event.getOperation());
    }
    return true;
  }

  @Override
  public void close() {

  }
}


