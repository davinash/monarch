package io.ampool.monarch.table.coprocessor;

import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.Put;

/**
 * SampleRegionObserver2
 *
 * @since 0.2.0.0
 */

public class SampleRegionObserver2 extends MBaseRegionObserver {

  private int prePut;
  private int postPut;
  private int preDelete;
  private int postDelete;
  private int preGet;
  private int postGet;

  public int getTotalPrePutCount() {
    return prePut;
  }

  public int getTotalPostPutCount() {
    return postPut;
  }

  public int getTotalPreDeleteCount() {
    return preDelete;
  }

  public int getTotalPostDeleteCount() {
    return postDelete;
  }

  public int getTotalPreGetCount() {
    return preGet;
  }

  public int getTotalPostGetCount() {
    return postGet;
  }

  public SampleRegionObserver2() {
    prePut = 0;
    postPut = 0;
    preDelete = 0;
    postDelete = 0;
    this.preGet = 0;
    this.postGet = 0;
  }

  @Override
  public void prePut(MObserverContext mObserverContext, Put put) {
    prePut++;
    System.out.println("RRRROOOO SampleRegionObserver2 - prePut() called");
  }

  @Override
  public void postPut(MObserverContext mObserverContext, Put put) {
    postPut++;
    System.out.println("RRRROOOO SampleRegionObserver2 - postPut() called");
  }

  @Override
  public void preDelete(MObserverContext mObserverContext, Delete delete) {
    preDelete++;
    System.out.println("RRRROOOO SampleRegionObserver2 - preDelete() called");
  }

  @Override
  public void postDelete(MObserverContext mObserverContext, Delete delete) {
    postDelete++;
    System.out.println("RRRROOOO SampleRegionObserver2 - postDelete() called");
  }

  @Override
  public String toString() {
    return "io.ampool.monarch.table.coprocessor.SampleRegionObserver2";
  }

  @Override
  public void preGet(MObserverContext context, Get get) {
    preGet++;
    System.out.println("RRRROOOO SampleRegionObserver2 - preGet() called, total = " + preGet);
  }

  @Override
  public void postGet(MObserverContext context, Get get) {
    postGet++;
    System.out.println("RRRROOOO SampleRegionObserver2 - preGet() called total = " + postGet);
  }

}
