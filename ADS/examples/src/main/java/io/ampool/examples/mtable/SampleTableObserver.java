package io.ampool.examples.mtable;

import org.apache.geode.internal.logging.LogService;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.coprocessor.MBaseRegionObserver;
import io.ampool.monarch.table.coprocessor.MObserverContext;
import org.apache.logging.log4j.Logger;

/**
 * SampleTableObserver which observes client actions on MTable.
 */

public class SampleTableObserver extends MBaseRegionObserver {
  private static final Logger logger = LogService.getLogger();
  private int postOpen;
  private int preClose;
  private int postClose;
  private int preCheckAndPut;
  private int postCheckAndPut;
  private int preCheckAndDelete;
  private int postCheckAndDelete;
  private int prePut;
  private int postPut;
  private int preDelete;
  private int postDelete;
  private int preGet;
  private int postGet;

  public SampleTableObserver() {
    this.postOpen = 0;
    this.preClose = 0;
    this.postClose = 0;
    this.preCheckAndPut = 0;
    this.postCheckAndPut = 0;
    this.preCheckAndDelete = 0;
    this.postCheckAndDelete = 0;
    this.prePut = 0;
    this.postPut = 0;
    this.preDelete = 0;
    this.postDelete = 0;
    this.preGet = 0;
    this.postGet = 0;
  }

  @Override
  public void postOpen() {
    postOpen++;
    logger.info("SampleTableObserver.postOpen count = " + postOpen);
  }

  @Override
  public void preClose(MObserverContext mObserverContext) {
    preClose++;
    logger.info("SampleTableObserver.preClose on Table = " + mObserverContext.getTable().getName()
        + " count = " + preClose);
  }

  @Override
  public void postClose() {
    postClose++;
    logger.info("SampleTableObserver.postClose count = " + postClose);
  }

  @Override
  public void preCheckAndPut(MObserverContext mObserverContext, byte[] rowKey, byte[] columnName,
      byte[] columnValue, Put put, boolean isCheckSuccess) {
    preCheckAndPut++;
    logger.info("SampleTableObserver.preCheckAndPut on Table = "
        + mObserverContext.getTable().getName() + " with rowKey = " + Bytes.toString(rowKey)
        + " columnName = " + Bytes.toString(columnName) + " and columnValue = "
        + Bytes.toInt(columnValue) + " count = " + preCheckAndPut);
  }

  @Override
  public void postCheckAndPut(MObserverContext mObserverContext, byte[] rowKey, byte[] columnName,
      byte[] columnValue, Put put, boolean isCheckSuccessful) {
    postCheckAndPut++;
    logger.info("SampleTableObserver.postCheckAndPut on Table = "
        + mObserverContext.getTable().getName() + " with rowKey = " + Bytes.toString(rowKey)
        + " columnName = " + Bytes.toString(columnName) + " and columnValue = "
        + Bytes.toInt(columnValue) + " count = " + postCheckAndPut);
  }

  @Override
  public void preCheckAndDelete(MObserverContext mObserverContext, byte[] rowKey, byte[] columnName,
      byte[] columnValue, Delete delete, boolean isCheckSuccessful) {
    preCheckAndDelete++;
    logger.info("SampleTableObserver.preCheckAndDelete on Table = "
        + mObserverContext.getTable().getName() + " with rowKey = " + Bytes.toString(rowKey)
        + " columnName = " + Bytes.toString(columnName) + " and columnValue = "
        + Bytes.toInt(columnValue) + " count = " + preCheckAndDelete);
  }

  @Override
  public void postCheckAndDelete(MObserverContext mObserverContext, byte[] rowKey,
      byte[] columnName, byte[] columnValue, Delete delete, boolean isCheckSuccessful) {
    postCheckAndDelete++;
    logger.info("SampleTableObserver.postCheckAndDelete on Table = "
        + mObserverContext.getTable().getName() + " with rowKey = " + Bytes.toString(rowKey)
        + " columnName = " + Bytes.toString(columnName) + " and columnValue = "
        + Bytes.toInt(columnValue) + " count = " + postCheckAndDelete);
  }

  @Override
  public void prePut(MObserverContext mObserverContext, Put put) {
    prePut++;
    logger.info("SampleTableObserver.prePut on Table = " + mObserverContext.getTable().getName()
        + " count = " + prePut);
  }

  @Override
  public void postPut(MObserverContext mObserverContext, Put put) {
    postPut++;
    logger.info("SampleTableObserver.postPut on Table = " + mObserverContext.getTable().getName()
        + " count = " + postPut);
  }

  @Override
  public void preDelete(MObserverContext mObserverContext, Delete delete) {
    preDelete++;
    logger.info("SampleTableObserver.preDelete on Table = " + mObserverContext.getTable().getName()
        + " count = " + preDelete);
  }

  @Override
  public void postDelete(MObserverContext mObserverContext, Delete delete) {
    postDelete++;
    logger.info("SampleTableObserver.postDelete on Table = " + mObserverContext.getTable().getName()
        + " count = " + postDelete);
  }

  @Override
  public String toString() {
    return "io.ampool.examples.mtable.SampleTableObserver";
  }

  @Override
  public void preGet(MObserverContext context, Get get) {
    preGet++;
    logger.info("SampleTableObserver.preGet on Table = " + context.getTable().getName()
        + " count = " + preGet);
  }

  @Override
  public void postGet(MObserverContext context, Get get) {
    postGet++;
    logger.info("SampleTableObserver.postGet on Table = " + context.getTable().getName()
        + " count = " + postGet);
  }
}
