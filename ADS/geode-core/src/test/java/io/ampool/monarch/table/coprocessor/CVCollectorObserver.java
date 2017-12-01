package io.ampool.monarch.table.coprocessor;

import io.ampool.monarch.table.*;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Callable;

public class CVCollectorObserver extends MBaseRegionObserver {
  protected static final Logger logger = LogService.getLogger();

  public CVCollectorObserver() {}

  public class ColumnValueGenerator implements Callable<Integer> {
    private final String cName;
    private final int k;

    public ColumnValueGenerator(String colName, int key) {
      this.cName = colName;
      this.k = key;
    }

    @Override
    public Integer call() throws Exception {
      String columnName = cName;
      int key = k;
      String[] split = columnName.split("-");
      return key * Integer.valueOf(split[1]);
    }
  }

  private Object getCVfromUnderlyingDB(String columnName, int key) throws Exception {
    ColumnValueGenerator columnValueGenerator = new ColumnValueGenerator(columnName, key);
    return columnValueGenerator.call();
  }

  private void updatePutObject(Get get, Put row, MColumnDescriptor cd) {
    byte[] columnName = cd.getColumnName();
    try {
      Object v = getCVfromUnderlyingDB(Bytes.toString(columnName), Bytes.toInt(get.getRowKey()));
      row.addColumn(columnName, cd.getColumnType().serialize(v));
    } catch (Exception e) {
      logger.error("Exception Occured ", e);
      e.printStackTrace();
    }
  }

  @Override
  public void preGet(MObserverContext observerContext, Get get) {
    MTable fedTable = observerContext.getTable();
    // First see if the row already exists in the table.
    if (!fedTable.get(get).isEmpty()) {
      System.out.println("CVCollectorObserver.preGet.row already Exists");
      return;
    }
    System.out.println("CVCollectorObserver.preGet");
    // Row does not exists so try populating it from the existing DB.
    Put row = new Put(get.getRowKey());

    if (get.getColumnNameList().isEmpty()) {
      fedTable.getTableDescriptor().getAllColumnDescriptors().forEach(cd -> {
        if (0 != Bytes.compareTo(Bytes.toBytes(MTableUtils.KEY_COLUMN_NAME), cd.getColumnName())) {
          updatePutObject(get, row, cd);
        }
      });
      fedTable.put(row);
    }
  }
}
