package io.ampool.monarch.table.loader;

import io.ampool.monarch.table.*;
import io.ampool.monarch.table.internal.MOperation;
import io.ampool.monarch.table.internal.MTableKey;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.internal.MValue;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.concurrent.Callable;

public class MultiTableColumnLoader implements CacheLoader {
  protected static final Logger logger = LogService.getLogger();

  @Override
  public void close() {
    System.out.println("MultiTableColumnLoader.close");
  }


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

  private void updatePutObject(Put row, MColumnDescriptor cd) {
    byte[] columnName = cd.getColumnName();
    try {
      Object v = getCVfromUnderlyingDB(Bytes.toString(columnName), Bytes.toInt(row.getRowKey()));
      row.addColumn(columnName, cd.getColumnType().serialize(v));
    } catch (Exception e) {
      logger.error("Exception Occured ", e);
      e.printStackTrace();
    }
  }

  @Override
  public Object load(LoaderHelper helper) throws CacheLoaderException {
    MTableKey key = (MTableKey) helper.getKey();
    MTable fedTable = helper.getMTable();
    MTableDescriptor tableDescriptor = fedTable.getTableDescriptor();
    Put row = new Put(key.getBytes());
    if (key.getColumnPositions().size() == 0) {
      tableDescriptor.getAllColumnDescriptors().forEach(cd -> {
        if (0 != Bytes.compareTo(Bytes.toBytes(MTableUtils.KEY_COLUMN_NAME), cd.getColumnName())) {
          updatePutObject(row, cd);
        }
      });
    } else {
      System.out.println("MultiTableColumnLoader.load --> [" + Bytes.toInt(key.getBytes()) + "] : "
          + key.getColumnPositions());
      for (Integer columnPosition : key.getColumnPositions()) {
        MColumnDescriptor cd = tableDescriptor.getColumnDescriptorByIndex(columnPosition);
        // System.out.println("cd = " + cd);
        updatePutObject(row, cd);
      }
    }
    return row.serialize(tableDescriptor);
  }
}
