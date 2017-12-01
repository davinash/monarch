package io.ampool.monarch.table.coprocessor;

import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.types.BasicTypes;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

@Category(MonarchTest.class)
public class MTablePrePutJUnitTest {
  private final String columnPrefix = "Column-";
  private final int numOfRows = 10;

  @Test
  public void testPrePutOldValueNotChangedUpdateWithVersions() {
    MCache geodeCache = createCache();
    MCache ampoolCache = MCacheFactory.getAnyInstance();

    String tableName = "PRE_PUT_MTABLE";

    MTableDescriptor tableDescriptor = new MTableDescriptor(MTableType.ORDERED_VERSIONED);
    tableDescriptor.addColumn(columnPrefix + 0, BasicTypes.INT);
    tableDescriptor.addColumn(columnPrefix + 1, BasicTypes.INT);
    tableDescriptor.addColumn(columnPrefix + 2, BasicTypes.STRING);
    tableDescriptor.addColumn(columnPrefix + 3, BasicTypes.STRING);
    tableDescriptor.addColumn("DOB", BasicTypes.STRING);

    tableDescriptor
        .addCoprocessor("io.ampool.monarch.table.coprocessor.PrePutEventValueCheckCoProcessor2");
    tableDescriptor.setMaxVersions(3);
    MTable table = ampoolCache.getAdmin().createMTable(tableName, tableDescriptor);

    for (int key = 0; key < numOfRows; key++) {
      for (int ts = 100; ts <= 300; ts += 100) {
        Put row = new Put(Bytes.toBytes(key));
        row.setTimeStamp(ts);
        row.addColumn(columnPrefix + 0, key * ts * 0);
        row.addColumn(columnPrefix + 1, key * ts * 1);
        row.addColumn(columnPrefix + 2, "Dummy-Value-2");
        row.addColumn(columnPrefix + 3, "Dummy-Value-3");
        if (ts != 300) {
          row.addColumn("DOB", "19-01-1978");
        } else {
          row.addColumn("DOB", "19-01-1979");
        }

        try {
          table.put(row);
        } catch (NotAuthorizedException nae) {
        }
      }
    }

    for (int key = 0; key < numOfRows; key++) {
      for (int ts = 100; ts <= 200; ts += 100) {
        Get get = new Get(Bytes.toBytes(key));
        get.setTimeStamp(ts);
        Row row = table.get(get);
        assertFalse(row.isEmpty());
        List<Cell> cells = row.getCells();
        assertEquals(cells.get(0).getColumnValue(), key * ts * 0);
        assertEquals(cells.get(1).getColumnValue(), key * ts * 1);
        assertEquals(cells.get(2).getColumnValue(), "Dummy-Value-2");
        assertEquals(cells.get(3).getColumnValue(), "Dummy-Value-3");
        assertEquals(cells.get(4).getColumnValue(), "19-01-1978");
      }
    }
  }

  private MCache createCache() {
    return new MCacheFactory(createLonerProperties()).create();
  }

  private Properties createLonerProperties() {
    Properties props = new Properties();
    props.put("mcast-port", "0");
    props.put("locators", "");
    return props;
  }
}
