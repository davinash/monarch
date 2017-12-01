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

import static org.junit.Assert.*;

@Category(MonarchTest.class)
@RunWith(JUnitParamsRunner.class)
public class MTablePrePutDUnitTest extends MTableDUnitHelper {
  private final int numOfColumns = 10;
  private final String columnPrefix = "Column-";
  private final int numOfRows = 10;

  @Test
  @Parameters(method = "tableType")
  public void testPrePutOldValueNotChanged(MTableType tableType) {
    MTable table = createTable(tableType);
    for (int key = 0; key < numOfRows; key++) {
      Put row = new Put(Bytes.toBytes(key));
      for (int colIdx = 0; colIdx < numOfColumns; colIdx++) {
        row.addColumn(columnPrefix + colIdx, key * colIdx);
      }
      try {
        table.put(row);
      } catch (NotAuthorizedException nae) {
        nae.printStackTrace();
      }
    }
  }

  @Test
  @Parameters(method = "tableType")
  public void testPrePutOldValueNotChangedUpdate(MTableType tableType) {
    String tableName = "PRE_PUT_MTABLE";
    if (tableType == MTableType.UNORDERED) {
      tableName += "__UNORDERED";
    } else {
      tableName += "__ORDERED";
    }
    MTableDescriptor tableDescriptor = new MTableDescriptor(tableType);
    tableDescriptor.addColumn(columnPrefix + 0, BasicTypes.INT);
    tableDescriptor.addColumn(columnPrefix + 1, BasicTypes.INT);
    tableDescriptor.addColumn(columnPrefix + 2, BasicTypes.STRING);
    tableDescriptor.addColumn(columnPrefix + 3, BasicTypes.STRING);
    tableDescriptor.addColumn(columnPrefix + 4, BasicTypes.INT);

    tableDescriptor
        .addCoprocessor("io.ampool.monarch.table.coprocessor.PrePutEventValueCheckCoProcessor1");
    MTable table =
        MClientCacheFactory.getAnyInstance().getAdmin().createMTable(tableName, tableDescriptor);

    for (int key = 0; key < numOfRows; key++) {
      Put row = new Put(Bytes.toBytes(key));
      row.addColumn(columnPrefix + 0, 0);
      row.addColumn(columnPrefix + 1, 1);
      row.addColumn(columnPrefix + 2, "Dummy-Value-2");
      row.addColumn(columnPrefix + 3, "Dummy-Value-3");
      row.addColumn(columnPrefix + 4, 3);

      try {
        table.put(row);
      } catch (NotAuthorizedException nae) {
      }
    }

    for (int key = 0; key < numOfRows; key++) {
      Get get = new Get(Bytes.toBytes(key));
      Row row = table.get(get);
      assertTrue(row.isEmpty());
    }
  }


  @Test
  @Parameters(method = "tableType")
  public void testPrePutOldValueNotChangedUpdateWithVersions(MTableType tableType) {
    if (tableType == MTableType.UNORDERED) {
      return;
    }
    String tableName = "PRE_PUT_MTABLE";
    if (tableType == MTableType.UNORDERED) {
      tableName += "__UNORDERED";
    } else {
      tableName += "__ORDERED";
    }
    MTableDescriptor tableDescriptor = new MTableDescriptor(tableType);
    tableDescriptor.addColumn(columnPrefix + 0, BasicTypes.INT);
    tableDescriptor.addColumn(columnPrefix + 1, BasicTypes.INT);
    tableDescriptor.addColumn(columnPrefix + 2, BasicTypes.STRING);
    tableDescriptor.addColumn(columnPrefix + 3, BasicTypes.STRING);
    tableDescriptor.addColumn("DOB", BasicTypes.STRING);

    tableDescriptor
        .addCoprocessor("io.ampool.monarch.table.coprocessor.PrePutEventValueCheckCoProcessor2");
    tableDescriptor.setMaxVersions(3);
    MTable table =
        MClientCacheFactory.getAnyInstance().getAdmin().createMTable(tableName, tableDescriptor);

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

  @Test
  public void testPrePutGetClientServer() {
    MTableDescriptor tableDescriptor = new MTableDescriptor(MTableType.ORDERED_VERSIONED);
    tableDescriptor.addColumn("Column1", BasicTypes.STRING);
    tableDescriptor.addColumn("Column2", BasicTypes.STRING);
    tableDescriptor.addColumn("Column3", BasicTypes.STRING);
    tableDescriptor.addCoprocessor("io.ampool.monarch.table.coprocessor.ObserverCoProcessorTest");

    MTable table =
        MClientCacheFactory.getAnyInstance().getAdmin().createMTable("TEST_TABLE", tableDescriptor);

    Put put = new Put(Bytes.toBytes(1));
    put.addColumn("Column1", "val1");
    put.addColumn("Column2", "val2");
    put.addColumn("Column3", "val3");

    table.put(put);
    table.get(new Get(Bytes.toBytes(1)));

    this.vm1.invoke(() -> {
      MTable table1 = MCacheFactory.getAnyInstance().getMTable("TEST_TABLE");
      Put put1 = new Put(Bytes.toBytes(1));
      put1.addColumn("Column1", "val1");
      put1.addColumn("Column2", "val2");
      put1.addColumn("Column3", "val3");

      table1.put(put1);
      table1.get(new Get(Bytes.toBytes(1)));
    });

    MClientCacheFactory.getAnyInstance().getAdmin().deleteMTable("TEST_TABLE");

  }

  private MTable createTable(MTableType tableType) {
    String tableName = "PRE_PUT_MTABLE";
    if (tableType == MTableType.UNORDERED) {
      tableName += "__UNORDERED";
    } else {
      tableName += "__ORDERED";
    }
    MTableDescriptor tableDescriptor = new MTableDescriptor(tableType);
    for (int i = 0; i < numOfColumns; i++) {
      tableDescriptor.addColumn(columnPrefix + i, BasicTypes.INT);
    }
    tableDescriptor
        .addCoprocessor("io.ampool.monarch.table.coprocessor.PrePutEventValueCheckCoProcessor");
    return MClientCacheFactory.getAnyInstance().getAdmin().createMTable(tableName, tableDescriptor);
  }

  private MTableType[] tableType() {
    return new MTableType[] {MTableType.ORDERED_VERSIONED, MTableType.UNORDERED};
  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    createClientCache(this.client1);

    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(client1);

    new ArrayList<>(Arrays.asList(vm0, vm1, vm2))
        .forEach((VM) -> VM.invoke(new SerializableCallable() {
          @Override
          public Object call() throws Exception {
            MCacheFactory.getAnyInstance().close();
            return null;
          }
        }));

    super.tearDown2();
  }

}
