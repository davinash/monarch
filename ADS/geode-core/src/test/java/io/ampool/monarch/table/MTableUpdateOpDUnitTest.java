/*
 * Copyright (c) 2017 Ampool, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 */
package io.ampool.monarch.table;

import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.ByteArrayKey;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.junit.Assert;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.Test;

@Category(MonarchTest.class)
public class MTableUpdateOpDUnitTest extends MTableDUnitHelper {

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    createClientCache(this.vm3);
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache(this.vm3);
    super.tearDown2();
  }

  public MTableUpdateOpDUnitTest() {
    super();
  }

  private ArrayList<MColumnDescriptor> createTableSchema() {
    ArrayList<MColumnDescriptor> mcds = new ArrayList<MColumnDescriptor>();
    mcds.add(new MColumnDescriptor(Bytes.toBytes("NAME")));
    mcds.add(new MColumnDescriptor(Bytes.toBytes("ID")));
    mcds.add(new MColumnDescriptor(Bytes.toBytes("AGE")));
    mcds.add(new MColumnDescriptor(Bytes.toBytes("SALARY")));

    return mcds;

  }

  private void UpdateOperationFromClient(VM vm, final int locatorPort, final boolean ordered) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MClientCache clientCache = MClientCacheFactory.getAnyInstance();

        MTableDescriptor tableDescriptor =
            new MTableDescriptor(ordered ? MTableType.ORDERED_VERSIONED : MTableType.UNORDERED);
        tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
            .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));

        tableDescriptor.setRedundantCopies(1).setMaxVersions(5);

        Admin admin = clientCache.getAdmin();
        MTable mtable = admin.createTable("EmployeeTable", tableDescriptor);

        Put myput = new Put(Bytes.toBytes("RowKey1"));

        myput.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Avinash"));
        myput.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(100));
        myput.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(37));
        myput.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(500000));

        mtable.put(myput);


        /* Now Create new Put Object with lesser columns */
        // MPut myPutSomeColumns = new MPut(Bytes.toBytes("RowKey2"));
        myput.clear();
        myput.setRowKey(Bytes.toBytes("RowKey2"));

        myput.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Nilkanth"));
        myput.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(250000));
        mtable.put(myput);
        myput.clear();

        // MPut myPutRemainingColumns = new MPut(Bytes.toBytes("RowKey2"));
        // myput.setRowKey(Bytes.toBytes("RowKey2"));
        myput.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(101));
        myput.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(35));
        mtable.put(myput);

        // Following is the expected value
        // this object is used only for testing.
        Put testPut = new Put(Bytes.toBytes("RowKey1"));
        testPut.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Nilkanth"));
        testPut.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(250000));
        testPut.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(101));
        testPut.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(35));


        Get myget = new Get(Bytes.toBytes("RowKey2"));
        Row result = mtable.get(myget);
        assertNotNull(result);

        Iterator<MColumnDescriptor> iteratorColumnDescriptor =
            mtable.getTableDescriptor().getAllColumnDescriptors().iterator();

        List<Cell> row = result.getCells();
        for (int i = 0; i < row.size() - 1; i++) {
          if (row.get(i).getColumnValue() == null) {
            Assert.fail("Not All column updated");
          }

          byte[] expectedColumnName = iteratorColumnDescriptor.next().getColumnName();
          byte[] expectedColumnValue =
              (byte[]) testPut.getColumnValueMap().get(new ByteArrayKey(expectedColumnName));

          if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
            Assert.fail("Invalid Values for Column Name");
          }
          if (!Bytes.equals(expectedColumnValue, (byte[]) row.get(i).getColumnValue())) {
            Assert.fail("Invalid Values for Column Value");
          }
        }
        return null;
      }
    });
  }


  /*
   * TEST DESCRIPTION 1. Starts 3 data servers 2. Start 1 client 3. Create MTable Instance with 4
   * columns 4. Put 1 record with key "RowKey1" 5. Create another put record with 2 columns 6.
   * Perform the put operation 7. Update local MPut record with remaining 2 columns 8. Perform the
   * put operation 9. Do Get using the Key 10. Verify if all the columns are returned by Get
   * Operation
   */

  @Test
  public void testUpdateOperation() throws Exception {
    UpdateOperationFromClient(vm3, getLocatorPort(), true);
  }

  @Test
  public void testUpdateOperationUnordered() throws Exception {
    UpdateOperationFromClient(vm3, getLocatorPort(), false);
  }
}
