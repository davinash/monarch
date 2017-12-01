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

package io.ampool.monarch.table.coprocessor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDUnitHelper;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * MTableCoprocessorFrameworkDUnitTest
 *
 * @since 0.2.0.0
 */

@Category(MonarchTest.class)
public class MTableCoprocessorObjectSerilizableDUnitTest extends MTableDUnitHelper {

  private static final Logger logger = LogService.getLogger();
  public static final int ROWS_PER_BUCKET = 10;
  public static final String COPROCESSOR_CLASS_NAME =
      "io.ampool.monarch.table.coprocessor.SampleCoprocessorForSerialization";

  private Host host = null;
  private VM vm0 = null;
  private VM vm1 = null;
  private VM vm2 = null;
  private VM vm3 = null;

  public MTableCoprocessorObjectSerilizableDUnitTest() {
    super();
  }



  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);

    startServerOn(vm0, DUnitLauncher.getLocatorString());
    startServerOn(vm1, DUnitLauncher.getLocatorString());
    startServerOn(vm2, DUnitLauncher.getLocatorString());

    createClientCache(vm3);
    createClientCache();
  }


  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(this.vm3);
    super.tearDown2();
  }


  /**
   * Passing MGet and verifying at co-processor Positive should pass
   */
  @Test
  public void testMGetSerializerWithCP() {
    // Testcase with invalid/non existing classname
    final MTable mtable = createMTable(getTestMethodName());
    final List<byte[]> keys = putDataInEachBucket(mtable, ROWS_PER_BUCKET);
    final int randomInt = new Random().nextInt(keys.size() - 1);
    Get get = new Get(keys.get(randomInt));
    get.setTimeStamp(Long.MAX_VALUE);
    get.addColumn("COL1");

    final MExecutionRequest request = new MExecutionRequest();
    List arguments = new ArrayList();
    arguments.add(get);
    arguments.add(keys.get(randomInt));
    request.setArguments(arguments);
    final Map<Integer, List<Object>> result =
        mtable.coprocessorService(COPROCESSOR_CLASS_NAME, "serializeMGet", null, null, request);
    boolean testpassed = true;
    final Iterator<Entry<Integer, List<Object>>> itr = result.entrySet().iterator();
    System.out.println(result);
    while (itr.hasNext()) {
      final Entry<Integer, List<Object>> res = itr.next();
      testpassed = testpassed && ((Boolean) ((ArrayList) res.getValue()).get(0));
    }
    assertTrue(testpassed);
    deleteMTable(getTestMethodName());

  }

  /**
   * Passing MGet and verifying at co-processor. Negative should pass.
   */
  @Test
  public void testMGetSerializerWithCPNegative() {
    // Testcase with invalid/non existing classname
    final MTable mtable = createMTable(getTestMethodName());
    final List<byte[]> keys = putDataInEachBucket(mtable, ROWS_PER_BUCKET);
    final int randomInt = new Random().nextInt(keys.size() - 1);
    Get get = new Get(keys.get(randomInt));
    get.setTimeStamp(Long.MAX_VALUE);
    get.addColumn("COL2");

    final MExecutionRequest request = new MExecutionRequest();
    List arguments = new ArrayList();
    arguments.add(get);
    arguments.add(keys.get(randomInt));
    request.setArguments(arguments);
    final Map<Integer, List<Object>> result =
        mtable.coprocessorService(COPROCESSOR_CLASS_NAME, "serializeMGet", null, null, request);
    boolean testpassed = true;
    final Iterator<Entry<Integer, List<Object>>> itr = result.entrySet().iterator();
    System.out.println(result);
    while (itr.hasNext()) {
      final Entry<Integer, List<Object>> res = itr.next();
      testpassed = testpassed && ((Boolean) ((ArrayList) res.getValue()).get(0));
    }
    assertFalse(testpassed);
    deleteMTable(getTestMethodName());

  }

  /**
   * Passing MGet and verifying at co-processor Positive should pass
   */
  @Test
  public void testMDeleteSerializerWithCP() {
    // Testcase with invalid/non existing classname
    final MTable mtable = createMTable(getTestMethodName());
    final List<byte[]> keys = putDataInEachBucket(mtable, ROWS_PER_BUCKET);
    final int randomInt = new Random().nextInt(keys.size() - 1);
    Delete delete = new Delete(keys.get(randomInt));
    delete.setTimestamp(Long.MAX_VALUE);
    delete.addColumn(Bytes.toBytes("COL1"));

    final MExecutionRequest request = new MExecutionRequest();
    List arguments = new ArrayList();
    arguments.add(delete);
    arguments.add(keys.get(randomInt));
    request.setArguments(arguments);
    final Map<Integer, List<Object>> result =
        mtable.coprocessorService(COPROCESSOR_CLASS_NAME, "serializeMDelete", null, null, request);
    boolean testpassed = true;
    final Iterator<Entry<Integer, List<Object>>> itr = result.entrySet().iterator();
    System.out.println(result);
    while (itr.hasNext()) {
      final Entry<Integer, List<Object>> res = itr.next();
      testpassed = testpassed && ((Boolean) ((ArrayList) res.getValue()).get(0));
    }
    assertTrue(testpassed);
    deleteMTable(getTestMethodName());

  }

  /**
   * Passing MGet and verifying at co-processor. Negative should pass.
   */
  @Test
  public void testMDeleteSerializerWithCPNegative() {
    // Testcase with invalid/non existing classname
    final MTable mtable = createMTable(getTestMethodName());
    final List<byte[]> keys = putDataInEachBucket(mtable, ROWS_PER_BUCKET);
    final int randomInt = new Random().nextInt(keys.size() - 1);
    Delete delete = new Delete(keys.get(randomInt));
    delete.setTimestamp(Long.MAX_VALUE);
    delete.addColumn(Bytes.toBytes("COL2"));

    final MExecutionRequest request = new MExecutionRequest();
    List arguments = new ArrayList();
    arguments.add(delete);
    arguments.add(keys.get(randomInt));
    request.setArguments(arguments);
    final Map<Integer, List<Object>> result =
        mtable.coprocessorService(COPROCESSOR_CLASS_NAME, "serializeMDelete", null, null, request);
    boolean testpassed = true;
    final Iterator<Entry<Integer, List<Object>>> itr = result.entrySet().iterator();
    System.out.println(result);
    while (itr.hasNext()) {
      final Entry<Integer, List<Object>> res = itr.next();
      testpassed = testpassed && ((Boolean) ((ArrayList) res.getValue()).get(0));
    }
    assertFalse(testpassed);
    deleteMTable(getTestMethodName());

  }

}
