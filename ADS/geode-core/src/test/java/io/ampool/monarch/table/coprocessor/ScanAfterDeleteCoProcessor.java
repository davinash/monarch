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

import io.ampool.monarch.table.*;

import java.util.List;
import java.util.Map;

public class ScanAfterDeleteCoProcessor extends MCoprocessor {

  public int scanAfterDelete(MCoprocessorContext context) throws Exception {

    // Order of arguments :: <Keys> <Versions> <Columns>
    List<Object> arguments = (List<Object>) context.getRequest().getArguments();
    System.out
        .println("ScanAfterDeleteCoProcessor.scanAfterDelete ARGS.Size = " + arguments.size());

    int bucketId = getBucketId(context);
    MTable table = context.getTable();

    Map<Integer, List<byte[]>> keys = null;
    List<Integer> versionList = null;
    List<String> columns = null;
    Delete delete = null;

    if (arguments.size() == 3) {
      System.out.println("DELETE with key, version, cols");
      // keys, versions and selected columns are provided.
      keys = (Map<Integer, List<byte[]>>) arguments.get(0);
      versionList = (List<Integer>) arguments.get(1);
      columns = (List<String>) arguments.get(2);

      delete = new Delete(keys.get(bucketId).get(0));
      delete.setTimestamp(versionList.get(2));
      for (String column : columns) {
        delete.addColumn(Bytes.toBytes(column));
      }

    } else if (arguments.size() == 2) {
      System.out.println("DELETE with key, version!");
      // Keys and versions are provided
      keys = (Map<Integer, List<byte[]>>) arguments.get(0);
      versionList = (List<Integer>) arguments.get(0);

      delete = new Delete(keys.get(bucketId).get(0));
      delete.setTimestamp(versionList.get(1));
    } else if (arguments.size() == 1) {
      System.out.println(" DELETE a full row");
      // Only keys are provided
      keys = (Map<Integer, List<byte[]>>) arguments.get(0);
      delete = new Delete(keys.get(bucketId).get(1));
    } else {
      System.out.println("Error, Not a valid args!");
      throw new IllegalArgumentException("Arguments are not provided as expected!!");
    }

    // Delete a key

    table.delete(delete);
    int rowCount = 0;
    try {
      Scanner scanner = table.getScanner(new Scan());
      Row row = scanner.next();
      while (row != null) {
        rowCount++;
        row = scanner.next();
      }
      scanner.close();

    } catch (Exception e) {
      e.printStackTrace();
    }
    return rowCount;
  }


  private int getBucketId(MCoprocessorContext context) {
    String coprocessorName = (String) context.getCoprocessorId();
    String classNameSubstr = coprocessorName.substring(context.getTable().getName().length());
    int bucketIdIndex = classNameSubstr.lastIndexOf("_");
    int bucketId =
        Integer.parseInt(classNameSubstr.substring(bucketIdIndex + 1, classNameSubstr.length()));
    System.out.println("ScanAfterDeleteCoProcessor.scanAfterDelete bucketId = " + bucketId);
    return bucketId;
  }
}
