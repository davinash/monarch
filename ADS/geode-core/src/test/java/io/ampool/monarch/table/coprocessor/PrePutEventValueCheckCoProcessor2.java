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
import io.ampool.monarch.table.internal.ByteArrayKey;
import org.apache.geode.security.NotAuthorizedException;

import java.util.Arrays;

public class PrePutEventValueCheckCoProcessor2 extends MBaseRegionObserver {
  @Override
  public void prePut(MObserverContext mObserverContext, Put put) {
    MTable table = mObserverContext.getTable();
    Row row = table.get(new Get(put.getRowKey()));
    if (!row.isEmpty()) {
      System.out.println("put = " + put);
      final String DOB = (String) row.getCells().get(4).getColumnValue();
      final String DOB_FROM_PUT =
          (String) put.getColumnValueMap().get(new ByteArrayKey(Bytes.toBytes("DOB")));
      System.out.println("[ " + put.getTimeStamp() + " ] -> " + "DOB_FROM_PUT = " + DOB_FROM_PUT);
      System.out.println("[ " + put.getTimeStamp() + " ] -> " + "DOB          = " + DOB);
      if (!DOB.equals(DOB_FROM_PUT)) {
        throw new NotAuthorizedException("Not Authorised to change DOB");
      }
    } else {
      System.out.println("PrePutEventValueCheckCoProcessor2.prePut.new KEY");
    }
  }

  @Override
  public void postPut(MObserverContext mObserverContext, Put put) {

  }

  @Override
  public void preGet(MObserverContext mObserverContext, Get get) {

  }
}
