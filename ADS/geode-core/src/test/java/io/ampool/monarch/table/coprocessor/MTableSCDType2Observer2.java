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

import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.types.BasicTypes;

import java.sql.Date;
import java.util.function.BiFunction;

public class MTableSCDType2Observer2 extends MBaseRegionObserver {
  public MTableSCDType2Observer2() {}

  @Override
  public void prePut(MObserverContext mObserverContext, Put put) {
    BiFunction<BasicTypes, Object, Object> newValueFunction = ((basicTypes, startdate) -> {
      return String.valueOf(Long.parseLong((String) startdate) - 1);
    });
    MTableUtils.changeOldVersionValue(mObserverContext.getEntryEvent().getOldValue(),
        mObserverContext.getTable().getTableDescriptor(), put, "start_date", "end_date",
        newValueFunction);

  }
}
