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

import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.coprocessor.internal.MCoprocessorContextImpl;
import io.ampool.monarch.table.internal.MKeyBase;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.execute.RegionFunctionContextImpl;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * ComputeAverageFunction - Sample Coprocessor.
 *
 * @since 0.2.0.0
 */

public class ComputeAverageFunction extends MCoprocessor {

  /*
   * @Override public void run(MCoprocessorContext context) {
   * 
   * System.out.println("NNNN ComputeAverageFunction...");
   * //MCoprocessorUtils.getLogger().info("NNNN == Executing Function ComputeAverageFunction");
   * Object arguments = context.getRequest().getArguments(); int total = 0; int result = 0;
   * if(arguments instanceof ArrayList){ ArrayList list = (ArrayList)arguments;
   * //MCoprocessorUtils.getLogger().info("CCCC size of arguments = " + list.size());
   * System.out.println("CCCC size of arguments = " + list.size());
   * 
   * for(Object element : list){ //MCoprocessorUtils.getLogger().info("Argument received = " +
   * element.toString()); System.out.println("Argument received = " + element.toString()); total =
   * total + ((Integer)element).intValue(); result = total /list.size(); } }
   * 
   * if (hasResult()) { context.getResultSender().lastResult(result); }else {
   * context.getResultSender().lastResult("Error"); }
   * System.out.println("ComputeAverageFunction.run:: RESULT " + result);
   * 
   * }
   */

  public int computeAverage(MCoprocessorContext context) {
    System.out.println("NNNN ComputeAverageFunction...");
    Object arguments = context.getRequest().getArguments();
    int total = 0;
    int result = 0;

    if (arguments instanceof ArrayList) {
      ArrayList list = (ArrayList) arguments;
      // MCoprocessorUtils.getLogger().info("CCCC size of arguments = " + list.size());
      System.out.println("CCCC size of arguments = " + list.size());

      for (Object element : list) {
        // MCoprocessorUtils.getLogger().info("Argument received = " + element.toString());
        System.out.println("Argument received = " + element.toString());
        total = total + ((Integer) element).intValue();
        result = total / list.size();
      }
    }
    return result;
  }

  public boolean getBIDToHostingServer(MCoprocessorContext context) {
    System.out.println("DEBUG: Start getBIDToHostingServer...");
    Object arguments = context.getRequest().getArguments();
    boolean isKeyExist = false;
    if (arguments instanceof byte[]) {
      byte[] key = (byte[]) arguments;

      RegionFunctionContextImpl rfci = ((MCoprocessorContextImpl) context).getNativeContext();
      int targetBucketId = rfci.getTargetBucketId();

      MTable table = context.getTable();
      String regionName = table.getName();
      System.out.println("ComputeAverageFunction.getBIDToHostingServer RegionName = " + regionName
          + " Key = " + Arrays.toString(key));
      PartitionedRegion pr =
          (PartitionedRegion) MonarchCacheImpl.getExisting().getRegion(regionName);

      BucketRegion bucketRegion = pr.getDataStore().getLocalBucketById(targetBucketId);

      isKeyExist = bucketRegion.containsKey(new MKeyBase(key));
    }
    return isKeyExist;

  }

  @Override
  public String getId() {
    return this.id;
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean isHA() {
    return true;
  }
}
