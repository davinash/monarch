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

package io.ampool.monarch.table.coprocessor.internal;

import io.ampool.monarch.table.coprocessor.MCoprocessorContext;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;


/**
 * MCoprocessorAdapter, Function Adapter class for coprocessor.
 *
 * @since 0.2.0.0
 */

public abstract class MCoprocessorAdapter extends FunctionAdapter {

  private static final long serialVersionUID = -6244542488911729777L;
  protected String id;

  @Override
  public void execute(FunctionContext context) {
    MCoprocessorContext mCContext = new MCoprocessorContextImpl(context);
    run(mCContext);
  }

  public abstract void run(MCoprocessorContext context);

  public String getId() {
    return this.id;
  }

  public void setId(String id) {
    this.id = id;
  }
}
