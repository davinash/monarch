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
package io.ampool.monarch.table.internal;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.filter.Filter;

/**
 * Abstract Query class to apply filter chain to data operations
 *
 */

@InterfaceAudience.Private
@InterfaceStability.Stable
public abstract class MQuery {
  protected Filter filter = null;

  /**
   * @return Filter
   */
  public Filter getFilter() {
    return filter;
  }

  /**
   * Apply the specified server-side filter when performing the Query.
   * 
   * @param filter filter to run on the server
   * @return this for invocation chaining
   */
  public MQuery setFilter(Filter filter) {
    this.filter = filter;
    return this;
  }


}
