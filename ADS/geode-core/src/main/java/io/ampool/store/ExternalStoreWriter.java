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
package io.ampool.store;

import io.ampool.monarch.table.ArchiveConfiguration;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.TableDescriptor;

import java.net.MalformedURLException;


public interface ExternalStoreWriter {
  /**
   * Writer the rows to the external store
   * 
   * @param tableDescriptor
   * @param rows
   * @param configuration
   */
  void write(TableDescriptor tableDescriptor, Row[] rows, ArchiveConfiguration configuration)
      throws MalformedURLException, Exception;

}
