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

package io.ampool.tierstore;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;

/**
 * Enumeration will hold list of file formats supported. It will take reader and writer class and
 * can be used from Mash instead of taking separate reader writer classes.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public enum FileFormats {

  ORC("io.ampool.stores.orc.writer.TierStoreORCWriter",
      "io.ampool.stores.orc.reader.TierStoreORCReader"),

  PARQUET("io.ampool.tierstore.writers.parquet.TierStoreParquetWriter",
      "io.ampool.tierstore.writers.parquet.TierStoreParquetReader");

  private final String readerClass;
  private final String writerClass;

  FileFormats(final String writerClass, final String readerClass) {
    this.writerClass = writerClass;
    this.readerClass = readerClass;
  }

  @Override
  public String toString() {
    return "Reader class: " + this.readerClass + " Writer class: " + writerClass;
  }
}
