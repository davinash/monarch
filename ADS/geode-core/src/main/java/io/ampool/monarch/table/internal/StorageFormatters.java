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

import java.util.HashMap;
import java.util.Map;

/**
 * It will return instance according to Storage format identifier (magic number), encoding, and
 * reserved bits
 * <p>
 */
public class StorageFormatters {

  private static Map<String, StorageFormatter> storageFormatterMap = new HashMap<>();

  public static StorageFormatter getInstance(byte magicNumber, byte encoding, byte reserved) {
    String key = String.valueOf(magicNumber) + String.valueOf(encoding) + String.valueOf(reserved);
    if (storageFormatterMap.containsKey(key)) {
      // read and return that instance
      return storageFormatterMap.get(key);
    } else {
      StorageFormatter storageFormatter = null;
      if (magicNumber == 1 || magicNumber == 2) {
        // mtable storage formatter
        storageFormatter = new MTableStorageFormatter(magicNumber, encoding, reserved);
      }
      if (magicNumber == 3) {
        // ftable storage formatter
        storageFormatter = new FTableStorageFormatter(magicNumber, encoding, reserved);
      }
      if (storageFormatter != null)
        storageFormatterMap.put(key, storageFormatter);

      return storageFormatter;
    }
  }
}
