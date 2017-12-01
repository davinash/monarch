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

import java.lang.reflect.Constructor;

public class StoreUtils {
  public static Constructor<?> hasParameterlessPublicConstructor(Class<?> clazz)
      throws DefaultConstructorMissingException {
    for (Constructor<?> constructor : clazz.getConstructors()) {
      if (constructor.getParameterCount() == 0) {
        return constructor;
      }
    }
    throw new DefaultConstructorMissingException("Default constructor missing");
  }

  public static Constructor<?> getTierStoreConstructor(Class<?> clazz)
      throws DefaultConstructorMissingException {
    for (Constructor<?> c : clazz.getConstructors()) {
      if (c.getParameterCount() == 1 && c.getParameterTypes()[0] == String.class) {
        return c;
      }
    }
    throw new DefaultConstructorMissingException("TierStore default Constructor missing.");
  }
}
