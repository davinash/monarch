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
package io.ampool.utils;


import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.conf.Configurable;
import io.ampool.conf.Configuration;

/**
 * General reflection utils.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class ReflectionUtils {

  private static final Class<?>[] EMPTY_ARRAY = new Class[] {};

  /**
   * Cache of constructors for each class. Pins the classes so they can't be garbage collected until
   * ReflectionUtils can be collected.
   */
  private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE = new ConcurrentHashMap<>();

  /**
   * Check and set 'configuration' if necessary.
   *
   * @param theObject object for which to set configuration
   * @param conf Configuration
   */
  private static void setConf(Object theObject, Configuration conf) {
    if (conf != null) {
      if (theObject instanceof Configurable) {
        ((Configurable) theObject).setConf(conf);
      }
    }
  }

  /**
   * Create an object for the given class and initialize it from conf.
   *
   * @param theClass class of which an object is created
   * @param conf Configuration
   * @return a new object
   */
  @SuppressWarnings("unchecked")
  public static <T> T newInstance(Class<T> theClass, Configuration conf) {
    T result;
    try {
      Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
      if (meth == null) {
        meth = theClass.getDeclaredConstructor(EMPTY_ARRAY);
        meth.setAccessible(true);
        CONSTRUCTOR_CACHE.put(theClass, meth);
      }
      result = meth.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    setConf(result, conf);
    return result;
  }

  /**
   * Return the correctly-typed {@link Class} of the given object.
   *
   * @param o object whose correctly-typed <code>Class</code> is to be obtained
   * @return the correctly typed <code>Class</code> of the given object.
   */
  @SuppressWarnings("unchecked")
  public static <T> Class<T> getClass(T o) {
    return (Class<T>) o.getClass();
  }

  // methods to support testing
  static void clearCache() {
    CONSTRUCTOR_CACHE.clear();
  }

  static int getCacheSize() {
    return CONSTRUCTOR_CACHE.size();
  }

  /**
   * Get the value of an instance field in the provided object.
   *
   * @param object the object
   * @param fieldName the instance field name
   * @return the value of the instance field in the provided object
   */
  public static Object getFieldValue(final Object object, final String fieldName) {
    if (object != null) {
      try {
        Field field = object.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(object);
      } catch (NoSuchFieldException | IllegalAccessException e) {
        e.printStackTrace();
      }
    }
    return null;
  }

  /**
   * Set the value of an instance field in the provided object.
   *
   * @param clazz the class
   * @param fieldName the instance field name
   * @param value the value to be set
   */
  public static void setStaticFieldValue(final Class<?> clazz, final String fieldName,
      final Object value) {
    try {
      Field field = clazz.getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(null, value);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      e.printStackTrace();
    }
  }

  /**
   * Set the value of an instance field in the provided object.
   *
   * @param clazz the class
   * @param fieldName the instance field name
   * @return value the value
   */
  public static Object getStaticFieldValue(final Class<?> clazz, final String fieldName) {
    try {
      Field field = clazz.getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(null);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Set the value of an instance field in the provided object.
   *
   * @param object the object
   * @param fieldName the instance field name
   * @param value the value to be set
   */
  public static void setFieldValue(final Object object, final String fieldName,
      final Object value) {
    if (object != null) {
      try {
        Field field = object.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(object, value);
      } catch (NoSuchFieldException | IllegalAccessException e) {
        e.printStackTrace();
      }
    }
  }
}
