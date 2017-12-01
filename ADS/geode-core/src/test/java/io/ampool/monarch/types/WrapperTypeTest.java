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

package io.ampool.monarch.types;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.geode.DataSerializer;
import io.ampool.monarch.types.interfaces.DataType;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class WrapperTypeTest {
  @Test
  @SuppressWarnings("unchecked")
  public void testWrappedObject() {
    BasicTypes type = BasicTypes.INT;
    WrapperType wrapper = new WrapperType(type, null, e -> e, e -> e);
    WrapperType wrapper1 = new WrapperType(type, null, e -> e, e -> e);
    assertTrue(wrapper.equals(type));
    assertTrue(wrapper.equals(wrapper1));

    assertEquals(wrapper.getCategory(), type.getCategory());

    final int targetValue = 10;
    CompareOp op = CompareOp.LESS;
    Predicate wrapperPredicate = wrapper.getPredicate(op, 10);
    Predicate typePredicate = type.getPredicate(op, 10);

    assertEquals(wrapperPredicate.test(targetValue), typePredicate.test(targetValue));
  }

  private static final Function<Object, Object> IntToStringFunction =
      (Function<Object, Object> & Serializable) e -> Integer.valueOf(e.toString());
  private static final Function<Object, Object> StringToLongFunction =
      (Function<Object, Object> & Serializable) e -> Long.valueOf(e.toString());
  /** a function to convert CSV data to an array of ints i.e. "1,2,3" -> int[]{1,2,3} **/
  private static final Function<Object, Object> CsvToIntArrayFunction =
      (Function<Object, Object> & Serializable) e -> Arrays.stream(String.valueOf(e).split(","))
          .mapToInt(Integer::valueOf).toArray();

  @DataProvider
  public static Object[][] getData() {
    return new Object[][] {{10, BasicTypes.STRING, "10", IntToStringFunction, new byte[] {49, 48}},
        {"10", BasicTypes.LONG, 10L, StringToLongFunction,
            ClassCastException.class.getSimpleName()},
        {"1,2,3", new ListType(BasicTypes.INT), Arrays.asList(1, 2, 3), CsvToIntArrayFunction,
            new byte[] {}},};
  }



  /**
   * Test for pre-serialize converter.. with wrapper class to make sure that pre-converter works
   * only when expected. And also it is hidden/not-executed when it is not expected to execute i.e.
   * invalid converter-dependency is provided with the respective converters.
   *
   * @param input the input object
   * @param objectType the object type corresponding to the input object
   * @param expected expected output object of transformed/converted type
   * @param preSerFunction the pre-serialization function
   * @param expectedValueWithoutDependency the expected value with invalid dependency
   */
  @Test(dataProvider = "getData")
  public void testPreSerialize(final Object input, final DataType objectType, final Object expected,
      final Function<Object, Object> preSerFunction, final Object expectedValueWithoutDependency) {
    WrapperType type = new WrapperType(objectType, null, preSerFunction, null);
    Assert.assertEquals(type.serialize(input), objectType.serialize(expected));

    Object output = type.deserialize(objectType.serialize(expected));
    /** only for basic types.. skip this check for list/map etc. **/
    if (objectType instanceof BasicTypes) {
      Assert.assertEquals(output.getClass(), expected.getClass());
    }
    Assert.assertEquals(output, expected);
    System.out.println("objectType = " + objectType.toString());
    /** serialization-deserialization of object.. **/
    File file = new File("/tmp/a.txt");
    try {
      DataOutputStream oos = new DataOutputStream(new FileOutputStream(file));
      type.setConverterDependency("abc");
      DataSerializer.writeObject(type, oos);
      /** pass invalid converter-dependency to assert pre-converter does not work **/
      DataInputStream ois = new DataInputStream(new FileInputStream(file));
      DataType type1 = DataSerializer.readObject(ois);
      if (expectedValueWithoutDependency instanceof String) {
        try {
          type1.serialize(input);
        } catch (Exception e) {
          assertEquals(expectedValueWithoutDependency, e.getClass().getSimpleName());
        }
      } else {
        assertArrayEquals((byte[]) expectedValueWithoutDependency, type1.serialize(input));
      }

      /*** with valid converter-dependency, assert that pre-converter works as expected ***/
      type.setPrePostFunction("java.io.ObjectInputStream", preSerFunction, null);
      DataSerializer.writeObject(type, oos);

      DataType type2 = DataSerializer.readObject(ois);
      Assert.assertEquals(type2.serialize(input), objectType.serialize(expected));
      Object output2 = type.deserialize(objectType.serialize(expected));
      Assert.assertEquals(output2, expected);

      /** close the streams.. **/
      oos.close();
      ois.close();
    } catch (IOException | ClassNotFoundException e) {
      fail("No exception expected.");
    }
  }

  @Test
  public void testPreSerializeNested() {
    final Map<String, Function<Object, Object>> map = new HashMap<>(1);
    map.put(BasicTypes.STRING.name(), (Function<Object, Object> & Serializable) e -> e);
    map.put(ListType.NAME, (Function<Object, Object> & Serializable) e -> e);

    final String typeStr = "array<STRING>";
    DataType type = DataTypeFactory.getTypeFromString(typeStr, Collections.emptyMap(), map,
        Collections.emptyMap(), "abc");

    File file = new File("/tmp/a.txt");
    try {
      DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
      DataSerializer.writeObject(type, dos);
      /** pass invalid converter-dependency to assert pre-converter does not work **/
      DataInputStream dis = new DataInputStream(new FileInputStream(file));
      DataType type1 = DataSerializer.readObject(dis);

      assertEquals(type, type1);

      /** close the streams.. **/
      dos.close();
      dis.close();
    } catch (IOException | ClassNotFoundException e) {
      fail("No exception expected.");
    }
  }
}
