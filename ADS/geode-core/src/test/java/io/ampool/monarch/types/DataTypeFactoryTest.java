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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import io.ampool.monarch.types.interfaces.DataType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class DataTypeFactoryTest {

  /**
   * Test for constructor.. for code coverage..
   */
  @Test
  public void testDummyConstructor() {
    try {
      Constructor<?> c;
      c = Class.forName("io.ampool.monarch.types.DataTypeFactory").getDeclaredConstructor();
      c.setAccessible(true);
      Object o = c.newInstance();
      assertNotNull(o);
    } catch (Exception e) {
      fail("No exception was expected here..");
    }
  }

  @DataProvider
  public static Object[][] getTypeData() {
    return new Object[][] {{"INT", BasicTypes.INT}, {"CHAR(100)", BasicTypes.CHAR},
        {"BIG_DECIMAL(20,10)", BasicTypes.BIG_DECIMAL},
        {"array<INT>", new ListType(BasicTypes.INT)},
        {"array<LONG>", new ListType(BasicTypes.LONG)},
        {"array<array<INT>>", new ListType(new ListType(BasicTypes.INT))},
        {"array<BIG_DECIMAL(10,5)>", new ListType(BasicTypes.BIG_DECIMAL)},
        {"map<STRING,BIG_DECIMAL(10,5)>", new MapType(BasicTypes.STRING, BasicTypes.BIG_DECIMAL)},
        {"struct<c1:INT,c2:CHAR(100),c3:BIG_DECIMAL(20,10)>",
            new StructType(new String[] {"c1", "c2", "c3"},
                new DataType[] {BasicTypes.INT, BasicTypes.CHAR, BasicTypes.BIG_DECIMAL})},
        {"struct<c1:INT,c2:CHAR(100),c3:BIG_DECIMAL(20,10)>(vector)",
            new StructType(new String[] {"c1", "c2", "c3"},
                new DataType[] {BasicTypes.INT, BasicTypes.CHAR, BasicTypes.BIG_DECIMAL})},
        {"union<FLOAT,BIG_DECIMAL(20,10),STRING>",
            new UnionType(
                new DataType[] {BasicTypes.FLOAT, BasicTypes.BIG_DECIMAL, BasicTypes.STRING})},
        {"struct<c1:INT,c2:STRING,c3:map<INT,STRING>,c4:array<FLOAT>>", new StructType(
            new String[] {"c1", "c2", "c3", "c4"},
            new DataType[] {BasicTypes.INT, BasicTypes.STRING,
                new MapType(BasicTypes.INT, BasicTypes.STRING), new ListType(BasicTypes.FLOAT)})},
        {"map<INT,STRING>", new MapType(BasicTypes.INT, BasicTypes.STRING)},
        {"map<INT,union<INT,STRING>>",
            new MapType(BasicTypes.INT,
                new UnionType(new DataType[] {BasicTypes.INT, BasicTypes.STRING}))},
        {"union<INT,DOUBLE,array<INT>>",
            new UnionType(
                new DataType[] {BasicTypes.INT, BasicTypes.DOUBLE, new ListType(BasicTypes.INT)})},
        {"array<union<INT,DOUBLE,array<INT>>>",
            new ListType(new UnionType(
                new DataType[] {BasicTypes.INT, BasicTypes.DOUBLE, new ListType(BasicTypes.INT)}))},
        {"map<FLOAT,struct<c1:INT,c2:STRING,c3:map<DOUBLE,array<STRING>>,c4:BINARY>>",
            new MapType(BasicTypes.FLOAT,
                new StructType(new String[] {"c1", "c2", "c3", "c4"},
                    new DataType[] {BasicTypes.INT, BasicTypes.STRING,
                        new MapType(BasicTypes.DOUBLE,
                            new ListType(BasicTypes.STRING)),
                        BasicTypes.BINARY}))}};
  }

  /**
   * Test type construction from string for built-in types.
   *
   * @param typeStr the type-string
   * @param expectedType expected type
   * @throws Exception
   */
  @Test(dataProvider = "getTypeData")
  public void testGetTypeFromString(final String typeStr, final DataType expectedType)
      throws Exception {
    DataType objectType = DataTypeFactory.getTypeFromString(typeStr);
    /** test equality via MBasicObjectTypeWrapper **/
    assertTrue(objectType.equals(expectedType));
    assertEquals(objectType.toString(), typeStr);
  }

  public static final Map<String, String> TypeConvertMap = new HashMap<String, String>(20) {
    {
      put(BasicTypes.STRING.toString(), "str");
      put(BasicTypes.VARCHAR.toString(), "vc");
      put(BasicTypes.CHARS.toString(), "cs");
      put(BasicTypes.CHAR.toString(), "c");
      put(BasicTypes.O_INT.toString(), "oi");
      put(BasicTypes.O_LONG.toString(), "ol");
      put(BasicTypes.INT.toString(), "i");
      put(BasicTypes.LONG.toString(), "l");
      put(BasicTypes.DOUBLE.toString(), "d");
      put(BasicTypes.BINARY.toString(), "bi");
      put(BasicTypes.BOOLEAN.toString(), "bl");
      put(BasicTypes.BYTE.toString(), "b");
      put(BasicTypes.DATE.toString(), "dt");
      put(BasicTypes.FLOAT.toString(), "f");
      put(BasicTypes.SHORT.toString(), "s");
      put(BasicTypes.TIMESTAMP.toString(), "ts");
      put(BasicTypes.BIG_DECIMAL.name(), "bd");
      put(StructType.NAME, "st");
      put(MapType.NAME, "mp");
      put(ListType.NAME, "li");
      put(UnionType.NAME, "un");
    }
  };

  @DataProvider
  public static Object[][] getConvertTypeStringData() {
    return new Object[][] {{"INT", "i"}, {"CHAR(100)", "c(100)"},
        {"BIG_DECIMAL(20,10)", "bd(20,10)"}, {"array<INT>", "li<i>"}, {"array<O_LONG>", "li<ol>"},
        {"array<O_INT>", "li<oi>"}, {"array<LONG>", "li<l>"}, {"array<array<INT>>", "li<li<i>>"},
        {"array<BIG_DECIMAL(10,5)>", "li<bd(10,5)>"},
        {"map<STRING,BIG_DECIMAL(10,5)>", "mp<str,bd(10,5)>"},
        {"struct<c1:INT,c2:CHAR(100),c3:BIG_DECIMAL(20,10)>", "st<c1:i,c2:c(100),c3:bd(20,10)>"},
        {"struct<c1:INT,c2:CHAR(100),c3:BIG_DECIMAL(20,10)>", "st<c1:i,c2:c(100),c3:bd(20,10)>"},
        {"union<FLOAT,BIG_DECIMAL(20,10),STRING>", "un<f,bd(20,10),str>"},
        {"struct<c1:INT,c2:STRING,c3:map<INT,STRING>,c4:array<FLOAT>>",
            "st<c1:i,c2:str,c3:mp<i,str>,c4:li<f>>"},
        {"map<INT,STRING>", "mp<i,str>"}, {"map<INT,union<INT,STRING>>", "mp<i,un<i,str>>"},
        {"union<INT,DOUBLE,array<INT>>", "un<i,d,li<i>>"},
        {"array<union<INT,DOUBLE,array<INT>>>", "li<un<i,d,li<i>>>"},
        {"map<FLOAT,struct<c1:INT,c2:STRING,c3:map<DOUBLE,array<STRING>>,c4:BINARY>>",
            "mp<f,st<c1:i,c2:str,c3:mp<d,li<str>>,c4:bi>>"},};
  }

  /**
   * Test type string conversion using the specified map.
   *
   * @param typeStr the type-string
   * @param expectedTypeStr expected type-string
   */
  @Test(dataProvider = "getConvertTypeStringData")
  public void testConvertTypeString(final String typeStr, final String expectedTypeStr) {
    final String newTypeStr = DataTypeFactory.convertTypeString(typeStr, TypeConvertMap);
    assertEquals(newTypeStr, expectedTypeStr);
  }

  @DataProvider
  public static Object[][] getConvertTypeStringNegativeData() {
    return new Object[][] {{"struct<c1,c2:INT>"}, {"map<FLOAT>"}};
  }

  /**
   * Test negative test cases for type string conversion using the specified map.
   *
   * @param typeStr the type-string
   */
  @Test(dataProvider = "getConvertTypeStringNegativeData",
      expectedExceptions = IllegalArgumentException.class)
  public void testConvertTypeStringNegative(final String typeStr) {
    DataTypeFactory.convertTypeString(typeStr, TypeConvertMap);
  }

  @DataProvider
  public static Object[][] getTypeDataInvalid() {
    return new Object[][] {{"MY_INT"}, {"MY_ARRAY<MY_INT>"}, {"junk"}, {"array<"}, {"array<junk>"},
        {"array<array<INT>"}, {"map<INT, STRING>"}, {"map<INT>"}, {"struct<INT, array<STRING>>"},
        {"INT , STRING"},};
  }

  /**
   * Test for invalid type string.
   *
   * @param typeStr the type-string
   * @throws IllegalArgumentException
   */
  @Test(dataProvider = "getTypeDataInvalid", expectedExceptions = IllegalArgumentException.class)
  public void testGetTypeFromString_Invalid(final String typeStr) {
    DataTypeFactory.getTypeFromString(typeStr);
  }

  private static final Map<String, String> TYPE_CONVERT_MAP = new HashMap<String, String>(5) {
    {
      put("int", "INT");
      put("string", "STRING");
      put("long", "LONG");
      put("char", "CHAR");
      put("decimal", "BIG_DECIMAL");
      put("uniontype", "union");
    }
  };

  @DataProvider
  public static Object[][] getTypeDataWithMap() {
    return new Object[][] {{"int", BasicTypes.INT}, {"char(100)", BasicTypes.CHAR},
        {"decimal(20,10)", BasicTypes.BIG_DECIMAL},
        {"array<char(100)>", new ListType(BasicTypes.CHAR)},
        {"array<string>", new ListType(BasicTypes.STRING)},
        {"array<string>", new ListType(BasicTypes.STRING)},
        {"array<long>", new ListType(BasicTypes.LONG)},
        {"array<array<int>>", new ListType(new ListType(BasicTypes.INT))},
        {"union<int,array<long>>",
            new UnionType(new DataType[] {BasicTypes.INT, new ListType(BasicTypes.LONG)})},};
  }

  /**
   * Test type construction from aliases (from the provided map).
   *
   * @param typeStr the type-string
   * @param expectedType expected type
   * @throws Exception
   */
  @Test(dataProvider = "getTypeDataWithMap")
  public void testGetTypeFromStringWithMap(final String typeStr, final DataType expectedType)
      throws Exception {
    DataType objectType = DataTypeFactory.getTypeFromString(typeStr, TYPE_CONVERT_MAP);
    assertTrue(objectType.equals(expectedType));
  }

  /**
   * Tests for MBasicObjectTypeWrapper with post-deserialization..
   */
  private static final Function<Object, Object> StringToIntFunction =
      e -> Integer.valueOf((String) e);
  private static final Function<Object, Object> LongToStringFunction = String::valueOf;

  private static final Map<String, Function<Object, Object>> TYPE_TO_POST_DES_MAP =
      new HashMap<String, Function<Object, Object>>(5) {
        {
          put("STRING", StringToIntFunction);
          put("LONG", LongToStringFunction);
        }
      };

  private static final List<Object> dummyList = new ArrayList<>(1);

  @DataProvider
  public static Object[][] getTypeDataWithPostDeserialize() {
    dummyList.add("10");
    return new Object[][] {{"10", "STRING", 10}, {10L, "LONG", "10"}, {10, "INT", 10},
        {new Object[] {10L}, "array<LONG>", dummyList},
        {new Object[] {10L}, "struct<c1:LONG>", new Object[] {10}},};
  }

  public static String deepToString(final Object object) {
    final StringBuilder sb = new StringBuilder(32);
    TypeHelper.deepToString(object, sb);
    return sb.toString();
  }

  /**
   * Valid tests for object-type wrapper with post deserialize..
   *
   * @param input the input object
   * @param typeStr type string
   * @param expected the expected object after deserialization
   */
  @Test(dataProvider = "getTypeDataWithPostDeserialize")
  public void testTypeWithPostDeserialize(final Object input, final String typeStr,
      final Object expected) {
    DataType type =
        DataTypeFactory.getTypeFromString(typeStr, Collections.emptyMap(), TYPE_TO_POST_DES_MAP);
    Object output = type.deserialize(type.serialize(input));
    if (TYPE_TO_POST_DES_MAP.containsKey(typeStr)) {
      assertTrue(type instanceof WrapperType);
    } else if (type instanceof StructType) {
      DataType[] subTypes = ((StructType) type).getColumnTypes();
      for (DataType subType : subTypes) {
        if (TYPE_TO_POST_DES_MAP.containsKey(subType.toString())) {
          assertTrue(subType instanceof WrapperType);
        }
      }
    } else if (type instanceof ListType) {
      DataType subType = ((ListType) type).getTypeOfElement();
      if (TYPE_TO_POST_DES_MAP.containsKey(subType.toString())) {
        assertTrue(subType instanceof WrapperType);
      }
    }
    assertEquals(output.getClass(), expected.getClass());
    assertEquals(deepToString(output), deepToString(expected));
  }

  /**
   * Data for {@code testTypeWithPostDeserializeInvalid}
   *
   * @return the data required for testTypeWithPostDeserializeInvalid
   */
  @DataProvider
  public static Object[][] getTypeDataWithPostDeserializeInvalid() {
    return new Object[][] {{"10", "STRING", "10"}, {10L, "LONG", 10L}, {10, "INT", 10},};
  }

  @Test(dataProvider = "getTypeDataWithPostDeserializeInvalid")
  public void testTypeWithPostDeserializeInvalid(final Object input, final String typeStr,
      final Object expected) {
    DataType type =
        DataTypeFactory.getTypeFromString(typeStr, Collections.emptyMap(), TYPE_TO_POST_DES_MAP);
    Object output = type.deserialize(type.serialize(input));
    if (TYPE_TO_POST_DES_MAP.containsKey(typeStr)) {
      assertTrue(type instanceof WrapperType);
      assertNotEquals(output.getClass(), expected.getClass());
    } else {
      assertFalse(type instanceof WrapperType);
    }
  }

  @DataProvider
  public static Object[][] getDataSplit() {
    return new Object[][] {{"a,b,c", Arrays.asList("a", "b", "c")},
        {"a,,c", Arrays.asList("a", "c")}, {"INT , STRING", Arrays.asList("INT ", " STRING")},
        {"INT,", Collections.singletonList("INT")},
        {"array<int>,struct<c1:int,c2:string,c3:map<int,string>>,map<int,string>,uniontype<int,double,array<string>>",
            Arrays.asList("array<int>", "struct<c1:int,c2:string,c3:map<int,string>>",
                "map<int,string>", "uniontype<int,double,array<string>>")},
        {"struct<c1:int,c2:string,c3:map<int,string>,c4:double>,map<int,string>,uniontype<int,double,array<string>>",
            Arrays.asList("struct<c1:int,c2:string,c3:map<int,string>,c4:double>",
                "map<int,string>", "uniontype<int,double,array<string>>")}};
  }

  /**
   * Test to make sure that types are split correctly. Comma (,) is used as separator when not
   * enclosed within < and >.
   * <p>
   *
   * @param inputStr the input string to be split
   * @param expectedList expected list of sub strings
   */
  @Test(dataProvider = "getDataSplit")
  public void testSplit(final String inputStr, final List<String> expectedList) {
    assertEquals(DataTypeFactory.split(inputStr, ',', '<', '>'), expectedList);
  }

  @DataProvider
  public static Object[][] getDataSplitDefault() {
    return new Object[][] {{"a,b(10,20),c", Arrays.asList("a", "b(10,20)", "c")},
        {"a(1,2, 3),,c", Arrays.asList("a(1,2, 3)", "c")},
        {"array<int>,struct<c1:int,c2:map<int,decimal(20,10)>,c3:string>,map<int,string>,union<int,double,array<char(100)>>",
            Arrays.asList("array<int>", "struct<c1:int,c2:map<int,decimal(20,10)>,c3:string>",
                "map<int,string>", "union<int,double,array<char(100)>>")},
        {"struct<c1:int,c2:string,c3:map<int,string>,c4:double>,map<int,char(100)>,union<int,double,array<decimal(20,10)>>",
            Arrays.asList("struct<c1:int,c2:string,c3:map<int,string>,c4:double>",
                "map<int,char(100)>", "union<int,double,array<decimal(20,10)>>")}};
  }

  @Test(dataProvider = "getDataSplitDefault")
  public void testSplitDefault(final String inputStr, final List<String> expectedList) {
    assertEquals(DataTypeFactory.splitDefault(inputStr), expectedList);
  }

  public static final class MY_INT_CLS implements DataType {
    @Override
    public Object deserialize(byte[] bytes, Integer offset, Integer length) {
      return null;
    }

    @Override
    public byte[] serialize(Object object) {
      return new byte[0];
    }

    @Override
    public Category getCategory() {
      return null;
    }
  }
  public static final class MY_INT_CLS_1 implements DataType {
    public MY_INT_CLS_1(final String arg) {
      //// handle arguments..
    }

    @Override
    public Object deserialize(byte[] bytes, Integer offset, Integer length) {
      return null;
    }

    @Override
    public byte[] serialize(Object object) {
      return new byte[0];
    }

    @Override
    public Category getCategory() {
      return null;
    }
  }

  /**
   * Tests for creating object type using reflection..
   */
  @Test
  public void testForTypeUsingReflection() {
    System.out.println("DataTypeFactoryTest.testForTypeUsingReflection");
    String cls = "io.ampool.monarch.types.DataTypeFactoryTest$MY_INT_CLS";
    DataType objectType = DataTypeFactory.getTypeFromString(cls);
    assertNotNull(objectType);

    cls = "io.ampool.monarch.types.DataTypeFactoryTest$MY_INT_CLS_1(10)";
    objectType = DataTypeFactory.getTypeFromString(cls);
    assertNotNull(objectType);

    try {
      cls = "io.ampool.monarch.types.DataTypeFactoryTest";
      DataTypeFactory.getTypeFromString(cls);
      fail("Should not reach here..");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Class must extend"), e.getMessage());
    }
  }
}
