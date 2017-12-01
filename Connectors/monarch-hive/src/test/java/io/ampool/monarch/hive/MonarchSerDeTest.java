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

package io.ampool.monarch.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Tests for serialization and de-serialization used while storing data in Monarch.
 *
 */
public class MonarchSerDeTest {

  @DataProvider
  private Object[][] getColumnInfo() {
    return new Object[][][] {
      {new String[]{"inode","size","path"}, new String[]{"int","int","string"}},
      {new String[]{"col1","col2","col3","col4"}, new String[]{"int","double","map<int,string>","uniontype<string,string,int>"}},
      {new String[]{"col1","col2","col3","col4"}, new String[]{"int","struct<c1:int>","map<int,string>","uniontype<string,string,int>"}},
      {new String[]{"col1","col2","col3","col4"}, new String[]{"array<int>","struct<c1:int,c2:string,c3:map<int,string>>","map<int,string>","uniontype<int,double,array<string>>"}},
      {new String[]{"col1","col2","col3"}, new String[]{"struct<c1:int,c2:string,c3:map<int,string>,c4:double>","map<int,string>","uniontype<int,double,array<string>>"}}
    };
  }
  @Test(dataProvider = "getColumnInfo")
  public void testGetSerdeParams(final String[] columns, String[] columnsType) throws Exception {
//    System.out.println("MonarchSerDeTest.testGetSerdeParams :: " + Arrays.toString(columns) + " -- " + Arrays.toString(columnsType));
    Properties props = new Properties();
    props.setProperty("storage_handler", MonarchStorageHandler.class.getName());
    StringBuilder sb = new StringBuilder(32);
    Arrays.stream(columns).forEach(e -> sb.append(',').append(e));
    sb.deleteCharAt(0);
    props.setProperty("columns", sb.toString());

    sb.delete(0, sb.length());
    Arrays.stream(columnsType).forEach(e -> sb.append(':').append(e));
    sb.deleteCharAt(0);
    props.setProperty("columns.types", sb.toString());

    MonarchSerDe msd = new MonarchSerDe();
    msd.initialize(new Configuration(), props);

    assertTrue(msd.getColumnNames().size() == msd.getColumnTypes().size());

    /** assert on column names **/
    Collection<String> list = msd.getColumnNames();
    assertEquals(list.toArray(new String[list.size()]), columns);

    /** assert on column types **/
    final List<String> actualTypes = msd.getColumnTypes()
      .stream().map(TypeInfo::toString).collect(Collectors.toList());
    assertEquals(actualTypes.toArray(new String[actualTypes.size()]), columnsType);
  }
}