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
package io.ampool.monarch.table;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.types.*;
import io.ampool.monarch.types.interfaces.DataType;

/**
 * This class wraps allowed column types. If the user defines complex representation for a column
 * type (as a String in proper format; see below) it will be parsed and validated and may generate
 * an IllegalArgumentException if it is not in proper format. Simple types can be passed using the
 * {@link BasicTypes} enum, lists and maps of basic types using the {@link ListType} and
 * {@link MapType} classes.
 * <P>
 * </P>
 * Simple basic type examples:
 * <P>
 * </P>
 * new MTableColumnType(MBasicObjectType.STRING);<br>
 * new MTableColumnType(MBasicObjectType.INT);<br>
 * new MTableColumnType(MBasicObjectType.LONG);<br>
 * new MTableColumnType(new MListObjectType(MBasicObjectType.STRING));<br>
 * new MTableColumnType(new MMapObjectType(MBasicObjectType.STRING, MBasicObjectType.LONG));<br>
 * <P>
 * </P>
 * Simple basic type examples with limit args (Note the limits are currently informational not
 * enforced):
 * <P>
 * </P>
 * new MTableColumnType(MBasicObjectType.CHARS.setArgs("100"));<br>
 * new MTableColumnType(MBasicObjectType.VARCHAR.setArgs("500"));<br>
 * new MTableColumnType(MBasicObjectType.BIG_DECIMAL.setArgs("10,10"));<br>
 * <P>
 * </P>
 * Complex Types defined as a String:
 * <P>
 * </P>
 * Complex types are container types and these are used to wrap other types, including themselves or
 * the basic types. They can be defined as a String that will be parsed to determine the actual
 * DataType.
 * <P>
 * </P>
 * "LIST&lt;T&gt;" represents a Java List&lt;T&gt;<br>
 * "MAP&lt;K, V&gt;" represents Java Map&lt;K,V&gt;<br>
 * "STRUCT&lt;T1,T2,...&gt" represents a Java Object.<br>
 * <P>
 * </P>
 * The LIST and MAP types are similar to respective Java types. Whereas STRUCT is similar to
 * C-language structures which can contain multiple columns with their respective types. These are
 * expected in the same order that was provided at the time of creation.
 * <P>
 * </P>
 * When specifying complex types the contained types must be enclosed within diamond brackets (i.e.
 * < and >) and in case multiple columns are to be provided (for example STRUCT) these should be
 * separated by comma (,).
 * <P>
 * </P>
 * Below are few examples how you can specify the complex types using a string:
 * <P>
 * </P>
 * new MTableColumnType("LIST&lt;INT&gt;"));<br>
 * new MTableColumnType("MAP&lt;STRING,LIST&lt;DOUBLE&gt;&gt;");<br>
 * new MTableColumnType("STRUCT&lt;C1:STRING,C2:DATE,C3:DOUBLE&gt;");<br>
 * new MTableColumnType("array&lt;INT&gt;"));<br>
 * new MTableColumnType("array&lt;BIG_DECIMAL(10,10)&gt;"));
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public class MTableColumnType {
  private final DataType columnType;

  public MTableColumnType(String columnTypeString) {
    this.columnType = DataTypeFactory.getTypeFromString(columnTypeString);
  }

  public MTableColumnType(DataType existingType) {
    this.columnType = existingType;
  }

  /**
   * Get the actual {@link DataType} this instance wraps.
   * 
   * @return The type this instance contains.
   */
  public DataType getObjectType() {
    return this.columnType;
  }

  public String toString() {
    if (columnType.getCategory().equals(DataType.Category.Basic)) {
      return ((BasicTypes) columnType).toString();
    } else if (columnType.getCategory().equals(DataType.Category.List)) {
      return ((ListType) columnType).toString();
    } else if (columnType.getCategory().equals(DataType.Category.Map)) {
      return ((MapType) columnType).toString();
    } else if (columnType.getCategory().equals(DataType.Category.Struct)) {
      return ((StructType) columnType).toString();
    } else if (columnType.getCategory().equals(DataType.Category.Union)) {
      return ((UnionType) columnType).toString();
    } else {
      return "Unknown Type Category";
    }
  }
}
