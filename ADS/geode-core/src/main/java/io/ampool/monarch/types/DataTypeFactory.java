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

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.types.interfaces.DataType;

/**
 * The factory class that provides the conversion from string representation to ObjectType.
 *
 * Since version: 0.2.0
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class DataTypeFactory {

  /** escape begin characters **/
  public static final char[] ESCAPE_BEGIN_CHARS =
      new char[] {DataType.COMPLEX_TYPE_BEGIN_CHAR, DataType.TYPE_ARGS_BEGIN_CHAR};
  /** escape end characters **/
  public static final char[] ESCAPE_END_CHARS =
      new char[] {DataType.COMPLEX_TYPE_END_CHAR, DataType.TYPE_ARGS_END_CHAR};

  private DataTypeFactory() {
    /// should not be instantiated..
  }

  /**
   * The regular-expression for matching complex types with type-name and arguments..
   */
  private static final String COMPLEX_TYPE_REGEX = "(.*?)" + DataType.COMPLEX_TYPE_BEGIN_CHAR
      + "(.*)" + DataType.COMPLEX_TYPE_END_CHAR + "(?:\\((.*)\\))?";
  private static final Matcher MATCHER = Pattern.compile(COMPLEX_TYPE_REGEX).matcher("dummy");

  /**
   * Provide the DataType object from string for the respective category.
   *
   * @param typeStr the object type as string
   * @param basicTypeConvertMap the string-mapping for basic types
   * @return the object-type constructed from type-string
   */
  public static DataType getTypeFromString(final String typeStr,
      final Map<String, String> basicTypeConvertMap) {
    return DataTypeFactory.getTypeFromString(typeStr, basicTypeConvertMap, Collections.emptyMap(),
        Collections.emptyMap());
  }

  public static DataType getTypeFromString(final String typeStr,
      final Map<String, String> basicTypeConvertMap,
      final Map<String, ? extends Function<Object, Object>> postDeserializeMap) {
    return DataTypeFactory.getTypeFromString(typeStr, basicTypeConvertMap, Collections.emptyMap(),
        postDeserializeMap);
  }

  /**
   * Provide the DataType object from string for the respective category.
   *
   * @param typeStr the object type as string
   * @param basicTypeConvertMap the string-mapping for basic types
   * @return the object-type constructed from type-string
   */
  public static DataType getTypeFromString(final String typeStr,
      final Map<String, String> basicTypeConvertMap,
      final Map<String, ? extends Function<Object, Object>> preSerializeMap,
      final Map<String, ? extends Function<Object, Object>> postDeserializeMap) {
    return getTypeFromString(typeStr, basicTypeConvertMap, preSerializeMap, postDeserializeMap,
        null);
  }

  public static DataType getTypeFromString(final String typeStr,
      final Map<String, String> basicTypeConvertMap,
      final Map<String, ? extends Function<Object, Object>> preSerializeMap,
      final Map<String, ? extends Function<Object, Object>> postDeserializeMap,
      final String converterDependency) {
    DataType objectType = null;
    Objects.requireNonNull(typeStr, "The type must be a valid string.");
    String type = null;
    if (typeStr.indexOf('<') <= 0) {
      type = typeStr;
      String args = null;
      /**
       * Consider the string before first ( as the type (either built-in or custom). Some types can
       * have arguments like char(100) or decimal(10,10), ignore such arguments for built-in basic
       * types as none require these at the moment. For custom types pass it to the constructor. The
       * custom types should have FQCN as type with arguments (as string), if any.
       */
      final int argPos = typeStr.indexOf(DataType.TYPE_ARGS_BEGIN_CHAR);
      final int argEnd = typeStr.indexOf(DataType.TYPE_ARGS_END_CHAR);
      if (argPos > 0 && argEnd == typeStr.length() - 1) {
        type = typeStr.substring(0, argPos);
        args = typeStr.substring(argPos + 1, argEnd);
      }
      try {
        Object newType = basicTypeConvertMap.get(type);
        objectType = BasicTypes.valueOf(newType == null ? type : newType.toString());
        if (args != null) {
          objectType = new WrapperType(objectType, args, null, null);
        }
      } catch (IllegalArgumentException e) {
        objectType = createTypeUsingReflection(type, args);
      }
    } else {
      /** this is a complex type **/
      if (MATCHER.reset(typeStr).matches()) {
        /** get alias if any.. **/
        type = basicTypeConvertMap.get(MATCHER.group(1));
        if (type == null) {
          type = MATCHER.group(1).toLowerCase();
        }
        switch (type) {
          case ListType.NAME:
            DataType elType = getTypeFromString(MATCHER.group(2), basicTypeConvertMap,
                preSerializeMap, postDeserializeMap, converterDependency);
            objectType = new ListType(elType);
            break;
          case MapType.NAME:
            List<String> types = splitDefault(MATCHER.group(2));
            if (types.size() != 2) {
              throw new IllegalArgumentException("Map: Input should be only key and value.");
            }
            DataType keyType = getTypeFromString(types.get(0), basicTypeConvertMap, preSerializeMap,
                postDeserializeMap, converterDependency);
            DataType valType = getTypeFromString(types.get(1), basicTypeConvertMap, preSerializeMap,
                postDeserializeMap, converterDependency);
            objectType = new MapType(keyType, valType);
            break;
          case StructType.NAME:
            List<String> stringList = splitDefault(MATCHER.group(2));
            final String[] nameList = new String[stringList.size()];
            final DataType[] typeList = new DataType[stringList.size()];
            List<String> nameTypeList;
            for (int i = 0; i < stringList.size(); i++) {
              nameTypeList = split(stringList.get(i), DataType.COMPLEX_TYPE_NAME_TYPE_SEPARATOR,
                  DataType.COMPLEX_TYPE_BEGIN_CHAR, DataType.COMPLEX_TYPE_END_CHAR);
              if (nameTypeList.size() != 2) {
                throw new IllegalArgumentException("Struct: Input should be only name and type.");
              }
              nameList[i] = nameTypeList.get(0);
              typeList[i] = getTypeFromString(nameTypeList.get(1), basicTypeConvertMap,
                  preSerializeMap, postDeserializeMap, converterDependency);
            }
            objectType = new StructType(nameList, typeList);
            break;
          case UnionType.NAME:
            List<String> list = splitDefault(MATCHER.group(2));
            final DataType[] t1 = new DataType[list.size()];
            for (int i = 0; i < list.size(); i++) {
              t1[i] = getTypeFromString(list.get(i), basicTypeConvertMap, preSerializeMap,
                  postDeserializeMap, converterDependency);
            }
            objectType = new UnionType(t1);
            break;
        }
        /** process arguments, if any, for complex types.. **/
        final String args = MATCHER.group(3);
        if (args != null && objectType != null) {
          objectType = new WrapperType(objectType, args, null, null);
        }
      }
    }
    if (objectType == null) {
      throw new IllegalArgumentException("Incorrect type syntax: " + typeStr);
    }
    /**
     * return wrapper object in case we want to process the input/output pre-serialization or
     * post-deserialization
     */
    Function<Object, Object> preSerialize = preSerializeMap.get(type);
    Function<Object, Object> postDeserialize = postDeserializeMap.get(type);
    if (objectType instanceof WrapperType) {
      ((WrapperType) objectType).setPrePostFunction(converterDependency, preSerialize,
          postDeserialize);
    } else if (preSerialize != null || postDeserialize != null) {
      objectType = new WrapperType(objectType, null, preSerialize, postDeserialize);
      ((WrapperType) objectType).setConverterDependency(converterDependency);
    }
    return objectType;
  }

  /**
   * Convert the provided string representation of types using the specified conversion map. In case
   * the type is not present in the attached map then the original type is retained.
   *
   * @param typeStr the string representation of a type
   * @param convertMap the string-mapping for types
   * @return the converted string representation of the type
   */
  public static String convertTypeString(final String typeStr,
      final Map<String, String> convertMap) {
    String outTypeStr = null;
    Objects.requireNonNull(typeStr, "The type must be a valid string.");
    String type;
    if (typeStr.indexOf(DataType.COMPLEX_TYPE_BEGIN_CHAR) <= 0) {
      type = typeStr;
      final int argPos = typeStr.indexOf(DataType.TYPE_ARGS_BEGIN_CHAR);
      final int argEnd = typeStr.indexOf(DataType.TYPE_ARGS_END_CHAR);
      String args = null;
      if (argPos > 0 && argEnd == typeStr.length() - 1) {
        type = typeStr.substring(0, argPos);
        args = typeStr.substring(argPos + 1, argEnd);
      }
      outTypeStr = convertMap.getOrDefault(type, type);
      if (args != null) {
        outTypeStr += DataType.TYPE_ARGS_BEGIN_CHAR + args + DataType.TYPE_ARGS_END_CHAR;
      }
    } else {
      /** this is a complex type **/
      if (MATCHER.reset(typeStr).matches()) {
        /** get alias if any.. **/
        type = MATCHER.group(1).toLowerCase();
        final String newType = convertMap.getOrDefault(MATCHER.group(1), MATCHER.group(1));
        switch (type) {
          case ListType.NAME:
            outTypeStr = newType + DataType.COMPLEX_TYPE_BEGIN_CHAR
                + convertTypeString(MATCHER.group(2), convertMap) + DataType.COMPLEX_TYPE_END_CHAR;
            break;
          case MapType.NAME:
            List<String> types = splitDefault(MATCHER.group(2));
            if (types.size() != 2) {
              throw new IllegalArgumentException("Map: Input should be only key and value.");
            }
            outTypeStr = newType + DataType.COMPLEX_TYPE_BEGIN_CHAR
                + convertTypeString(types.get(0), convertMap) + DataType.COMPLEX_TYPE_SEPARATOR
                + convertTypeString(types.get(1), convertMap) + DataType.COMPLEX_TYPE_END_CHAR;
            break;
          case StructType.NAME:
            List<String> stringList = splitDefault(MATCHER.group(2));
            List<String> nameTypeList;
            StringBuilder sb = new StringBuilder(32);
            sb.append(newType).append(DataType.COMPLEX_TYPE_BEGIN_CHAR);
            for (final String e : stringList) {
              nameTypeList = split(e, DataType.COMPLEX_TYPE_NAME_TYPE_SEPARATOR,
                  DataType.COMPLEX_TYPE_BEGIN_CHAR, DataType.COMPLEX_TYPE_END_CHAR);
              if (nameTypeList.size() != 2) {
                throw new IllegalArgumentException("Struct: Input should be only name and type.");
              }
              sb.append(nameTypeList.get(0)).append(DataType.COMPLEX_TYPE_NAME_TYPE_SEPARATOR)
                  .append(convertTypeString(nameTypeList.get(1), convertMap))
                  .append(DataType.COMPLEX_TYPE_SEPARATOR);
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append(DataType.COMPLEX_TYPE_END_CHAR);
            outTypeStr = sb.toString();
            break;
          case UnionType.NAME:
            List<String> list = splitDefault(MATCHER.group(2));
            StringBuilder sb1 = new StringBuilder(32);
            sb1.append(newType).append(DataType.COMPLEX_TYPE_BEGIN_CHAR);
            for (final String e : list) {
              sb1.append(convertTypeString(e, convertMap)).append(DataType.COMPLEX_TYPE_SEPARATOR);
            }
            sb1.deleteCharAt(sb1.length() - 1);
            sb1.append(DataType.COMPLEX_TYPE_END_CHAR);
            outTypeStr = sb1.toString();
            break;
        }
      }
    }
    return outTypeStr;
  }

  /**
   * Load class using reflection for custom types.. The specified class must implement
   * {@link DataType} interface.
   *
   * @param className the FQCN for the object/column type
   * @param arg the string argument, if any
   * @return the object type
   */
  private static DataType createTypeUsingReflection(final String className, final String arg) {
    DataType objectType = null;
    try {
      Class<?> clazz = Class.forName(className);
      if (!(DataType.class.isAssignableFrom(clazz))) {
        throw new IllegalArgumentException("Class must extend: " + DataType.class.getName());
      }
      if (arg != null) {
        objectType = (DataType) clazz.getDeclaredConstructor(String.class).newInstance(arg);
      } else {
        objectType = (DataType) clazz.getDeclaredConstructor().newInstance();
      }
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException
        | InvocationTargetException | NoSuchMethodException e) {
      // e.printStackTrace();
    }
    return objectType;
  }

  /**
   * Provide the DataType object from string for the respective category.
   *
   * @param typeStr the object type as string
   * @return the object-type constructed from type-string
   */
  public static DataType getTypeFromString(final String typeStr) {
    return DataTypeFactory.getTypeFromString(typeStr, Collections.emptyMap());
  }

  /**
   * Split the string using default split-character along with default begin and end escape
   * characters. The defaults are: - splitChar : , - escapeBeginChar: < - escapeEndChar : >
   *
   * @param string the input string to be split
   * @return the list of sub string split using the default split characters
   */
  public static List<String> splitDefault(final String string) {
    return split(string, DataType.COMPLEX_TYPE_SEPARATOR, ESCAPE_BEGIN_CHARS, ESCAPE_END_CHARS);
  }

  public static List<String> split(final String string, final char splitChar,
      final char escapeBeginChar, final char escapeEndChar) {
    return split(string, splitChar, new char[] {escapeBeginChar}, new char[] {escapeEndChar});
  }

  /**
   * Split the input string on specified char. But do not split on the specified char if it is
   * escaped -- present between specific characters like < and > or a pair of quotes -- ' or " It
   * removes the empty elements if any.. and retains <space> characters, if any
   * <p>
   * 
   * @param string the string to be split
   * @param splitChar the character to be used for splitting
   * @param escapeBeginChars escape begin character
   * @param escapeEndChars escape end character
   * @return the list of sub-strings separated by the specified character
   */
  public static List<String> split(final String string, final char splitChar,
      final char[] escapeBeginChars, final char[] escapeEndChars) {
    List<String> list = new ArrayList<>(5);
    char currentChar;
    Arrays.sort(escapeBeginChars);
    Arrays.sort(escapeEndChars);
    int beginIndex = 0;
    int nestedEscapeCount = 0;
    for (int i = 0; i < string.length(); i++) {
      currentChar = string.charAt(i);
      if (currentChar == splitChar && nestedEscapeCount == 0) {
        if (i > beginIndex) {
          list.add(string.substring(beginIndex, i));
        }
        beginIndex = i + 1;
      } else if (Arrays.binarySearch(escapeBeginChars, currentChar) >= 0) {
        ++nestedEscapeCount;
      } else if (Arrays.binarySearch(escapeEndChars, currentChar) >= 0) {
        --nestedEscapeCount;
      }
    }
    if (string.length() > beginIndex) {
      list.add(string.substring(beginIndex));
    }
    return list;
  }

  public static String getExternalTypeString(DataType mType, Map<String, String> typeMap) {
    StringBuilder extType = new StringBuilder();
    if (mType instanceof MapType) {
      String targetValue = typeMap.get(MapType.NAME);
      extType.append(targetValue == null ? MapType.NAME : targetValue);
      extType.append(DataType.COMPLEX_TYPE_BEGIN_CHAR);
      extType.append(getExternalTypeString(((MapType) mType).getTypeOfKey(), typeMap));
      extType.append(DataType.COMPLEX_TYPE_SEPARATOR);
      extType.append(getExternalTypeString(((MapType) mType).getTypeOfValue(), typeMap));
      extType.append(DataType.COMPLEX_TYPE_END_CHAR);
    } else if (mType instanceof ListType) {
      String targetValue = typeMap.get(ListType.NAME);
      extType.append(targetValue == null ? ListType.NAME : targetValue);
      extType.append(DataType.COMPLEX_TYPE_BEGIN_CHAR);
      extType.append(getExternalTypeString(((ListType) mType).getTypeOfElement(), typeMap));
      extType.append(DataType.COMPLEX_TYPE_END_CHAR);
    } else if (mType instanceof StructType) {
      String targetValue = typeMap.get(StructType.NAME);
      extType.append(targetValue == null ? StructType.NAME : targetValue);
      extType.append(DataType.COMPLEX_TYPE_BEGIN_CHAR);

      String[] subtypeNames = ((StructType) mType).getColumnNames();
      DataType[] subtypes = ((StructType) mType).getColumnTypes();
      String delim = "";
      for (int i = 0; i < subtypes.length; i++) {
        extType.append(delim);
        delim = "" + DataType.COMPLEX_TYPE_SEPARATOR;
        extType.append(subtypeNames[i] + DataType.COMPLEX_TYPE_NAME_TYPE_SEPARATOR);
        extType.append(getExternalTypeString(subtypes[i], typeMap));
      }
      extType.append(DataType.COMPLEX_TYPE_END_CHAR);
    } else if (mType instanceof UnionType) {
      String targetValue = typeMap.get(UnionType.NAME);
      extType.append(targetValue == null ? UnionType.NAME : targetValue);
      extType.append(DataType.COMPLEX_TYPE_BEGIN_CHAR);

      DataType[] subtypes = ((UnionType) mType).getColumnTypes();
      String delim = "";
      for (int i = 0; i < subtypes.length; i++) {
        extType.append(delim);
        delim = "" + DataType.COMPLEX_TYPE_SEPARATOR;
        extType.append(getExternalTypeString(subtypes[i], typeMap));
      }
      extType.append(DataType.COMPLEX_TYPE_END_CHAR);
    } else if (mType instanceof BasicTypes || mType instanceof WrapperType) {
      String args = null;
      if (mType instanceof WrapperType) {
        args = ((WrapperType) mType).getArgs();
        mType = ((WrapperType) mType).getBasicObjectType();

      }
      String targetValue = typeMap.get(((BasicTypes) mType).name());
      extType.append(targetValue == null ? ((BasicTypes) mType).name() : targetValue);
      if (args != null) {
        extType.append(DataType.TYPE_ARGS_BEGIN_CHAR).append(args)
            .append(DataType.TYPE_ARGS_END_CHAR);
      }
    }
    return extType.toString();
  }
}
