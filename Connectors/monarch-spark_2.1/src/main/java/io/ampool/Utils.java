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

package io.ampool;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.MConfiguration;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.exceptions.MCacheClosedException;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.internal.AdminImpl;
import io.ampool.monarch.table.internal.InternalTable;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.DataTypeFactory;
import io.ampool.monarch.types.ListType;
import io.ampool.monarch.types.MapType;
import io.ampool.monarch.types.StructType;
import io.ampool.monarch.types.WrapperType;
import io.ampool.monarch.types.interfaces.DataType;
import io.ampool.security.SecurityConfigurationKeysPublic;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import org.apache.spark.ml.linalg.MatrixUDT;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.UserDefinedType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import org.apache.spark.sql.types.DataType;
//import org.apache.spark.sql.types.StructType;

public class Utils {
  /** Logger **/
  private static final Logger logger = LoggerFactory.getLogger(Utils.class);
  /**
   * Type mapping -- Spark data types to MTable types.
   */
  private static final Map<String, String> TYPE_MAP_SPARK_TO_AMPOOL =
    new HashMap<String, String>(10) {{
      put(DataTypes.StringType.simpleString(), BasicTypes.STRING.name());
      put(DataTypes.IntegerType.simpleString(), BasicTypes.INT.name());
      put(DataTypes.LongType.simpleString(), BasicTypes.LONG.name());
      put(DataTypes.DoubleType.simpleString(), BasicTypes.DOUBLE.name());
      put(DataTypes.BinaryType.simpleString(), BasicTypes.BINARY.name());
      put(DataTypes.BooleanType.simpleString(), BasicTypes.BOOLEAN.name());
      put(DataTypes.ByteType.simpleString(), BasicTypes.BYTE.name());
      put(DataTypes.DateType.simpleString(), BasicTypes.DATE.name());
      put(DataTypes.FloatType.simpleString(), BasicTypes.FLOAT.name());
      put(DataTypes.ShortType.simpleString(), BasicTypes.SHORT.name());
      put(DataTypes.TimestampType.simpleString(), BasicTypes.TIMESTAMP.name());
    }};
  /**
   * Type mapping -- MTable types to Spark data types.
   */
  private static final Map<String, org.apache.spark.sql.types.DataType> TYPE_MAP_AMPOOL_TO_SPARK =
    new HashMap<String, org.apache.spark.sql.types.DataType>(10) {{
      put(BasicTypes.STRING.name(), DataTypes.StringType);
      put(BasicTypes.O_INT.name(), DataTypes.IntegerType);
      put(BasicTypes.O_LONG.name(), DataTypes.LongType);
      put(BasicTypes.INT.name(), DataTypes.IntegerType);
      put(BasicTypes.LONG.name(), DataTypes.LongType);
      put(BasicTypes.DOUBLE.name(), DataTypes.DoubleType);
      put(BasicTypes.BINARY.name(), DataTypes.BinaryType);
      put(BasicTypes.BOOLEAN.name(), DataTypes.BooleanType);
      put(BasicTypes.BYTE.name(), DataTypes.ByteType);
      put(BasicTypes.DATE.name(), DataTypes.DateType);
      put(BasicTypes.FLOAT.name(), DataTypes.FloatType);
      put(BasicTypes.SHORT.name(), DataTypes.ShortType);
      put(BasicTypes.TIMESTAMP.name(), DataTypes.TimestampType);
      put(VectorUDT.class.getName(), new VectorUDT());
      put(MatrixUDT.class.getName(), new MatrixUDT());
    }};

  /**
   * The pre-serialization converters.. these convert the data-types
   * from Scala types to the respective Java types as the serialization
   * happens based on Java types and Scala types (as array/seq) are
   * not recognized by Java code (without Scala dependency).
   */
  private static final Function<Object, Object> PRE_ARR_FUN =
    (Function<Object, Object> & Serializable) DataConverter$.MODULE$::arrToAmpool;
  private static final Function<Object, Object> PRE_MAP_FUN =
    (Function<Object, Object> & Serializable) DataConverter$.MODULE$::mapToAmpool;
  private static final Function<Object, Object> PRE_ST_FUN =
    (Function<Object, Object> & Serializable) DataConverter$.MODULE$::stToAmpool;
  private static final Map<String, Function<Object, Object>> PRE_SER_MAP =
    new HashMap<String, Function<Object, Object>>(2) {{
      put(ListType.NAME, PRE_ARR_FUN);
      put(MapType.NAME, PRE_MAP_FUN);
      put(StructType.NAME, PRE_ST_FUN);
    }};

  /**
   * The post-deserialization converters.. as the ser-de happens based on Java
   * types these need to be converted back to Spark/Scala understandable
   * data-types such as array/seq/map/row etc.
   * These transform the result of de-serialization to the respective
   * Scala data structures..
   */
  private static final Function<Object, Object> POST_ARR_FUN =
    (Function<Object, Object> & Serializable) DataConverter$.MODULE$::arrToSpark;
  private static final Function<Object, Object> POST_MAP_FUN =
    (Function<Object, Object> & Serializable) DataConverter$.MODULE$::mapToSpark;
  private static final Function<Object, Object> POST_ST_FUN =
    (Function<Object, Object> & Serializable) DataConverter$.MODULE$::stToSpark;
  private static final Map<String, Function<Object, Object>> POST_DES_MAP =
    new HashMap<String, Function<Object, Object>>(2) {{
      put(ListType.NAME, POST_ARR_FUN);
      put(MapType.NAME, POST_MAP_FUN);
      put(StructType.NAME, POST_ST_FUN);
    }};

  private static final Map<String, Function<Object, Object>> VECTOR_POST_MAP =
    Collections.unmodifiableMap(new HashMap<String, Function<Object, Object>>(1) {{
      put(StructType.NAME, (Function<Object, Object> & Serializable) DataConverter$.MODULE$::toVector);
    }});

  private static final Map<String, Function<Object, Object>> VECTOR_PRE_MAP =
    Collections.unmodifiableMap(new HashMap<String, Function<Object, Object>>(1) {{
      put(StructType.NAME, (Function<Object, Object> & Serializable) DataConverter$.MODULE$::fromVector);
    }});

  private static final Map<String, Map<String, Function<Object, Object>>> CUSTOM_PRE_POST_MAP =
    Collections.unmodifiableMap(new HashMap<String, Map<String, Function<Object, Object>>>(4) {{
      put("vector-pre", VECTOR_PRE_MAP);
      put("vector-post", VECTOR_POST_MAP);
//      put("matrix-pre", VECTOR_PRE_MAP);
//      put("matrix-post", VECTOR_POST_MAP);
    }});

  /**
   * Convert Spark DataFrame schema to corresponding MTable schema. Map the
   * DataTypes to MTable (DataType) names.
   *
   * @param sparkStruct the Spark DataFrame schema
   * @return the table descriptor
   */
  public static TableDescriptor toAmpoolSchema(final org.apache.spark.sql.types.StructType sparkStruct, TableDescriptor td, boolean isFTable) {
    scala.collection.Iterator<StructField> itr = sparkStruct.iterator();
    StructField sf;
    DataType type;
    String str;
    Schema.Builder sb = new Schema.Builder();
    while (itr.hasNext()) {
      sf = itr.next();
      if (sf.dataType() instanceof UserDefinedType) {
        str = ((UserDefinedType) sf.dataType()).sqlType().simpleString();
        type = DataTypeFactory.getTypeFromString(str, TYPE_MAP_SPARK_TO_AMPOOL);
        type = new WrapperType(type, ((UserDefinedType) sf.dataType()).getClass()
          .getName(), null, null);
      } else {
        str = sf.dataType().simpleString();
        type = DataTypeFactory.getTypeFromString(str, TYPE_MAP_SPARK_TO_AMPOOL);
      }
      if(isFTable) {
        if (!FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME.equals(sf.name())) {
          sb.column(sf.name(), type);
        }
      }else{
        sb.column(sf.name(), type);
      }
    }
    td.setSchema(sb.build());
    return td;
  }

  /**
   * Convert Spark DataFrame schema to corresponding FTable schema. Map the
   * DataTypes to FTable (DataType) names.
   *
   * @param sparkStruct the Spark DataFrame schema
   * @return the table descriptor
   */
  public static FTableDescriptor toAmpoolFTableSchema(final org.apache.spark.sql.types.StructType sparkStruct, final Map<String, String> parameters) {
    FTableDescriptor td = new FTableDescriptor();
    toAmpoolSchema(sparkStruct, td, true);
    return td;
  }

  /**
   * Convert Spark DataFrame schema to corresponding MTable schema. Map the
   * DataTypes to FTable (DataType) names.
   *
   * @param sparkStruct the Spark DataFrame schema
   * @return the table descriptor
   */
  public static MTableDescriptor toAmpoolMTableSchema(final org.apache.spark.sql.types.StructType sparkStruct, MTableType type) {
    MTableDescriptor td = new MTableDescriptor(type);
    toAmpoolSchema(sparkStruct, td, false);
    return td;
  }

  /**
   * Convert MTable schema to Spark DataFrame schema. Map the respective type
   * names from MTable (DataType) to corresponding DataTypes.
   *
   * @param td the table descriptor
   * @return the Spark DataFrame schema
   */
  public static org.apache.spark.sql.types.StructType toSparkSchema(final TableDescriptor td) {
    List<StructField> fields = td.getAllColumnDescriptors()
      .stream().sequential().map(cd -> DataTypes.createStructField(cd.getColumnNameAsString(),
        toSparkType(cd.getColumnType()), true))
      .collect(Collectors.toList());
    return DataTypes.createStructType(fields);
  }

  /**
   * Convert the Ampool type to respective Spark DataType. For basic types
   * there is simple lookup but for user-defined-types (like Vector) the
   * respective type name is stored with type and is used here for data-frame
   * types.
   *
   * @param aType the Ampool type
   * @return the Spark type
   */
  public static org.apache.spark.sql.types.DataType toSparkType(final DataType aType) {
    DataType type = aType instanceof WrapperType ?
      ((WrapperType) aType).getBasicObjectType() : aType;
    org.apache.spark.sql.types.DataType field = null;
    switch (type.getCategory()) {
      case Basic:
        field = TYPE_MAP_AMPOOL_TO_SPARK.get(type.toString());
        break;
      case List:
        final DataType elType = ((ListType) type).getTypeOfElement();
        field = DataTypes.createArrayType(toSparkType(elType));
        break;
      case Map:
        final DataType kType = ((MapType) type).getTypeOfKey();
        final DataType vType = ((MapType) type).getTypeOfValue();
        field = DataTypes.createMapType(toSparkType(kType), toSparkType(vType));
        break;
      case Struct:
        org.apache.spark.sql.types.DataType udt = null;
        if (aType instanceof WrapperType) {
          udt = TYPE_MAP_AMPOOL_TO_SPARK.get(((WrapperType) aType).getArgs());
        }
        if (udt != null) {
          field = udt;
        } else {
          final StructType sType = (StructType) type;
          List<StructField> fields = new ArrayList<>(sType.getColumnNames().length);
          final String[] names = sType.getColumnNames();
          final DataType[] types = sType.getColumnTypes();
          for (int i = 0; i < names.length; i++) {
            fields.add(DataTypes.createStructField(names[i], toSparkType(types[i]), true));
          }
          field = DataTypes.createStructType(fields);
        }
        break;
      case Union:
        break;
    }
    if (field == null) {
      throw new IllegalArgumentException("Unsupported type: " + aType.toString());
    }
    return field;
  }

  /**
   * Provide the client cache.
   *
   * @param parameters map of configuration parameters
   * @return the client cache
   */
  public static MClientCache getClientCache(final Map<String, String> parameters) {
    MClientCache cache;
    try {
      cache = MClientCacheFactory.getAnyInstance();
    } catch (MCacheClosedException e) {
      logger.info("No cache found.. trying to create using: {}", parameters);
      MConfiguration conf = MConfiguration.create();
      conf.set(Constants.MonarchLocator.MONARCH_LOCATOR_ADDRESS,
        parameters.getOrDefault(Constants$.MODULE$.AmpoolLocatorHostKey(), "localhost"));
      conf.set(Constants.MonarchLocator.MONARCH_LOCATOR_PORT,
        parameters.getOrDefault(Constants$.MODULE$.AmpoolLocatorPortKey(), "10334"));
      conf.set(Constants.MClientCacheconfig.MONARCH_CLIENT_READ_TIMEOUT,
        parameters.getOrDefault(Constants$.MODULE$.AmpoolReadTimeout(), Constants$.MODULE$.AmpoolReadTimeoutDefault()));

      /* log file provided, if any.. */
      final String logFile = parameters.getOrDefault(Constants$.MODULE$.AmpoolLogFile(),null);
      if (logFile != null) {
        conf.set(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG, logFile);
      }


      final String isKerberosAuthCEnabled = parameters.getOrDefault(Constants$.MODULE$.KerberosAuthCEnabled(), null);
      if("true".equalsIgnoreCase(isKerberosAuthCEnabled)) {
        conf.set("security-enable-kerberos-authc", "true");

        final String kerberosServicePrincipal = parameters.getOrDefault(Constants$.MODULE$.KerberosServicePrincipal(), null);
        if(kerberosServicePrincipal != null) {
          conf.set(SecurityConfigurationKeysPublic.KERBEROS_SERVICE_PRINCIPAL, kerberosServicePrincipal);
        }else {
          throw new IllegalArgumentException("User must provide [" + SecurityConfigurationKeysPublic.KERBEROS_SERVICE_PRINCIPAL + "] when kerberos authC is enabled! ");
        }

        final String kerberosSecurityUsername = parameters.getOrDefault(Constants$.MODULE$.KerberosSecurityUsername(), null);
        if( kerberosSecurityUsername != null){
          conf.set(SecurityConfigurationKeysPublic.KERBEROS_SECURITY_USERNAME, kerberosSecurityUsername);
        }else {
          throw new IllegalArgumentException("User must provide [" + SecurityConfigurationKeysPublic.KERBEROS_SECURITY_USERNAME + "] when kerberos authC is enabled! ");
        }

        final String kerberosSecurityPassword = parameters.getOrDefault(Constants$.MODULE$.KerberosSecurityPassword(), null);
        if(kerberosSecurityPassword != null){
          conf.set(SecurityConfigurationKeysPublic.KERBEROS_SECURITY_PASSWORD, kerberosSecurityPassword);
        }else {
          throw new IllegalArgumentException("User must provide [" + SecurityConfigurationKeysPublic.KERBEROS_SECURITY_PASSWORD + "] when kerberos authC is enabled! ");
        }

        conf.set("security-client-auth-init","io.ampool.security.AmpoolAuthInitClient.create");
      }

      cache = MClientCacheFactory.getOrCreate(conf);
    }
    return cache;
  }

  public static InternalTable getInternalTable(String tableName, MClientCache clientCache){
    return (InternalTable) ((MonarchCacheImpl) clientCache).getAnyTable(tableName);
  }

  public static boolean tableExists(String tableName, MClientCache clientCache){
    return  ((AdminImpl)(clientCache.getAdmin())).tableExists(tableName);
  }

  public static InternalTable createTable(String tablename, TableDescriptor td, MClientCache clientCache, boolean isFTable){
    if(isFTable) {
      return (InternalTable) clientCache.getAdmin().createFTable(tablename, (FTableDescriptor) td);
    }
    else {
      return (InternalTable) clientCache.getAdmin().createMTable(tablename, (MTableDescriptor)td);
    }
  }

  public static void deleteTable(String tablename, MClientCache clientCache){
    ((AdminImpl)(clientCache.getAdmin())).deleteTable(tablename);
  }
}
