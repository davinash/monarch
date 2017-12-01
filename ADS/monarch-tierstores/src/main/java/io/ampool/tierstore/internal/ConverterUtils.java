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

package io.ampool.tierstore.internal;

import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.tierstore.ConverterDescriptor;
import io.ampool.tierstore.FileFormats;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class ConverterUtils {

  // diallow instance creation
  private ConverterUtils() {}

  ;


  public static final Map<String, Function<Object, Object>> ORCReaderFunctions =
      new HashMap<String, Function<Object, Object>>(20) {
        {
          put(BasicTypes.STRING.name(), e -> e.toString());
          put(BasicTypes.VARCHAR.toString(), e -> e.toString());
          put(BasicTypes.CHARS.toString(), e -> e.toString());
          put(BasicTypes.CHAR.toString(),
              e -> ((HiveCharWritable) e).getHiveChar().getValue().charAt(0));
          put(BasicTypes.O_INT.name(), e -> ((IntWritable) e).get());
          put(BasicTypes.O_LONG.name(), e -> ((LongWritable) e).get());
          put(BasicTypes.INT.name(), e -> ((IntWritable) e).get());
          put(BasicTypes.LONG.name(), e -> ((LongWritable) e).get());
          put(BasicTypes.DOUBLE.name(), e -> ((DoubleWritable) e).get());
          put(BasicTypes.BINARY.name(), e -> ((BytesWritable) e).copyBytes());
          put(BasicTypes.BOOLEAN.name(), e -> ((BooleanWritable) e).get());
          put(BasicTypes.BYTE.name(), e -> ((ByteWritable) e).get());
          put(BasicTypes.DATE.name(), e -> ((DateWritable) e).get());
          put(BasicTypes.FLOAT.name(), e -> ((FloatWritable) e).get());
          put(BasicTypes.SHORT.name(), e -> ((ShortWritable) e).get());
          put(BasicTypes.TIMESTAMP.name(), e -> ((TimestampWritable) e).getTimestamp());
          put(BasicTypes.BIG_DECIMAL.name(),
              e -> ((HiveDecimalWritable) e).getHiveDecimal().bigDecimalValue());
        }
      };

  public static final Map<String, Function<Object, Object>> ORCWriterFunctions =
      new HashMap<String, Function<Object, Object>>(20) {
        {
          put(BasicTypes.STRING.name(), e -> new Text(e.toString()));
          put(BasicTypes.VARCHAR.toString(), e -> new Text(e.toString()));
          put(BasicTypes.CHARS.toString(), e -> new Text(e.toString()));
          put(BasicTypes.CHAR.toString(),
              e -> new HiveCharWritable(new HiveChar(e.toString(), e.toString().length())));
          put(BasicTypes.O_INT.name(), e -> new IntWritable((int) e));
          put(BasicTypes.O_LONG.name(), e -> new LongWritable((long) e));
          put(BasicTypes.INT.name(), e -> new IntWritable((int) e));
          put(BasicTypes.LONG.name(), e -> new LongWritable((long) e));
          put(BasicTypes.DOUBLE.name(), e -> new DoubleWritable((double) e));
          put(BasicTypes.BINARY.name(), e -> new BytesWritable((byte[]) e));
          put(BasicTypes.BOOLEAN.name(), e -> new BooleanWritable((boolean) e));
          put(BasicTypes.BYTE.name(), e -> new ByteWritable((byte) e));
          put(BasicTypes.DATE.name(), e -> new DateWritable((Date) e));
          put(BasicTypes.FLOAT.name(), e -> new FloatWritable((float) e));
          put(BasicTypes.SHORT.name(), e -> new ShortWritable((short) e));
          put(BasicTypes.TIMESTAMP.name(), e -> new TimestampWritable((Timestamp) e));
          put(BasicTypes.BIG_DECIMAL.name(),
              e -> new HiveDecimalWritable(HiveDecimal.create(e.toString())));
        }
      };

  public static final Map<String, TypeInfo> ORCTYpeInfoMap = new HashMap<String, TypeInfo>(20) {
    {
      put(BasicTypes.STRING.toString(), TypeInfoFactory.stringTypeInfo);
      put(BasicTypes.VARCHAR.toString(), TypeInfoFactory.varcharTypeInfo);
      put(BasicTypes.CHARS.toString(), TypeInfoFactory.stringTypeInfo);
      put(BasicTypes.CHAR.toString(), TypeInfoFactory.charTypeInfo);
      put(BasicTypes.O_INT.toString(), TypeInfoFactory.intTypeInfo);
      put(BasicTypes.O_LONG.toString(), TypeInfoFactory.longTypeInfo);
      put(BasicTypes.INT.toString(), TypeInfoFactory.intTypeInfo);
      put(BasicTypes.LONG.toString(), TypeInfoFactory.longTypeInfo);
      put(BasicTypes.DOUBLE.toString(), TypeInfoFactory.doubleTypeInfo);
      put(BasicTypes.BINARY.toString(), TypeInfoFactory.binaryTypeInfo);
      put(BasicTypes.BOOLEAN.toString(), TypeInfoFactory.booleanTypeInfo);
      put(BasicTypes.BYTE.toString(), TypeInfoFactory.byteTypeInfo);
      put(BasicTypes.DATE.toString(), TypeInfoFactory.dateTypeInfo);
      put(BasicTypes.FLOAT.toString(), TypeInfoFactory.floatTypeInfo);
      put(BasicTypes.SHORT.toString(), TypeInfoFactory.shortTypeInfo);
      put(BasicTypes.TIMESTAMP.toString(), TypeInfoFactory.timestampTypeInfo);
      put(BasicTypes.BIG_DECIMAL.name(), TypeInfoFactory.decimalTypeInfo);

    }
  };

  public static final ConverterDescriptor getConverterDescriptor(
      final TableDescriptor tableDescriptor, FileFormats fileFormat) {
    if (fileFormat == FileFormats.ORC) {
      return new ORCConverterDescriptor(tableDescriptor);
    } else if (fileFormat == FileFormats.PARQUET) {
      return new ParquetConverterDescriptor(tableDescriptor);
    }

    return null;
  }

  public static TableDescriptor testTableDescriptor = null;

}
