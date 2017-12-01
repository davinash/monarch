/*
* Copyright (c) 2017 Ampool, Inc. All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License"); you
* may not use this file except in compliance with the License. You
* may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
* implied. See the License for the specific language governing
* permissions and limitations under the License. See accompanying
* LICENSE file.
*/
package io.ampool.presto.connector;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.primitives.Bytes;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.ampool.monarch.table.Row;
import org.apache.commons.lang3.time.FastDateFormat;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

public class AmpoolRecordCursor implements RecordCursor
{
    private static final Logger log = Logger.get(AmpoolRecordCursor.class);

    private static final TimeZone gmt = TimeZone.getTimeZone("GMT");
    private static final FastDateFormat TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt);
    private static final FastDateFormat TIME_FORMAT_TIMESTAMP = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", gmt);
    public static final long MILLIS_PER_DAY = 86400000L;

    private final List<AmpoolColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;

    private final Iterator<Row> rows;

    private Row fields;

    public AmpoolRecordCursor(List<AmpoolColumnHandle> columnHandles, Iterator<Row> iterator)
    {
        this.columnHandles = columnHandles;
        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            AmpoolColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }
        rows = iterator;

        log.info("INFORMATION: AmpoolMetadata created successfully.");
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int i)
    {
        log.info("INFORMATION: AmpoolRecordCursor getType() called.");
        checkArgument(i < columnHandles.size(), "Invalid field index");
        return columnHandles.get(i).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        log.info("INFORMATION: AmpoolRecordCursor advanceNextPosition() called.");
        if (!rows.hasNext()) {
            return false;
        }
        fields = rows.next();
        return true;
    }

    private String getFieldValue(int field)
    {
        log.info("INFORMATION: AmpoolRecordCursor getFieldValue() called.");
        checkState(fields != null, "Cursor has not been advanced yet");

        int columnIndex = fieldToColumnIndex[field];
        return fields.getCells().get(columnIndex).getColumnValue().toString();
    }

    @Override
    public boolean getBoolean(int i)
    {
        log.info("INFORMATION: AmpoolRecordCursor getBoolean() called.");
        checkFieldType(i, BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(i));
    }

    @Override
    public long getLong(int i)
    {
        log.info("INFORMATION: AmpoolRecordCursor getLong() called.");
        if (getType(i).equals(TIMESTAMP))
        {
            try
            {
                Date date = TIME_FORMAT_TIMESTAMP.parse(getFieldValue(i));
                return date.getTime();
            }
            catch (ParseException e)
            {
                log.debug("WARNING: TIMESTAMP not handled correctly.");
                return 0;
            }
        }
        else if (getType(i).equals(DATE))
        {
            try
            {
                Date date = TIME_FORMAT_DATE.parse(getFieldValue(i));
                return (int) (date.getTime() / MILLIS_PER_DAY);
            }
            catch (ParseException e)
            {
                log.debug("WARNING: DATE not handled correctly.");
                return 0;
            }
        }
        else
        {
            checkFieldType(i, BIGINT, INTEGER);
            return Long.parseLong(getFieldValue(i));
        }
    }

    @Override
    public double getDouble(int i)
    {
        log.info("INFORMATION: AmpoolRecordCursor getDouble() called.");
        checkFieldType(i, DOUBLE, REAL);
        return Double.parseDouble(getFieldValue(i));
    }

    @Override
    public Slice getSlice(int i)
    {
        log.info("INFORMATION: AmpoolRecordCursor getSlice() called.");
        if (getType(i).equals(VARBINARY))
        {
            return Slices.utf8Slice(getFieldValue(i));
        }
        checkFieldType(i, createUnboundedVarcharType());
        return Slices.utf8Slice(getFieldValue(i));
    }

    @Override
    public Object getObject(int i)
    {
        log.info("INFORMATION: AmpoolRecordCursor getObject() called.");
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int i)
    {
        log.info("INFORMATION: AmpoolRecordCursor isNull() called.");
        checkArgument(i < columnHandles.size(), "Invalid field index");
        return Strings.isNullOrEmpty(getFieldValue(i));
    }

    private void checkFieldType(int field, Type... expected)
    {
        log.info("INFORMATION: AmpoolRecordCursor checkFieldType() called.");

        Type actual = getType(field);
        for (Type type : expected) {
            if (actual.equals(type)) {
                return;
            }
        }
        String expectedTypes = Joiner.on(", ").join(expected);
        throw new IllegalArgumentException(format("Expected field %s to be type %s but is %s", field, expectedTypes, actual));
    }

    @Override
    public void close()
    {
        log.info("INFORMATION: AmpoolRecordCursor close() called.");
        //Do nothing
    }
}
