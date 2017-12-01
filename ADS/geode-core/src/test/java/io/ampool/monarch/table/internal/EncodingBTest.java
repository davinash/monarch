package io.ampool.monarch.table.internal;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.types.TypeHelper;
import io.ampool.monarch.types.TypeUtils;
import io.ampool.store.StoreRecord;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category(MonarchTest.class)
@RunWith(JUnitParamsRunner.class)
public class EncodingBTest {
  private static final EncodingB encoding = new EncodingB();

  private static MTableDescriptor tdMixed = new MTableDescriptor(MTableType.UNORDERED);
  private static MTableDescriptor tdFixed = new MTableDescriptor(MTableType.UNORDERED);
  private static MTableDescriptor tdVariable = new MTableDescriptor(MTableType.ORDERED_VERSIONED);


  static {
    tdMixed.setSchema(
        Schema.fromString("struct<c1:LONG,c2:STRING,c3:INT,__ROW__KEY__COLUMN__:BINARY>"));
    tdFixed.setSchema(Schema.fromString("struct<c1:INT,c2:LONG,c3:FLOAT>"));
    tdVariable.setSchema(Schema.fromString("struct<c1:STRING,c2:STRING,c3:BINARY>"));
  }

  private Object[] dataSerializeValue() {
    return new Object[][] {{tdMixed, new Object[] {11L, "ABC", 111}, //
        new byte[] {0, 0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 111, 65, 66, 67, 0, 0, 0, 15, 0, 0, 0, 12} //
        }, {tdMixed, new Object[] {null, "A", 222, new byte[] {66}}, //
            new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -34, 65, 66, 0, 0, 0, 13, 0, 0, 0, 12} //
        },
        {tdMixed, new Object[] {null, "A", 222, new byte[] {11, 22, 33}}, //
            new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -34, 65, 11, 22, 33, 0, 0, 0, 13, 0, 0, 0,
                12} //
        }, {tdMixed, new Object[] {null, null, 333, new byte[] {1, 2, 3}}, //
            new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 77, 1, 2, 3, 0, 0, 0, 12, 0, 0, 0, 12} //
        }, {tdMixed, new Object[] {333L, "AB", null, new byte[] {1, 2, 3}}, //
            new byte[] {0, 0, 0, 0, 0, 0, 1, 77, 0, 0, 0, 0, 65, 66, 1, 2, 3, 0, 0, 0, 14, 0, 0, 0,
                12} //
        }, {tdFixed, new Object[] {null, 0L, null}, //
            new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0} //
        }, {tdFixed, new Object[] {null, null, 1.1f}, //
            new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 63, -116, -52, -51} //
        }, {tdFixed, new Object[] {11, 222L, null}, //
            new byte[] {0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, -34, 0, 0, 0, 0} //
        }, {tdFixed, new Object[] {222, 11L, 4.4f}, //
            new byte[] {0, 0, 0, -34, 0, 0, 0, 0, 0, 0, 0, 11, 64, -116, -52, -51} //
        }, {tdFixed, new Object[] {null, null, null}, //
            new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0} //
        }, {tdVariable, new Object[] {null, "ABC", null}, //
            new byte[] {65, 66, 67, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0} //
        }, {tdVariable, new Object[] {null, null, new byte[] {0, 1, 2}}, //
            new byte[] {0, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0} //
        }, {tdVariable, new Object[] {"ABC", "ABC", null}, //
            new byte[] {65, 66, 67, 65, 66, 67, 0, 0, 0, 6, 0, 0, 0, 3, 0, 0, 0, 0} //
        }, {tdVariable, new Object[] {"ABC", "ABC", new byte[] {0, 1, 2}}, //
            new byte[] {65, 66, 67, 65, 66, 67, 0, 1, 2, 0, 0, 0, 6, 0, 0, 0, 3, 0, 0, 0, 0} //
        }, {tdVariable, new Object[] {null, null, null}, //
            new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0} //
        },};
  }

  @Test
  @Parameters(method = "dataSerializeValue")
  public void testSerializeValue(final TableDescriptor td, final Object[] values,
      final byte[] expected) {
    Record rec = new Record();
    for (int i = 0; i < values.length; i++) {
      rec.add(td.getSchema().getColumnDescriptorByIndex(i).getColumnName(), values[i]);
    }
    final byte[] bytes = (byte[]) encoding.serializeValue(td, rec);
    assertArrayEquals(expected, bytes);
  }

  @Test
  public void testInitFullRow() throws IOException {
    FTableDescriptor td = new FTableDescriptor();
    td.setSchema(tdMixed.getSchema());

    Encoding enc = new EncodingB();
    final int count = 5;
    final StoreRecord[] records = new StoreRecord[count];
    final List<MColumnDescriptor> cds = td.getAllColumnDescriptors();
    HeapDataOutputStream hdos = new HeapDataOutputStream(1024, null, true);
    for (int i = 0; i < count; i++) {
      StoreRecord record = new StoreRecord(td.getNumOfColumns());
      for (MColumnDescriptor cd : cds) {
        record.addValue(TypeUtils.getRandomValue(cd.getColumnType()));
      }
      records[i] = record;
      byte[] bytes = (byte[]) enc.serializeValue(td, record);
      hdos.writeInt(bytes.length);
      hdos.write(bytes);
    }

    final byte[] singleByteArray = hdos.toByteArray();
    ServerRow row = ServerRow.create(td);
    int offset = 0;
    int i = 0;
    while (offset < singleByteArray.length) {
      final int len = Bytes.toInt(singleByteArray, offset);
      offset += Bytes.SIZEOF_INT;
      row.reset(null, singleByteArray, enc, offset, len);
      offset += len;
      final StoreRecord record = records[i++];
      for (int j = 0; j < record.getValues().length; j++) {
        assertEquals("Incorrect value for: row= " + i + ", column= " + j,
            TypeHelper.deepToString(record.getValue(j)), TypeHelper.deepToString(row.getValue(j)));
      }
    }
  }
}
