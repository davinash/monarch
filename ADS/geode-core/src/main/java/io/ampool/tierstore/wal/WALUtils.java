package io.ampool.tierstore.wal;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.internal.ServerRow;
import io.ampool.store.StoreHandler;
import io.ampool.store.StoreRecord;
import org.apache.geode.internal.GemFireVersion;

public class WALUtils {
  public static void verifyIntegrity(Path file, FileInputStream fis, Boolean failOnMismatch)
      throws IOException {

    byte[] nodeIdLenArr = new byte[Integer.SIZE / 8];
    int retVal = fis.read(nodeIdLenArr);
    if (retVal != Integer.SIZE / 8) {
      throw new IOException("Corrupted node Id len from file: " + file.toString());
    }

    int nodeIdLen = Bytes.toInt(nodeIdLenArr);
    byte[] nodeId = new byte[nodeIdLen + Integer.SIZE / 8];
    byte[] nodeName = new byte[nodeIdLen];
    retVal = fis.read(nodeName);
    if (retVal != nodeIdLen) {
      throw new IOException("Corrupted node Id from file: " + file.toString());
    }
    System.arraycopy(nodeIdLenArr, 0, nodeId, 0, Integer.SIZE / 8);
    System.arraycopy(nodeName, 0, nodeId, Integer.SIZE / 8, nodeName.length);
    if (failOnMismatch && Bytes.compareTo(WriteAheadLog.getNodeId(), nodeId) != 0) {
      throw new IOException("nodeId in WAL file(" + Arrays.toString(nodeId)
          + ") is different from the nodeId of the current node("
          + Arrays.toString(WriteAheadLog.getNodeId()) + ")");
    }
    byte[] reserved = new byte[4];
    retVal = fis.read(reserved);
    if (retVal != reserved.length) {
      throw new IOException("Corrupted reserved bytes from file: " + file.toString());
    }

    byte[] writerVersion = new byte[16];
    retVal = fis.read(writerVersion);
    if (retVal != writerVersion.length) {
      throw new IOException("Corrupted version from file: " + file.toString());
    }
    if (failOnMismatch && Bytes.toString(writerVersion)
        .compareTo(String.format("%16s", GemFireVersion.getAmpoolVersion())) != 0) {
      throw new IOException(
          "Version in file: " + file.toString() + " does not match the reader version");
    }
  }

  /**
   * Write the WAL records to the tier-store.
   *
   * @param tableName the table name
   * @param partitionId the partition/bucket id
   * @param td the table descriptor
   * @param wrs an array of WAL records to be written to tier-store
   * @throws IOException if failed to write records to the tier-store
   */
  static void writeToStore(final String tableName, final int partitionId, final TableDescriptor td,
      final WALRecord[] wrs) throws IOException {
    if (wrs == null || wrs.length == 0) {
      return;
    }
    final StoreHandler sh = StoreHandler.getInstance();
    final ServerRow row = ServerRow.create(td);
    final StoreRecord[] srs = Arrays.stream(wrs).map(wr -> wr.getRecordStream().map(rec -> {
      if (rec instanceof StoreRecord) {
        return (StoreRecord) rec;
      } else {
        row.reset(null, rec, wr.getEncoding(), null);
        return new StoreRecord(row);
      }
    })).flatMap(e -> e).toArray(StoreRecord[]::new);
    sh.writeToStore(tableName, partitionId, srs);
  }
}
