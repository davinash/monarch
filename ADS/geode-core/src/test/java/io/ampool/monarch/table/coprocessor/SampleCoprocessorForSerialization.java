package io.ampool.monarch.table.coprocessor;

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.internal.logging.LogService;

import org.apache.logging.log4j.Logger;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.Get;

/**

 */
public class SampleCoprocessorForSerialization extends MCoprocessor {

  public SampleCoprocessorForSerialization() {}


  private static final Logger logger = LogService.getLogger();

  public boolean serializeMGet(MCoprocessorContext context) {
    MExecutionRequest request = context.getRequest();
    final Object object = request.getArguments();
    List argumentList = null;
    if (object instanceof List || object instanceof ArrayList) {
      argumentList = (ArrayList) object;
    }
    Get get = (Get) argumentList.get(0);
    byte[] rowKey = (byte[]) argumentList.get(1);

    // validate MGet object
    long timeStamp = Long.MAX_VALUE;
    List<byte[]> columnNameList = new ArrayList<>();
    columnNameList.add(Bytes.toBytes("COL1"));

    if (Bytes.compareTo(rowKey, get.getRowKey()) != 0) {
      System.out
          .println("SampleCoprocessorForSerialization.serializeMGet :: " + "RowKey exception");
      return false;
    }

    if (timeStamp != get.getTimeStamp()) {
      System.out
          .println("SampleCoprocessorForSerialization.serializeMGet :: " + "Timestamp exception");
      return false;
    }

    boolean allColNamesVerified = true;
    final List<byte[]> getCols = get.getColumnNameList();
    for (int i = 0; i < columnNameList.size(); i++) {
      if (Bytes.compareTo(columnNameList.get(i), getCols.get(i)) != 0) {
        System.out
            .println("SampleCoprocessorForSerialization.serializeMGet :: " + "Col list exception");
        return false;
      }
    }
    return true;
  }

  public boolean serializeMDelete(MCoprocessorContext context) {
    MExecutionRequest request = context.getRequest();
    final Object object = request.getArguments();
    List argumentList = null;
    if (object instanceof List || object instanceof ArrayList) {
      argumentList = (ArrayList) object;
    }
    Delete delete = (Delete) argumentList.get(0);
    byte[] rowKey = (byte[]) argumentList.get(1);

    // validate MDelete object
    long timeStamp = Long.MAX_VALUE;
    List<byte[]> columnNameList = new ArrayList<>();
    columnNameList.add(Bytes.toBytes("COL1"));

    if (Bytes.compareTo(rowKey, delete.getRowKey()) != 0) {
      System.out
          .println("SampleCoprocessorForSerialization.serializeMDelete :: " + "RowKey exception");
      return false;
    }

    if (timeStamp != delete.getTimestamp()) {
      System.out.println(
          "SampleCoprocessorForSerialization.serializeMDelete :: " + "Timestamp exception");
      return false;
    }

    boolean allColNamesVerified = true;
    final List<byte[]> getCols = delete.getColumnNameList();
    for (int i = 0; i < columnNameList.size(); i++) {
      if (Bytes.compareTo(columnNameList.get(i), getCols.get(i)) != 0) {
        System.out.println(
            "SampleCoprocessorForSerialization.serializeMDelete :: " + "Col list exception");
        return false;
      }
    }
    return true;
  }

}
