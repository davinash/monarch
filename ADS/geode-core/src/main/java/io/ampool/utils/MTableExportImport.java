package io.ampool.utils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MConfiguration;
import io.ampool.monarch.table.MDiskWritePolicy;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.internal.CellRef;
import io.ampool.monarch.types.BasicTypes;

public class MTableExportImport {

  public class Filter {
    public File[] finder(String dirName) {
      File dir = new File(dirName);
      return dir.listFiles(new FilenameFilter() {
        public boolean accept(File dir, String filename) {
          return filename.endsWith(".schema");
        }
      });
    }
  }


  private void createAndPopulateMTables(final MClientCache clientCache) {
    for (int i = 0; i < 10; i++) {
      MTableDescriptor mtd = new MTableDescriptor(MTableType.UNORDERED);
      mtd.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
      for (int j = 0; j < 10; j++) {
        mtd.addColumn(Bytes.toBytes("Column_" + j), BasicTypes.INT);
      }
      if (clientCache.getMTable("Table__" + i) != null) {
        clientCache.getAdmin().deleteMTable("Table__" + i);
      }

      MTable table = clientCache.getAdmin().createMTable("Table__" + i, mtd);

      for (int k = 0; k < 10; k++) {
        Put put = new Put(Bytes.toBytes("RowKey-" + k));
        for (int j = 0; j < 10; j++) {
          put.addColumn(Bytes.toBytes("Column_" + j), Bytes.toBytes(j));
        }
        table.put(put);
      }
    }
  }


  private void usage(final String issue) {
    System.out.println("");
    if (issue != null) {
      System.out.println("Error in parsing arguments near " + issue);
      System.out.println("");
    }

    System.out.println("\nUsage: MTableExportImport [options]\n\n" + "valid options:\n"
        + "--locatorHost <hostName>\t\t\tlocator host name or Address\n"
        + "--locatorPort <port>\t\t\t\tport at which Locator is running\n"
        + "--import\t\t\t\t\t\t\tImport From MTables to FTable\n"
        + "--export\t\t\t\t\t\t\tExport from MTable to Disk\n"
        + "--blockSize <blockSize>\t\t\t\tSpecifify FTable BlockSize\n"
        + "--dir<directory path>\t\t\t\tdirectory path to store exported MTables\n"
        + "--keyColumnName<Name of Key Column>\tIf passed then MTable Row Key will be used as special column in FTable\n"
        + "--suffix <suffix>\t\t\t\t\tSuffix to be added to FTable Name\n"
        + "--partitionColumn <partitionColumn>\tThis column will be used for Partition\n"
        + "--help\t\t\t\t\t\t\t\tPrint this help message\n");

    System.exit(1);
  }

  private void validateArguments(boolean importOperation, boolean exportOperation,
      String locatorHost, int locatorPort, String directoryPath, int blockSize,
      String keyColumnName) {
    if (importOperation == false && exportOperation == false) {
      System.out.println("No Operation Selected ");
      usage(null);
    }
    File f = new File(directoryPath);
    if (!(f.exists() && f.isDirectory())) {
      System.out.println("Directory " + directoryPath + " Does not Exists");
      usage(null);
    }

    if (importOperation) {
      System.out.println("Operation = Import");
    } else if (exportOperation) {
      System.out.println("Operation = Export");
    }
    System.out.println("Locator Host = " + locatorHost);
    System.out.println("Locator Port = " + locatorPort);
    System.out.println("Staging Directory = " + directoryPath);
    if (blockSize != 0) {
      System.out.println("Block Size = " + blockSize);
    }
    if (keyColumnName != null) {
      System.out.println("keyColumnName = " + keyColumnName);
    }

  }

  public static void main(String[] args) throws IOException {
    final MTableExportImport exportImport = new MTableExportImport();
    boolean importOperation = false;
    boolean exportOperation = false;
    String directoryPath = null;
    int blockSize = 0;
    String keyColumnName = null;
    String locatorHost = null;
    int locatorPort = 0;
    String suffix = null;
    String partitionColumn = null;

    if (args.length > 0) {
      int index = 0;
      try {
        while (index < args.length) {
          if (args[index].equals("--import")) {
            importOperation = true;
          } else if (args[index].equals("--export")) {
            exportOperation = true;
          } else if (args[index].equals("--locatorHost")) {
            locatorHost = args[index + 1];
            index += 1;
          } else if (args[index].equals("--locatorPort")) {
            locatorPort = new Integer(args[index + 1]).intValue();
            index += 1;
          } else if (args[index].equals("--dir")) {
            directoryPath = args[index + 1];
            index += 1;
          } else if (args[index].equals("--suffix")) {
            suffix = args[index + 1];
            index += 1;
          } else if (args[index].equals("--partitionColumn")) {
            partitionColumn = args[index + 1];
            index += 1;
          } else if (args[index].equals("--blockSize")) {
            blockSize = new Integer(args[index + 1]).intValue();
            index += 1;
          } else if (args[index].equals("--keyColumnName")) {
            keyColumnName = args[index + 1];
            index += 1;
          } else if (args[index].equals("--usage") || args[index].equals("--help")
              || args[index].equals("-help")) {
            exportImport.usage(null);
          }
          index += 1;
        }
      } catch (Exception exc) {
        System.out.println("Exc " + exc.toString());
        exportImport.usage(args[index]);
      }
    } else {
      exportImport.usage(null);
    }

    exportImport.validateArguments(importOperation, exportOperation, locatorHost, locatorPort,
        directoryPath, blockSize, keyColumnName);

    MClientCache clientCache =
        new MClientCacheFactory().addPoolLocator(locatorHost, locatorPort).create();


    if (importOperation) {
      exportImport.importFromMTableToFTable(clientCache, directoryPath, suffix, partitionColumn,
          blockSize, keyColumnName);
    } else if (exportOperation) {
      exportImport.exportFromMTable(clientCache, directoryPath);
    } else {
      exportImport.usage(null);
    }
  }

  private void importFromMTableToFTable(MClientCache clientCache, final String toDirectory,
      String suffix, final String partitionColumn, final int blockSize, final String keyColumnName)
      throws IOException {
    final File[] files = new Filter().finder(toDirectory);
    for (File file : files) {
      System.out.println("Importing Tables from " + file.getAbsolutePath());
      String tableName = file.getName();
      int pos = tableName.lastIndexOf(".");
      tableName = tableName.substring(0, pos);

      FTableDescriptor ftd = new FTableDescriptor();
      if (partitionColumn != null) {
        ftd.setPartitioningColumn(Bytes.toBytes(partitionColumn));
      }
      if (blockSize != 0) {
        ftd.setBlockSize(blockSize);
      }

      try (BufferedReader br = new BufferedReader(new FileReader(file))) {
        String line;
        while ((line = br.readLine()) != null) {
          final String[] columnNameAndType = line.split("\\#");
          ftd.addColumn(columnNameAndType[0], columnNameAndType[1]);
        }
      }
      if (keyColumnName != null) {
        ftd.addColumn("KEY_COLUMN_NAME");
      }

      if (suffix == null) {
        suffix = "";
      }

      if (clientCache.getFTable(tableName + suffix) != null) {
        clientCache.getAdmin().deleteFTable(tableName + suffix);
      }

      final FTable table = clientCache.getAdmin().createFTable(tableName + suffix, ftd);
      FileInputStream fis = new FileInputStream(toDirectory + "/" + tableName + ".data");
      File f = new File(toDirectory + "/" + tableName + ".data");
      BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f));
      DataInputStream dis = new DataInputStream(bis);

      int length = 0;
      final List<MColumnDescriptor> columnDescriptorList = ftd.getAllColumnDescriptors();
      try {
        while (true) {
          length = dis.readInt();
          if (length < 0)
            break;

          byte[] rowId = new byte[length];
          dis.read(rowId);

          Record record = new Record();

          length = dis.readInt();
          final long timeStamp = dis.readLong();

          if (keyColumnName != null) {
            for (MColumnDescriptor columnDescriptor : columnDescriptorList) {
              if (Bytes.compareTo(Bytes.toBytes(keyColumnName),
                  columnDescriptor.getColumnName()) != 0) {
                length = dis.readInt();
                byte[] value = new byte[length];
                dis.read(value);
                record.add(columnDescriptor.getColumnName(), value);
              }
            }
          } else {
            for (MColumnDescriptor columnDescriptor : columnDescriptorList) {
              length = dis.readInt();
              byte[] value = new byte[length];
              dis.read(value);
              record.add(columnDescriptor.getColumnName(), value);
            }
          }
          if (keyColumnName != null) {
            record.add(keyColumnName, rowId);
          }
          table.append(record);
        }
      } catch (EOFException eof) {
      }
    }
  }

  private void exportFromMTable(final MClientCache clientCache, final String toDirectory)
      throws IOException {
    final String[] listTableNames = clientCache.getAdmin().listTableNames();

    for (String tableName : listTableNames) {
      System.out.println("Exporting Table -> " + tableName);
      PrintWriter pw = new PrintWriter(toDirectory + "/" + tableName + ".schema");

      final MTable table = clientCache.getTable(tableName);
      for (MColumnDescriptor columnDescriptor : table.getTableDescriptor()
          .getAllColumnDescriptors()) {
        pw.write(columnDescriptor.getColumnNameAsString() + "#"
            + columnDescriptor.getColumnType().toString());
        pw.println();
      }
      pw.close();

      FileOutputStream fos = new FileOutputStream(toDirectory + "/" + tableName + ".data");

      for (Row row : table.getScanner(new Scan())) {
        final byte[] rowId = row.getRowId();
        final byte[] timeStamp = Bytes.toBytes(row.getRowTimeStamp());

        fos.write(Bytes.toBytes(rowId.length));
        fos.write(rowId);

        fos.write(Bytes.toBytes(timeStamp.length));
        fos.write(timeStamp);

        for (Cell cell : row.getCells()) {
          final byte[] valueArrayCopy = ((CellRef) cell).getValueArrayCopy();
          fos.write(Bytes.toBytes(valueArrayCopy.length));
          fos.write(valueArrayCopy);
        }
      }
      fos.close();
    }
  }
}
