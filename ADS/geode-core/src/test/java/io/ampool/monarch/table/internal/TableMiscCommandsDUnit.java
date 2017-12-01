package io.ampool.monarch.table.internal;


import io.ampool.monarch.cli.MashCliStrings;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.exceptions.TierStoreNotAvailableException;
import io.ampool.monarch.types.TypeUtils;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.geode.cache.*;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.ClassBuilder;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.commands.AMPLCliCommandTestBase;
import org.apache.geode.management.internal.cli.commands.CliCommandTestBase;
import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category(MonarchTest.class)
@RunWith(JUnitParamsRunner.class)
public class TableMiscCommandsDUnit extends AMPLCliCommandTestBase {

  final List<String> filesToBeDeleted = new CopyOnWriteArrayList<String>();

  private static final String columnSchema = TypeUtils.getJsonSchema(
      new String[][] {{"COL1", "INT"}, {"COL2", "INT"}, {"COL3", "INT"}, {"COL4", "INT"}});


  private String getTableTypeStr(TableType tableType) {
    String tableTypeStr = null;
    if (tableType == TableType.ORDERED_VERSIONED) {
      tableTypeStr = "ORDERED_VERSIONED";
    } else if (tableType == TableType.UNORDERED) {
      tableTypeStr = "UNORDERED";
    } else {
      tableTypeStr = "IMMUTABLE";
    }
    return tableTypeStr;
  }

  private File buildTemporaryJar1() throws IOException {
    final String OBSERVER_CLASS_STRING =
        "package io.ampool;" + "import io.ampool.monarch.table.coprocessor.MBaseRegionObserver;"
            + "public class TestObserver extends MBaseRegionObserver {}";

    ClassBuilder classBuilder = new ClassBuilder();
    // classBuilder.addToClassPath(".");
    final File prJarFile =
        new File(temporaryFolder.getRoot().getCanonicalPath() + File.separator, "myObserver.jar");
    this.filesToBeDeleted.add(prJarFile.getAbsolutePath());
    byte[] jarBytes =
        classBuilder.createJarFromClassContent("io/ampool/TestObserver", OBSERVER_CLASS_STRING);
    writeJarBytesToFile(prJarFile, jarBytes);
    return prJarFile;
  }

  private File buildTemporaryJar2() throws IOException {
    final String OBSERVER_CLASS_STRING =
        "package io.ampool;" + "import io.ampool.monarch.table.coprocessor.MBaseRegionObserver;"
            + "public class TestObserver2 extends MBaseRegionObserver {}";

    ClassBuilder classBuilder = new ClassBuilder();
    // classBuilder.addToClassPath(".");
    final File prJarFile =
        new File(temporaryFolder.getRoot().getCanonicalPath() + File.separator, "myObserver2.jar");
    this.filesToBeDeleted.add(prJarFile.getAbsolutePath());
    byte[] jarBytes =
        classBuilder.createJarFromClassContent("io/ampool/TestObserver2", OBSERVER_CLASS_STRING);
    writeJarBytesToFile(prJarFile, jarBytes);
    return prJarFile;
  }


  private CommandStringBuilder getCommandStringBuilder(String tableTypeStr,
      final String tableName) {
    CommandStringBuilder commandStringBuilder =
        new CommandStringBuilder(MashCliStrings.CREATE_MTABLE);
    commandStringBuilder.addOption(MashCliStrings.CREATE_MTABLE__NAME, tableName);
    commandStringBuilder.addOption(MashCliStrings.CREATE_MTABLE__TYPE, tableTypeStr);
    commandStringBuilder.addOption(MashCliStrings.CREATE_MTABLE_SCHEMA, columnSchema);
    return commandStringBuilder;
  }

  private void createACacheInVm1(VM vm) {
    vm.invoke(() -> {
      System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE",
          "true");
      assertNotNull(MCacheFactory.create(getSystem()));
      // assertNotNull(getCache());
      // MCacheFactory.getAnyInstance();
    });
  }


  private TableType[] tableType() {
    return new TableType[] {TableType.ORDERED_VERSIONED, TableType.UNORDERED, TableType.IMMUTABLE};
  }

  private void writeJarBytesToFile(File jarFile, byte[] jarBytes) throws IOException {
    final OutputStream outStream = new FileOutputStream(jarFile);
    outStream.write(jarBytes);
    outStream.flush();
    outStream.close();
  }

  @Test
  @Parameters(method = "tableType")
  public void testCoProcessorInTableDescriptor(TableType tableType)
      throws IOException, GfJsonException {
    if (tableType == TableType.IMMUTABLE) {
      return;
    }
    final String tableName = "TableWithSingleCoProc";
    setUpJmxManagerOnVm0ThenConnect(null);
    VM vm = Host.getHost(0).getVM(1);
    // Create a cache in vm 1
    createACacheInVm1(vm);

    final File prJarFile = buildTemporaryJar1();
    CommandResult cmdResult = executeCommand("deploy --jar=" + prJarFile.getAbsolutePath());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    String tableTypeStr = getTableTypeStr(tableType);

    CommandStringBuilder commandStringBuilder = getCommandStringBuilder(tableTypeStr, tableName);
    commandStringBuilder.addOption(MashCliStrings.CREATE_MTABLE__OBSERVER_COPROCESSORS,
        "io.ampool.TestObserver");

    CommandResult cmdResult1 = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult1.getStatus());

    vm.invoke(() -> {
      MCache ampoolServerCache = MCacheFactory.getAnyInstance();
      MTable table = ampoolServerCache.getMTable(tableName);
      ArrayList<String> coprocessorList = table.getTableDescriptor().getCoprocessorList();
      assertEquals(1, coprocessorList.size());
      assertEquals("io.ampool.TestObserver", coprocessorList.get(0));
    });
  }

  @Test
  @Parameters(method = "tableType")
  public void testMultipleCoProcessorInTableDescriptor(TableType tableType) throws IOException {
    if (tableType == TableType.IMMUTABLE) {
      return;
    }
    final String tableName = "TableWithMultiCoProc";
    setUpJmxManagerOnVm0ThenConnect(null);
    VM vm = Host.getHost(0).getVM(1);
    // Create a cache in vm 1
    createACacheInVm1(vm);

    final File prJarFile = buildTemporaryJar1();
    CommandResult cmdResult = executeCommand("deploy --jar=" + prJarFile.getAbsolutePath());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    final File prJarFile1 = buildTemporaryJar2();
    CommandResult cmdResult1 = executeCommand("deploy --jar=" + prJarFile1.getAbsolutePath());
    assertEquals(Result.Status.OK, cmdResult1.getStatus());

    String tableTypeStr = getTableTypeStr(tableType);

    CommandStringBuilder commandStringBuilder = getCommandStringBuilder(tableTypeStr, tableName);
    commandStringBuilder.addOption(MashCliStrings.CREATE_MTABLE__OBSERVER_COPROCESSORS,
        "io.ampool.TestObserver,io.ampool.TestObserver2");

    CommandResult cmdResult2 = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult2.getStatus());

    vm.invoke(() -> {
      MCache ampoolServerCache = MCacheFactory.getAnyInstance();
      MTable table = ampoolServerCache.getMTable(tableName);
      ArrayList<String> coprocessorList = table.getTableDescriptor().getCoprocessorList();
      assertEquals(2, coprocessorList.size());
      assertEquals("io.ampool.TestObserver", coprocessorList.get(0));
      assertEquals("io.ampool.TestObserver2", coprocessorList.get(1));
    });
  }

  @Test
  @Parameters(method = "tableType")
  public void testFTablePartitioningColumn(TableType tableType) throws IOException {
    if (tableType != TableType.IMMUTABLE) {
      return;
    }
    IgnoredException.addIgnoredException(TierStoreNotAvailableException.class.getName());
    final String tableName = "FTableWithPartitioningColumn";
    setUpJmxManagerOnVm0ThenConnect(null);
    VM vm = Host.getHost(0).getVM(1);
    // Create a cache in vm 1
    createACacheInVm1(vm);

    String tableTypeStr = getTableTypeStr(tableType);

    CommandStringBuilder commandStringBuilder = getCommandStringBuilder(tableTypeStr, tableName);
    commandStringBuilder.addOption(MashCliStrings.CREATE_MTABLE__FTABLE_PARTITION_COLUMN_NAME,
        "COL1");

    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    vm.invoke(() -> {
      MCache ampoolServerCache = MCacheFactory.getAnyInstance();
      FTable table = ampoolServerCache.getFTable(tableName);
      assertEquals(new ByteArrayKey(Bytes.toBytes("COL1")),
          table.getTableDescriptor().getPartitioningColumn());
    });
    IgnoredException.removeAllExpectedExceptions();
  }


  private File buildCacheLoaderJar() throws IOException {
    final String TABLE_LOADER_CLASS_STRING = "package io.ampool;"
        + "public class MyCacheLoader implements org.apache.geode.cache.CacheLoader {"
        + "  @Override" + " public void close() {" + " }" + " @Override"
        + " public Object load(org.apache.geode.cache.LoaderHelper helper) throws org.apache.geode.cache.CacheLoaderException {"
        + "   return null;" + " }" + "}";

    ClassBuilder classBuilder = new ClassBuilder();
    // classBuilder.addToClassPath(".");
    final File prJarFile = new File(temporaryFolder.getRoot().getCanonicalPath() + File.separator,
        "myTableLoader.jar");
    this.filesToBeDeleted.add(prJarFile.getAbsolutePath());
    byte[] jarBytes = classBuilder.createJarFromClassContent("io/ampool/MyCacheLoader",
        TABLE_LOADER_CLASS_STRING);
    writeJarBytesToFile(prJarFile, jarBytes);
    return prJarFile;
  }


  @Test
  @Parameters(method = "tableType")
  public void testTableCacheLoaderCommand(TableType tableType) throws IOException {
    if (tableType == TableType.IMMUTABLE) {
      return;
    }
    final String tableName = "MTableWithCacheLoader";
    setUpJmxManagerOnVm0ThenConnect(null);
    VM vm = Host.getHost(0).getVM(1);
    // Create a cache in vm 1
    createACacheInVm1(vm);

    final File prJarFile = buildCacheLoaderJar();
    CommandResult cmdResult = executeCommand("deploy --jar=" + prJarFile.getAbsolutePath());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    String tableTypeStr = getTableTypeStr(tableType);
    CommandStringBuilder commandStringBuilder = getCommandStringBuilder(tableTypeStr, tableName);
    commandStringBuilder.addOption(MashCliStrings.CREATE_MTABLE__CACHE_LOADER_CLASS,
        "io.ampool.MyCacheLoader");

    CommandResult cmdResult2 = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult2.getStatus());

    vm.invoke(() -> {
      MCache ampoolServerCache = MCacheFactory.getAnyInstance();
      MTable table = ampoolServerCache.getMTable(tableName);
      assertEquals("io.ampool.MyCacheLoader", table.getTableDescriptor().getCacheLoaderClassName());

      PartitionedRegion pr =
          (PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(tableName);
      assertNotNull(pr.getAttributes().getCacheLoader());
      assertEquals("io.ampool.MyCacheLoader",
          pr.getAttributes().getCacheLoader().getClass().getName());
    });
  }
}
