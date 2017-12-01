package io.ampool.monarch.cli;

import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.shell.GfshConfig;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;
import java.io.File;
import java.io.IOException;

@Category(MonarchTest.class)
public class GfshMashConfigTest extends GfshDunitHelper {

  public GfshMashConfigTest() {
    super();
  }

  private static final File HISTORY_FILE =
      new File(GfshDunitHelper.getHomeGemFireDirectory(), ".mash.history");
  private static final int MAX_HISTORY_SIZE = 500;

  protected void verifyMashShellConfigs() throws ClassNotFoundException, IOException {
    Gfsh shell = Gfsh.getInstance(true, false, null, new GfshConfig(HISTORY_FILE.getAbsolutePath(),
        MashLauncher.DEFAULT_PROMPT, MAX_HISTORY_SIZE, null, null, null, null, null));
    assertFalse(shell.isDefaultShell());
    assertEquals("{0}mash{1}>", shell.getGfshConfig().getDefaultPrompt());

    // TODO test command availability
  }

  protected void verifyGfshShellConfigs() throws ClassNotFoundException, IOException {
    Gfsh shell = Gfsh.getInstance(true, true, null, new GfshConfig());
    assertTrue(shell.isDefaultShell());
    assertEquals("{0}gfsh{1}>", shell.getGfshConfig().getDefaultPrompt());

    // TODO test command availability
  }

  private void checkGfshConfigsOnserver(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        verifyGfshShellConfigs();
        return null;
      }
    });
  }

  private void checkMashConfigsOnserver(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        verifyMashShellConfigs();
        return null;
      }
    });
  }

  @Test
  public void testGfshConfigs() throws ClassNotFoundException, IOException {
    checkGfshConfigsOnserver(this.vm_0);
    checkMashConfigsOnserver(this.vm_1);
  }

}
