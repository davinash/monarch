package io.ampool.monarch.cli;

import static org.junit.Assert.assertTrue;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MonarchTest.class)
public class MashLauncherJUnitTest {
  @Test
  public void testGlobalOptions() {
    MashLauncher launcher = new MashLauncher();
    launcher.parseCommandLineOptions(new String[] {"-e", "-e", "-g", "key1=value1,key2=value2"});
    assertTrue(MashLauncher.getGlobalOption("key1").equals("value1"));
    assertTrue(MashLauncher.getGlobalOption("key2").equals("value2"));
  }

  @Test
  public void testGlobalOptionsWithWhitespaces() {
    MashLauncher launcher = new MashLauncher();
    launcher.parseCommandLineOptions(
        new String[] {"-e", "-e", "-g", "     key1=   value1,      key2    = value2 "});
    assertTrue(MashLauncher.getGlobalOption("key1").equals("value1"));
    assertTrue(MashLauncher.getGlobalOption("key2").equals("value2"));
  }
}
