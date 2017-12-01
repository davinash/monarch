package io.ampool.monarch.cli;

import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.shell.GfshConfig;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.logging.Level;

import static org.junit.Assert.assertTrue;

@Category(MonarchTest.class)
public class MashShellCommandJUnitTest {

  private static Gfsh shell;
  public static final String DEFAULT_PROMPT = "mash";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    shell = Gfsh.getInstance(true, false, null,
        new GfshConfig("/tmp", DEFAULT_PROMPT, 100, null, Level.FINE, null, null, null));

  }

  @Test
  @SuppressWarnings("unused")
  public void testCommandsLoaded() {
    ArrayList<String> commandNames = new ArrayList<String>();
    commandNames = (ArrayList<String>) shell.getCommandNames("");
    assertTrue(commandNames.contains("create table"));
    assertTrue(commandNames.contains("list tables"));
    assertTrue(commandNames.contains("describe table"));
    assertTrue(commandNames.contains("delete table"));
  }


  @After
  public void tearDown() throws Exception {
    shell.stop();
  }

}
