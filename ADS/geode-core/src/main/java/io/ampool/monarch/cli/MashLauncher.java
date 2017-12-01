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
package io.ampool.monarch.cli;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.PureJavaMode;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.parser.SyntaxConstants;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.shell.GfshConfig;
import org.springframework.shell.core.ExitShellRequest;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

/**
 * Launcher class for :
 * <ul>
 * <li>gfsh 7.0
 * <li>server
 * <li>locator
 * <li>Tools (Pulse, VSD, JConsole, JVisualVM)
 * <li>Running Command Line Interface (CLI) Commands from OS prompt like
 * <ol>
 * <li>
 * <ul>
 * <li>compact offline-disk-store - Compact an offline disk store. If the disk store is large,
 * additional memory may need to be allocated to the process using the --J=-Xmx??? parameter.
 * <li>describe offline-disk-store - Display information about an offline disk store.
 * <li>encrypt password - Encrypt a password for use in data source configuration.
 * <li>run - Execute a set of MASH commands. Commands that normally prompt for additional input will
 * instead use default values.
 * <li>start jconsole - Start the JDK's JConsole tool in a separate process. JConsole will be
 * launched, but connecting to GemFire must be done manually.
 * <li>start jvisualvm - Start the JDK's Java VisualVM (jvisualvm) tool in a separate process. Java
 * VisualVM will be launched, but connecting to GemFire must be done manually.
 * <li>start locator - Start a Locator.
 * <li>start pulse - Open a new window in the default Web browser with the URL for the Pulse
 * application.
 * <li>start server - Start a GemFire Cache Server.
 * <li>start vsd - Start VSD in a separate process.
 * <li>status locator - Display the status of a Locator. Possible statuses are: started, online,
 * offline or not responding.
 * <li>status server - Display the status of a GemFire Cache Server.
 * <li>stop locator - Stop a Locator.
 * <li>stop server - Stop a GemFire Cache Server.
 * <li>validate offline-disk-store - Scan the contents of a disk store to verify that it has no
 * errors.
 * <li>version - Display product version information.
 * </ul>
 * </li>
 * <li>multiple commands specified using an option "-e"
 * </ol>
 * </ul>
 *
 *
 * @since 7.0
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public final class MashLauncher {
  // Shell configs
  public static final String INIT_FILE_PROPERTY = "mash.init-file";
  private static final String LOG_DIR_PROPERTY = "~/.gemfire/logs";
  private static final String LOG_LEVEL_PROPERTY = "mash.log-level";
  private static final String LOG_FILE_SIZE_LIMIT_PROPERTY = "mash.log-file-size-limit";
  private static final String LOG_DISK_SPACE_LIMIT_PROPERTY = "mash.log-disk-space-limit";

  private static final File HISTORY_FILE = new File(getHomeGemFireDirectory(), ".mash.history");

  // History file size
  private static final int MAX_HISTORY_SIZE = 500;

  public static final String DEFAULT_INIT_FILE_NAME = ".mash2rc";
  private static final Level DEFAULT_LOGLEVEL = Level.OFF;
  private static final int DEFAULT_LOGFILE_SIZE_LIMIT = 1024 * 1024 * 10;
  private static final int DEFAULT_LOGFILE_DISK_USAGE = 1024 * 1024 * 10;

  static final String DEFAULT_PROMPT = "{0}mash{1}>";

  private static final String EXECUTE_OPTION = "execute";
  private static final String HELP_OPTION = "help";
  private static final String GLOBAL_VARS = "golbal-vars";
  private static final String OPT_DELIM = ",";
  private static final String KV_DELIM = "=";
  private static final String HELP = CliStrings.HELP;

  private static final String MSG_INVALID_COMMAND_OR_OPTION = "Invalid command or option : {0}."
      + GfshParser.LINE_SEPARATOR + "Use 'mash help' to display additional information.";

  private final Set<String> allowedCommandLineCommands;
  private final OptionParser commandLineParser;
  private StartupTimeLogHelper startupTimeLogHelper;
  private static Map<String, String> globalVars = new HashMap();

  static {
    // See 47325
    System.setProperty(PureJavaMode.PURE_MODE_PROPERTY, "true");
  }



  public static void main(final String[] args) {
    // first check whether required dependencies exist in the classpath
    // should we start without tomcat/servlet jars?
    String nonExistingDependency = CliUtil.cliDependenciesExist(true);
    if (nonExistingDependency != null) {
      System.err.println("Required (" + nonExistingDependency
          + ") libraries not found in the classpath. mash can't start.");
      return;
    }

    MashLauncher launcher = new MashLauncher();
    System.exit(launcher.parseCommandLine(args));
  }

  public MashLauncher() {
    this.startupTimeLogHelper = new StartupTimeLogHelper();
    this.allowedCommandLineCommands = new HashSet<String>();

    this.allowedCommandLineCommands.add(CliStrings.VERSION);
    this.allowedCommandLineCommands.add(CliStrings.DESCRIBE_OFFLINE_DISK_STORE);
    this.allowedCommandLineCommands.add(CliStrings.ENCRYPT);
    this.allowedCommandLineCommands.add(CliStrings.RUN);
    this.allowedCommandLineCommands.add(CliStrings.START_PULSE);
    this.allowedCommandLineCommands.add(CliStrings.START_JCONSOLE);
    this.allowedCommandLineCommands.add(CliStrings.START_JVISUALVM);
    this.allowedCommandLineCommands.add(CliStrings.START_LOCATOR);
    this.allowedCommandLineCommands.add(CliStrings.START_MANAGER);
    this.allowedCommandLineCommands.add(CliStrings.START_SERVER);
    this.allowedCommandLineCommands.add(CliStrings.START_VSD);
    this.allowedCommandLineCommands.add(CliStrings.STATUS_LOCATOR);
    this.allowedCommandLineCommands.add(CliStrings.STATUS_SERVER);
    this.allowedCommandLineCommands.add(CliStrings.STOP_LOCATOR);
    this.allowedCommandLineCommands.add(CliStrings.STOP_SERVER);
    this.allowedCommandLineCommands.add(CliStrings.VERSION);
    this.allowedCommandLineCommands.add(CliStrings.COMPACT_OFFLINE_DISK_STORE);
    this.allowedCommandLineCommands.add(CliStrings.DESCRIBE_OFFLINE_DISK_STORE);
    this.allowedCommandLineCommands.add(CliStrings.EXPORT_OFFLINE_DISK_STORE);
    this.allowedCommandLineCommands.add(CliStrings.VALIDATE_DISK_STORE);
    this.allowedCommandLineCommands.add(CliStrings.PDX_DELETE_FIELD);
    this.allowedCommandLineCommands.add(CliStrings.PDX_RENAME);
    this.allowedCommandLineCommands.add(MashCliStrings.CREATE_ROLE);
    this.allowedCommandLineCommands.add(MashCliStrings.DROP_ROLE);
    this.allowedCommandLineCommands.add(MashCliStrings.GRANT_PRIVILEGE);
    this.allowedCommandLineCommands.add(MashCliStrings.REVOKE_PRIVILEGE);
    this.allowedCommandLineCommands.add(MashCliStrings.GRANT_ROLE);
    this.allowedCommandLineCommands.add(MashCliStrings.REVOKE_ROLE);
    this.commandLineParser = new OptionParser();
    this.commandLineParser.accepts(EXECUTE_OPTION).withOptionalArg().ofType(String.class);
    this.commandLineParser.accepts(HELP_OPTION).withOptionalArg().ofType(Boolean.class);
    this.commandLineParser.accepts(GLOBAL_VARS).withOptionalArg().ofType(String.class);
    this.commandLineParser.posixlyCorrect(false);
  }

  public static String getGlobalOption(String key) {
    return globalVars.get(key);
  }

  private int parseCommandLineCommand(final String... args) {
    Gfsh gfsh = null;
    try {
      // TODO need to change this process only non default shell commands
      gfsh = Gfsh.getInstance(false, true, args, new GfshConfig(HISTORY_FILE.getAbsolutePath(),
          DEFAULT_PROMPT, MAX_HISTORY_SIZE, this.LOG_DIR_PROPERTY, null, null, null, null));

      this.startupTimeLogHelper.logStartupTime();
    } catch (ClassNotFoundException cnfex) {
      log(cnfex, gfsh);
    } catch (IOException ioex) {
      log(ioex, gfsh);
    } catch (IllegalStateException isex) {
      System.err.println("ERROR : " + isex.getMessage());
    }

    ExitShellRequest exitRequest = ExitShellRequest.NORMAL_EXIT;

    if (gfsh != null) {
      final String commandLineCommand = combineStrings(args);

      if (commandLineCommand.startsWith(HELP)) {
        if (commandLineCommand.equals(HELP)) {
          printUsage(gfsh, System.out);
        } else {
          // help is also available for commands which are not available under
          // allowedCommandLineCommands
          gfsh.executeCommand(commandLineCommand);
        }
      } else {
        boolean commandIsAllowed = false;
        for (String allowedCommandLineCommand : this.allowedCommandLineCommands) {
          if (commandLineCommand.startsWith(allowedCommandLineCommand)) {
            commandIsAllowed = true;
            break;
          }
        }

        if (!commandIsAllowed) {
          System.err.println(
              CliStrings.format(MSG_INVALID_COMMAND_OR_OPTION, CliUtil.arrayToString(args)));
          exitRequest = ExitShellRequest.FATAL_EXIT;
        } else {
          if (!gfsh.executeScriptLine(commandLineCommand)) {
            if (gfsh.getLastExecutionStatus() != 0)
              exitRequest = ExitShellRequest.FATAL_EXIT;
          } else if (gfsh.getLastExecutionStatus() != 0) {
            exitRequest = ExitShellRequest.FATAL_EXIT;
          }
        }
      }
    }

    return exitRequest.getExitCode();
  }

  private int parseOptions(final String... args) {
    OptionSet parsedOptions;
    try {
      parsedOptions = parseCommandLineOptions(args);
    } catch (OptionException e) {
      System.err
          .println(CliStrings.format(MSG_INVALID_COMMAND_OR_OPTION, CliUtil.arrayToString(args)));
      return ExitShellRequest.FATAL_EXIT.getExitCode();
    } catch (IllegalArgumentException iae) {
      System.err.println(CliStrings.format(iae.getMessage()));
      return ExitShellRequest.FATAL_EXIT.getExitCode();
    }
    boolean launchShell = true;
    boolean onlyPrintUsage = parsedOptions.has(HELP_OPTION);
    if (parsedOptions.has(EXECUTE_OPTION) || onlyPrintUsage) {
      launchShell = false;
    }

    Gfsh gfsh = null;
    try {
      gfsh =
          Gfsh.getInstance(launchShell, false, args, new GfshConfig(HISTORY_FILE.getAbsolutePath(),
              DEFAULT_PROMPT, MAX_HISTORY_SIZE, null, null, null, null, null));
      this.startupTimeLogHelper.logStartupTime();
    } catch (ClassNotFoundException cnfex) {
      log(cnfex, gfsh);
    } catch (IOException ioex) {
      log(ioex, gfsh);
    } catch (IllegalStateException isex) {
      System.err.println("ERROR : " + isex.getMessage());
    }

    ExitShellRequest exitRequest = ExitShellRequest.NORMAL_EXIT;

    if (gfsh != null) {
      try {
        if (launchShell) {
          gfsh.start();
          gfsh.waitForComplete();
          exitRequest = gfsh.getExitShellRequest();
        } else if (onlyPrintUsage) {
          printUsage(gfsh, System.out);
        } else {
          @SuppressWarnings("unchecked")
          List<String> commandsToExecute = (List<String>) parsedOptions.valuesOf(EXECUTE_OPTION);

          // Execute all of the commands in the list, one at a time.
          for (int i = 0; i < commandsToExecute.size()
              && exitRequest == ExitShellRequest.NORMAL_EXIT; i++) {
            String command = commandsToExecute.get(i);
            System.out.println(GfshParser.LINE_SEPARATOR + "(" + (i + 1) + ") Executing - "
                + command + GfshParser.LINE_SEPARATOR);
            if (!gfsh.executeScriptLine(command) || gfsh.getLastExecutionStatus() != 0) {
              exitRequest = ExitShellRequest.FATAL_EXIT;
            }
          }
        }
      } catch (InterruptedException iex) {
        log(iex, gfsh);
      }
    }

    return exitRequest.getExitCode();
  }

  public OptionSet parseCommandLineOptions(String[] args) {
    OptionSet parsedOptions;
    parsedOptions = this.commandLineParser.parse(args);
    if (parsedOptions.has(GLOBAL_VARS)) {
      String globalVarsArgs = (String) parsedOptions.valueOf(GLOBAL_VARS);
      for (String kv : globalVarsArgs.split(OPT_DELIM)) {
        String[] singleVar = kv.split(KV_DELIM);
        if (singleVar.length < 2 || singleVar[0].trim().length() == 0) {
          throw new IllegalArgumentException("Global variable arg should be of the form key1"
              + KV_DELIM + "value1" + OPT_DELIM + "key2" + KV_DELIM + "value2,...");
        }
        globalVars.put(singleVar[0].trim(), singleVar[1].trim());
      }
    }
    return parsedOptions;
  }

  private int parseCommandLine(final String... args) {
    if (args.length > 0 && !args[0].startsWith(SyntaxConstants.SHORT_OPTION_SPECIFIER)) {
      return parseCommandLineCommand(args);
    }

    return parseOptions(args);
  }

  private void log(Throwable t, Gfsh gfsh) {
    if (!(gfsh != null && gfsh.logToFile(t.getMessage(), t))) {
      t.printStackTrace();
    }
  }

  private String combineStrings(String... strings) {
    StringBuilder stringBuilder = new StringBuilder();
    for (String string : strings) {
      stringBuilder.append(string).append(" ");
    }

    return stringBuilder.toString().trim();
  }

  private void printUsage(final Gfsh gfsh, final PrintStream stream) {
    StringBuilder usageBuilder = new StringBuilder();
    stream.print("Ampool Monarch(R) v");
    stream.print(GemFireVersion.getGemFireVersion());
    stream.println(" Command Line Shell" + GfshParser.LINE_SEPARATOR);
    stream.println("USAGE");
    stream.println(
        "mash [ <command> [option]* | <help> [command] | [--help | -h] | [-e \"<command> [option]*\"]* ]"
            + GfshParser.LINE_SEPARATOR);
    stream.println("OPTIONS");
    stream.println("-e  Execute a command");
    stream.println(Gfsh.wrapText(
        "Commands may be any that are available from the interactive mash prompt.  "
            + "For commands that require a Manager to complete, the first command in the list must be \"connect\".",
        1));
    stream.println(GfshParser.LINE_SEPARATOR + "AVAILABLE COMMANDS");
    stream.print(gfsh.obtainHelp("", this.allowedCommandLineCommands));
    stream.println("EXAMPLES");
    stream.println("mash");
    stream.println(Gfsh.wrapText("Start MASH in interactive mode.", 1));
    stream.println("mash -h");
    stream.println(
        Gfsh.wrapText("Displays 'this' help. ('mash --help' or 'mash help' is equivalent)", 1));
    stream.println("mash help start locator");
    stream.println(Gfsh.wrapText("Display help for the \"start locator\" command.", 1));
    stream.println("mash start locator --name=locator1");
    stream.println(Gfsh.wrapText("Start a Locator with the name \"locator1\".", 1));
    stream.println("mash -e \"connect\" -e \"list members\"");
    stream.println(Gfsh.wrapText(
        "Connect to a running Locator using the default connection information and run the \"list members\" command.",
        1));
    stream.println();

    printExecuteUsage(stream);

    stream.print(usageBuilder);
  }

  private void printExecuteUsage(final PrintStream printStream) {
    StringBuilder usageBuilder = new StringBuilder();

    printStream.print(usageBuilder);
  }

  private static class StartupTimeLogHelper {
    private final long start = System.currentTimeMillis();
    private long done;

    public void logStartupTime() {
      done = System.currentTimeMillis();
      LogWrapper.getInstance().info("Startup done in " + ((done - start) / 1000.0) + " seconds.");
    }

    @Override
    public String toString() {
      return StartupTimeLogHelper.class.getName() + " [start=" + start + ", done=" + done + "]";
    }
  }


  private static String getHomeGemFireDirectory() {
    String userHome = System.getProperty("user.home");
    String homeDirPath = userHome + "/.geode";
    File alternateDir = new File(homeDirPath);
    if (!alternateDir.exists()) {
      if (!alternateDir.mkdirs()) {
        homeDirPath = ".";
      }
    }
    return homeDirPath;
  }
}
