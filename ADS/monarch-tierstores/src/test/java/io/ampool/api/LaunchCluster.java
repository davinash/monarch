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

package io.ampool.api;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Launches the cluser components .
 */
public class LaunchCluster {
  private static final Log LOG = LogFactory.getLog(LaunchCluster.class);

  private static int retryCount = 3;
  private static int sleepBeforeRetry = 5000; // milliseconds

  /**
   * To launch the cluster component
   *
   * @param type - Type of service to launch
   * @param arguments - Arguments required to launch the cluster
   * @throws ClusterException
   */
  public static void launch(ClusterComponent type, List<String> arguments) throws ClusterException {
    LOG.info("Launching cluster : " + type);
    switch (type) {
      case KDC:
        launchInNewJVM(type.getClassName(), arguments);
        break;
      case HDFS:
        launchInNewJVM(type.getClassName(), arguments);
        break;
      case HBASE:
        launchInNewJVM(type.getClassName(), arguments);
        break;
    }
  }

  /**
   * Destroys the cluster service
   *
   * @param type - Service to be stopped or destoryed
   * @param arguments - Same arguments those are passed while creating it.
   * @throws ClusterException
   */
  public static void destroy(ClusterComponent type, List<String> arguments)
      throws ClusterException {
    LOG.info("Destroying cluster : " + type);
    switch (type) {
      case KDC:
        killPreviouslyLaunchedJVM(type.getClassName(), arguments);
        break;
      case HDFS:
        killPreviouslyLaunchedJVM(type.getClassName(), arguments);
        break;
      case HBASE:
        killPreviouslyLaunchedJVM(type.getClassName(), arguments);
        break;
    }
  }

  /**
   * This clean cluster's parent dir. Important for test cases it will wipe out logs etc
   *
   * @param dir - Cluster dir path
   * @throws ClusterException
   */
  public static void cleanupDir(String dir) throws ClusterException {
    File workDir = new File(dir);
    if (workDir.exists()) {
      try {
        FileUtils.forceDelete(workDir);
      } catch (IOException e) {
        throw new ClusterException(e.getMessage(), e);
      }
    }

  }

  /**
   * Checks if service started
   *
   * @param dir -Cluster dir path
   * @return
   */
  public static boolean isServiceStarted(ClusterComponent type, String dir)
      throws ClusterException {
    String className = type.getClassName();
    try {
      Class<?> serviceClass = Class.forName(className);
      Class[] paramString = new Class[1];
      paramString[0] = String.class;
      Method isStarted = serviceClass.getDeclaredMethod("isStarted", paramString);
      Object status = isStarted.invoke(null, (Object) dir);
      return Boolean.parseBoolean(String.valueOf(status));
    } catch (ClassNotFoundException e) {
      throw new ClusterException(e.getMessage(), e);
    } catch (NoSuchMethodException e) {
      throw new ClusterException(e.getMessage(), e);
    } catch (InvocationTargetException e) {
      throw new ClusterException(e.getMessage(), e);
    } catch (IllegalAccessException e) {
      throw new ClusterException(e.getMessage(), e);
    }
  }

  private static void killPreviouslyLaunchedJVM(String className, List<String> arguments)
      throws ClusterException {
    // assuming first argument is always working dir
    // find service.pid file and read it
    // getPID
    try {
      Class<?> serviceClass = Class.forName(className);
      Class[] paramString = new Class[1];
      paramString[0] = String.class;
      Method getPID = serviceClass.getDeclaredMethod("getPID", paramString);
      Object pid = null;
      for (int i = 0; i < LaunchCluster.retryCount; i++) {
        if (i > 0)
          LOG.info("Retrying to get PID");
        pid = getPID.invoke(null, (Object) arguments.get(0));
        if (pid != null)
          break;
        Thread.sleep(LaunchCluster.sleepBeforeRetry);
      }
      if (pid == null) {
        LOG.error("Couldn't find PID. Could not destroy process cleanly.");
      } else {
        killjavaProcess(Integer.parseInt(String.valueOf(pid)));
      }
      Method destroy = serviceClass.getDeclaredMethod("destroy", paramString);
      destroy.invoke(null, (Object) arguments.get(0));

    } catch (ClassNotFoundException e) {
      throw new ClusterException(e.getMessage(), e);
    } catch (NoSuchMethodException e) {
      throw new ClusterException(e.getMessage(), e);
    } catch (InvocationTargetException e) {
      throw new ClusterException(e.getMessage(), e);
    } catch (IllegalAccessException e) {
      throw new ClusterException(e.getMessage(), e);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private static void killjavaProcess(int pid) throws ClusterException {
    try {
      LOG.info("Stopping service process with PID " + pid);
      Runtime.getRuntime().exec("kill -9 " + pid);
    } catch (IOException e) {
      throw new ClusterException(e.getMessage(), e);
    }
  }

  private static void launchInNewJVM(String className, List<String> arguments)
      throws ClusterException {
    ProcessBuilder pb = new ProcessBuilder();
    String javaHome = System.getProperty("java.home");
    LOG.info("JAVA HOME : " + javaHome);
    if (javaHome == null) {
      LOG.error("JAVA_HOME not set. Exiting...");
      throw new ClusterException("JAVA_HOME not set. Exiting...");
    }
    String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
    String classpath = System.getProperty("java.class.path");
    List<String> commands = new ArrayList<>();
    // java executor java
    commands.add(javaBin);
    // add -cp option.
    commands.add("-cp");
    // actual classpath variable
    commands.add(classpath);
    // class to execute
    commands.add(className);
    // pass arguments to service class
    Iterator<String> argumentsItr = arguments.iterator();
    while (argumentsItr.hasNext()) {
      commands.add(argumentsItr.next());
    }
    // add command to execute
    pb.command(commands);
    LOG.debug("Launching with command " + pb.command());
    try {
      Process process = pb.start();
      String[] classNameParts = className.split("\\.");
      String serviceName = classNameParts[classNameParts.length - 1];
      ServiceLogWriter errorGobbler =
          new ServiceLogWriter(process.getErrorStream(), arguments.get(0), serviceName);
      ServiceLogWriter outputGobbler =
          new ServiceLogWriter(process.getInputStream(), arguments.get(0), serviceName);
      // start gobblers
      outputGobbler.start();
      errorGobbler.start();
      LOG.info("Process launched : " + process.isAlive());
    } catch (IOException ex) {
      throw new ClusterException(ex.getMessage(), ex);
    }
  }

  /**
   * For test
   *
   * @param args
   * @throws ClusterException
   */
  public static void main(String[] args) throws ClusterException {
    Thread.currentThread().setName("Launcher Thread->" + System.currentTimeMillis());
    LaunchCluster.launch(ClusterComponent.KDC, new ArrayList<String>());

  }
}
