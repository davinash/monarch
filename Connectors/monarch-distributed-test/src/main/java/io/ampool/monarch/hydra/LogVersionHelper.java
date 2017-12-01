/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.ampool.monarch.hydra;

import org.apache.geode.LogWriter;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LogWriterImpl;
import org.apache.geode.internal.logging.LoggingThreadGroup;

public class LogVersionHelper {

  protected static String levelToString(int level) {
    return LogWriterImpl.levelToString(level);
  }

  protected static int levelNameToCode(String level) {
    return LogWriterImpl.levelNameToCode(level);
  }

  protected static ThreadGroup getLoggingThreadGroup(String group, LogWriter logger) {
    return LoggingThreadGroup.createThreadGroup(group, (InternalLogWriter) logger);
  }

  protected static String getMergeLogFilesClassName() {
    return "org.apache.geode.internal.logging.MergeLogFiles";
  }
}
