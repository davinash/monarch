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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ServiceLogWriter extends Thread {
  private static final Log LOG = LogFactory.getLog(ServiceLogWriter.class);
  private final String log_dir;
  private final String LOG_DIR_PREFIX = "logs";
  private File log_file = null;
  InputStream is;

  ServiceLogWriter(InputStream is, String log_dir_parent, String serviceName) {
    this.is = is;
    this.log_dir = log_dir_parent;
    File loggin_dir = new File(log_dir_parent, LOG_DIR_PREFIX);
    if (!loggin_dir.exists()) {
      loggin_dir.mkdirs();
    }
    this.log_file = new File(loggin_dir, serviceName + ".log");
  }

  @Override
  public void run() {
    FileWriter logWriter = null;
    try {
      LOG.info("Started log aggregation at " + this.log_file);
      logWriter = new FileWriter(this.log_file);
      InputStreamReader isr = new InputStreamReader(is);
      BufferedReader br = new BufferedReader(isr);
      String line = null;
      while ((line = br.readLine()) != null) {
        System.out.println(">" + line);
        logWriter.write(line);
        logWriter.write("\n");
        logWriter.flush();
      }
    } catch (IOException ioe) {
      // Dont do anything. Let logging fail
      // ioe.printStackTrace();
    } finally {
      if (logWriter != null) {
        try {
          logWriter.close();
        } catch (IOException e) {
          // Dont do anything. Let logging fail
        }
      }
    }
  }
}
