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
import org.apache.hadoop.minikdc.MiniKdc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KDCQuasiService {
  public String KDC_DIR = null;

  private static final Log LOG = LogFactory.getLog(KDCQuasiService.class);
  public static final String KDC_DIR_PREFIX = "kdc";
  public static final String KEYTAB_DIR_PREFIX = "keytabs";
  private MiniKdc kdc;
  private String kdc_work_dir;
  private String krb5_conf_path;
  private String keytabs_dir;

  private void create(String kdc_work_dir, String krb5_conf_path, String keytabs_dir)
      throws KDCQuasiServiceException {
    this.kdc_work_dir = kdc_work_dir + File.separator + KDC_DIR_PREFIX;
    this.krb5_conf_path = krb5_conf_path;
    if (keytabs_dir == null) {
      this.keytabs_dir = this.kdc_work_dir + File.separator + KEYTAB_DIR_PREFIX;
    } else
      this.keytabs_dir = keytabs_dir;
    create(this.kdc_work_dir);
  }

  /**
   * Creates MiniKDC instance with default configuration.
   */
  public void create(String workDir) throws KDCQuasiServiceException {
    Properties kdcConf = MiniKdc.createConf();
    if (this.kdc_work_dir == null /* && !workDir.endsWith(KDCQuasiService.KDC_DIR_PREFIX) */)
      this.kdc_work_dir = workDir + File.separator + KDC_DIR_PREFIX;
    File kdcWorkDir = new File(this.kdc_work_dir);
    try {
      this.kdc = new MiniKdc(kdcConf, kdcWorkDir);
      start();
      File keytabDir = new File(this.getKeyTabDir());
      if (null != keytabDir && !keytabDir.exists())
        keytabDir.mkdirs();
    } catch (KDCQuasiServiceException e) {
      throw e;
    } catch (Exception e) {
      throw new KDCQuasiServiceException("Cannot spawn KDC service", e);
    }
  }

  public String getKrb5ConfPath() {
    return this.kdc.getKrb5conf().getPath();
  }

  /**
   * Starts KDC service
   */
  public void start() throws KDCQuasiServiceException {
    try {
      this.kdc.start();
    } catch (Exception e) {
      throw new KDCQuasiServiceException("Unable to start the KDC service", e);
    }
  }

  /**
   * Stops running KDC service instance. This doesn't delete working directories
   */
  public void stop() {
    this.kdc.stop();
  }

  /**
   * Destroys KDC Service. Deletes working dir and keytabs generated
   */
  public void destroy() {
    stop();
    cleanUpDir();
  }

  /**
   * Service version
   *
   * @param work_Dir
   */
  public static void destroy(String work_Dir) {
    cleanUpDir(work_Dir);
  }

  /**
   * Returns instance of KDC to operate
   */
  public MiniKdc getInstance() {
    return this.kdc;
  }

  /**
   * Returns dir where keytabs are stored
   *
   * @return
   */
  public String getKeyTabDir() {
    if (this.keytabs_dir != null)
      return keytabs_dir;
    else
      return this.kdc_work_dir + File.separator + "/keytabs";
  }

  /**
   * Return KDC realm
   */
  public String getRealm() {
    return this.kdc.getRealm();
  }

  public PrincipalKeytab addPrincipal(String keytabFile, String principal)
      throws KDCQuasiServiceException {
    return addPrincipal(new File(getKeyTabDir(), keytabFile), principal);
  }

  public PrincipalKeytab addPrincipal(File keytabFile, String principal)
      throws KDCQuasiServiceException {
    try {
      LOG.info(
          "Adding principal " + principal + " Keytab filepath " + keytabFile.getAbsolutePath());
      this.kdc.createPrincipal(keytabFile, principal);
      if (principal.contains("@")) {
        return new PrincipalKeytab(principal, keytabFile.getAbsolutePath());
      } else {
        return new PrincipalKeytab(principal + "@" + getRealm(), keytabFile.getAbsolutePath());
      }

    } catch (Exception e) {
      throw new KDCQuasiServiceException("Unable to add kerberos principal ", e);
    }
  }

  public List<PrincipalKeytab> addPrincipal(String keytabFile, String... principals)
      throws KDCQuasiServiceException {
    return addPrincipal(new File(getKeyTabDir(), keytabFile), principals);
  }

  public List<PrincipalKeytab> addPrincipal(File keytabFile, String... principals)
      throws KDCQuasiServiceException {
    ArrayList<PrincipalKeytab> pks = new ArrayList();
    try {
      this.kdc.createPrincipal(keytabFile, principals);
      String[] principalArr = principals.clone();
      for (String principal : principalArr) {
        pks.add(new PrincipalKeytab(principal, keytabFile.getAbsolutePath()));
      }
      return pks;
    } catch (Exception e) {
      throw new KDCQuasiServiceException("Unable to kerberos principal ", e);
    }
  }

  public void createPrincipals(String principals_list)
      throws KDCQuasiServiceException, IOException {
    if (principals_list == null)
      return;
    String[] principals = principals_list.split(",");
    for (String principal : principals) {
      LOG.info("Adding principal : " + principal);
      String userName = "";
      if (principal.contains("/")) {
        userName = principal.split("/")[0];
      } else {
        userName = principal;
      }
      String keytabName = userName + ".keytab";
      File keytabFileName = new File(this.getKeyTabDir(), keytabName);
      this.addPrincipal(keytabFileName, principal);
    }
  }

  // Private methods
  private void cleanUpDir() {
    try {
      LOG.info("Deleting Keytab dir. " + this.getKeyTabDir());
      File keytabDir = new File(this.getKeyTabDir());
      if (keytabDir.exists()) {
        FileUtils.forceDelete(keytabDir);
      }
      File kdcWorkDir = new File(this.kdc_work_dir);
      LOG.info("Deleting KDC temporary dir. " + kdcWorkDir);
      if (kdcWorkDir.exists()) {
        FileUtils.forceDelete(kdcWorkDir);
      }
    } catch (IOException ex) {
      LOG.error("Couldn't delete work dir");
    }
  }

  private static void cleanUpDir(String workDir) {
    try {
      File kdcWorkDir = new File(workDir, KDC_DIR_PREFIX);
      LOG.info("Deleting KDC temporary dir. " + kdcWorkDir);
      if (kdcWorkDir.exists()) {
        FileUtils.forceDelete(kdcWorkDir);
      }
    } catch (IOException ex) {
      LOG.error("Couldn't delete work dir");
    }
  }

  /**
   * Entry point
   *
   * @param args Args are 1. KDC launch Dir - All kdc related files will be present here. 2.
   *        Principals list - Comma separated list of Principal to be added in KDC. [Optional] 3.
   *        Keytabs dir - Where keytabs should be passed. [Optional] 4. KRB5 conf file path - Where
   *        to write KRB5.conf file. [Optional]
   * @throws KDCQuasiServiceException
   */
  public static void main(String[] args) throws KDCQuasiServiceException, IOException {
    // parse arguments
    if (args.length < 2) {
      KDCQuasiService.help();
      System.exit(1);
    }
    // if (args.length >1 && args.length < 4) {
    // KDCQuasiService.help();
    // System.exit(1);
    // }
    String kdc_work_dir = args[0];
    String krb5_conf_path = null;
    String keytabs_dir = null;
    String principals_list = null;
    if (args.length >= 2)
      principals_list = args[1];
    if (args.length >= 3)
      keytabs_dir = args[2];
    if (args.length == 4)
      krb5_conf_path = args[3];

    final KDCQuasiService kdcQS = new KDCQuasiService();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        kdcQS.destroy();
      }
    });

    kdcQS.create(kdc_work_dir, krb5_conf_path, keytabs_dir);
    kdcQS.createPrincipals(principals_list);
    kdcQS.writeKRB5();
    kdcQS.writePID();

    // String superUserName = "hdfs";
    // File hdfsKeytabFile = new File(kdcQS.getKeyTabDir(), superUserName + ".keytab");
    // kdcQS.addPrincipal(hdfsKeytabFile, superUserName + "/localhost", "HTTP/localhost");
    // kdcQS.addPrincipal("hbase.keytab", "hbase/localhost");
    // kdcQS.addPrincipal("zookeeper.keytab", "zookeeper/localhost");
    // System.out.println(kdcQS.getInstance().getRealm());
    // System.out.println(kdcQS.getInstance().getHost());
    // kdcQS.destroy();
  }

  private void writeKRB5() throws KDCQuasiServiceException {
    if (this.krb5_conf_path != null) {
      try {
        LOG.info("Writing KRB5 conf at " + this.krb5_conf_path);
        FileUtils.copyFile(this.kdc.getKrb5conf(), new File(this.krb5_conf_path));
      } catch (IOException e) {
        throw new KDCQuasiServiceException("Can not write KRB5 config", e);
      }
    }
  }

  /**
   * Compulsory for every service to override and call
   *
   * @throws IOException
   */
  private void writePID() throws KDCQuasiServiceException {
    // Note: may fail in some JVM implementations
    // therefore fallback has to be provided
    try {
      FileWriter pidWriter = new FileWriter(this.kdc_work_dir + File.separator + "service.pid");
      String jvmName = ManagementFactory.getRuntimeMXBean().getName();
      if (jvmName.contains("@")) {
        jvmName = jvmName.split("@")[0];
      }
      pidWriter.write(jvmName);
      pidWriter.close();
      LOG.info("KDC Pid : " + jvmName);
    } catch (IOException e) {
      throw new KDCQuasiServiceException(e.getMessage(), e);
    }
  }

  public static String getPID(String workDir) throws KDCQuasiServiceException {
    String kdc_work_dir = workDir + File.separator + KDC_DIR_PREFIX;
    String pidFile = kdc_work_dir + File.separator + "service.pid";
    String pid = null;
    try {
      FileReader pr = new FileReader(pidFile);
      BufferedReader pidReader = new BufferedReader(pr);
      String pidLine = pidReader.readLine();
      pid = pidLine;
    } catch (FileNotFoundException e) {
      throw new KDCQuasiServiceException(e.getMessage(), e);
    } catch (IOException e) {
      throw new KDCQuasiServiceException(e.getMessage(), e);
    }
    return pid;
  }

  public static boolean isStarted(String workDir) throws KDCQuasiServiceException {
    String kdc_work_dir = workDir + File.separator + KDC_DIR_PREFIX;
    String pidFilePath = kdc_work_dir + File.separator + "service.pid";
    File pidFile = new File(pidFilePath);
    String pid = null;
    if (pidFile.exists())
      return true;
    return false;
  }

  public static void help() {
    StringBuilder helpMsgBuilder = new StringBuilder();
    helpMsgBuilder.append("Missing arguments.\n");
    helpMsgBuilder.append("1. KDC launch Dir - All kdc related files will be present here.\n");
    helpMsgBuilder
        .append("2. Principals list -Comma separated list of Principal to be added in KDC.\n");
    helpMsgBuilder.append("3. Keytabs dir -Where keytabs should be passed.[Optional]\n");
    helpMsgBuilder.append("4. KRB5 conf file path -Where to write KRB5.conf file.[Optional]\n");
    System.out.println(helpMsgBuilder.toString());
  }
}
