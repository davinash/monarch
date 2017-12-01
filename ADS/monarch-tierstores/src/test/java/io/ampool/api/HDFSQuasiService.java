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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HTTP_POLICY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY;

public class HDFSQuasiService implements Serializable {

  public static final String KERBEROS_AUTH = "kerberos";
  private static final String DFS_DATA_TRANSFER_PROTECTION_KEY = "dfs.data.transfer.protection";
  public static final String AUTHENTICATION_PRIVACY = "authentication,privacy";
  private static final int DATANODES = 3;
  private static final String HDFS_DIR_PREFIX = "dfs";
  private static final String KDC_DIR_PREFIX = "embded_kdc";
  private static String SUPER_USER = "hdfs";
  private Configuration conf = null;
  private int numberOfDataNodes = -1;
  private String workingDir;
  public static final String SUPER_USER_NAME = "hdfs";

  private static final Log LOG = LogFactory.getLog(HDFSQuasiService.class);
  private boolean secureCluster;
  private String kdcWorkDir = null;

  public Configuration getConf() {
    return conf;
  }

  public void createSecuredUserDir(String userName, String keytabdir) {
    try {
      UserGroupInformation.setConfiguration(conf);
      UserGroupInformation.loginUserFromKeytab(getHDFSPrincipal(""),
          keytabdir + File.separator + "hdfs.keytab");
      FileSystem fs = FileSystem.get(conf);
      Path userDir = new Path("/user" + File.separator + userName);
      fs.mkdirs(userDir, new FsPermission(FsAction.ALL, FsPermission.getDefault().getGroupAction(),
          FsPermission.getDefault().getOtherAction()));
      fs.setOwner(userDir, userName, "hadoop");
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public boolean checkFileExistsSecured(final String user, final String keytab, String storeBaseDir,
      String tableName) {
    try {
      UserGroupInformation.setConfiguration(conf);
      UserGroupInformation.loginUserFromKeytab(user, keytab);
      FileSystem fs = FileSystem.get(conf);
      Path storeBasePath = new Path(fs.getHomeDirectory(), storeBaseDir);
      Path tablePath = new Path(storeBasePath, tableName);
      return fs.exists(tablePath);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  public boolean checkFileExists(String storeBaseDir, String tableName) {
    try {
      FileSystem fs = FileSystem.get(conf);
      Path storeBasePath = new Path(fs.getHomeDirectory(), storeBaseDir);
      Path tablePath = new Path(storeBasePath, tableName);
      return fs.exists(tablePath);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  public int getFilesCount(String storeBaseDir, String tableName) {
    int filesCount = 0;
    try {
      FileSystem fs = FileSystem.get(conf);
      Path storeBasePath = new Path(fs.getHomeDirectory(), storeBaseDir);
      Path tablePath = new Path(storeBasePath, tableName);
      if (fs.exists(tablePath)) {
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator =
            fs.listFiles(tablePath, false);
        while (locatedFileStatusRemoteIterator.hasNext()) {
          filesCount++;
          LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
          System.out.println("File name is " + next.getPath());
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return filesCount;
  }


  public List<OrcStruct> getORCRecords(String storeBaseDir, String tableName) throws IOException {
    List<OrcStruct> orcrecords = new ArrayList<>();
    try {
      FileSystem fs = FileSystem.get(conf);
      Path storeBasePath = new Path(fs.getHomeDirectory(), storeBaseDir);
      Path tablePath = new Path(storeBasePath, tableName);
      if (fs.exists(tablePath)) {
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator =
            fs.listFiles(tablePath, false);
        while (locatedFileStatusRemoteIterator.hasNext()) {
          LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
          final org.apache.hadoop.hive.ql.io.orc.Reader fis =
              OrcFile.createReader(next.getPath(), OrcFile.readerOptions(conf));
          RecordReader rows = fis.rows();
          while (rows.hasNext()) {
            orcrecords.add((OrcStruct) rows.next(null));
          }
          System.out.println("File name is " + next.getPath());
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return orcrecords;
  }

  public int getNumberOfDataNodes() {
    return numberOfDataNodes;
  }

  /*
   * private MiniDFSCluster createHDFSCluster(int numberOfDataNodes, String dfsDirPath,
   * Configuration conf) throws IOException { this.conf = conf; this.numberOfDataNodes =
   * numberOfDataNodes; this.DFS_CLUTSER_DIR = dfsDirPath;
   * 
   * MiniDFSCluster hdfsCluster = null;
   * 
   * File baseDir = new File(DFS_CLUTSER_DIR).getAbsoluteFile(); FileUtil.fullyDelete(baseDir);
   * conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
   * 
   * LOG.info("Auth type in config is: " +
   * conf.get(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION));
   * UserGroupInformation.setLoginUser(null); UserGroupInformation.setConfiguration(conf);
   * LOG.info("Security status in UGI:" + UserGroupInformation.isSecurityEnabled());
   * 
   * MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
   * builder.numDataNodes(numberOfDataNodes); try { hdfsCluster = builder.build(); } catch
   * (IOException e) { LOG.error("Error in creating mini DFS cluster ", e); throw e; }
   * ListIterator<DataNode> itr = hdfsCluster.getDataNodes().listIterator();
   * System.out.println("NameNode: " +
   * hdfsCluster.getNameNode().getNameNodeAddressHostPortString()); while (itr.hasNext()) { DataNode
   * dn = itr.next(); System.out.println("DataNode: " + dn.getDisplayName()); } return hdfsCluster;
   * }
   */

  public void setWorkingDir(String workingDir) {
    this.workingDir = workingDir;
  }

  public String getWorkingDir() {
    return workingDir;
  }

  public void setNumberOfDataNodes(int numberOfDataNodes) {
    this.numberOfDataNodes = numberOfDataNodes;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  // public void createHDFSDir(Path path) throws IOException {
  // this.createUserDir(path, "hdfs");
  // }
  //
  // public void createUserHomeDir(String userName) throws IOException {
  // Path path = new Path("/user/" + userName);
  // createUserDir(path, userName);
  // }

  // public void createUserDir(Path path, String userName) throws IOException {
  // FileSystem fs = hdfsCluster.getFileSystem();
  // fs.mkdirs(path, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
  // fs.setOwner(path, userName, userName);
  // }

  /**
   * Start HDFS service
   */
  public static void main(String[] args) throws HDFSQuasiServiceException {
    if (args.length < 1) {
      HDFSQuasiService.help();
      System.exit(1);
    }
    final String hdfs_work_dir = args[0];
    int numberOfDatanodes = HDFSQuasiService.DATANODES;
    if (args.length >= 2)
      numberOfDatanodes = Integer.parseInt(args[1]);

    String hdfs_conf_path = HDFSQuasiService.getDeafultHDFSConfPath(hdfs_work_dir);
    if (args.length >= 3)
      hdfs_conf_path = args[2];

    boolean isSecured = false;
    if (args.length >= 4)
      isSecured = Boolean.parseBoolean(args[3]);

    boolean embededKDC = true;
    String krb5ConfPath = null;
    if (args.length >= 5) {
      krb5ConfPath = args[4];
    }
    String hdfsKeyTab = null;
    if (args.length >= 6) {
      hdfsKeyTab = args[5];
      embededKDC = false;
    }

    String spengoKeyTab = null;
    if (args.length >= 7) {
      spengoKeyTab = args[6];
    }

    final HDFSQuasiService hdfsQuasiService = new HDFSQuasiService();
    hdfsQuasiService.create_new(hdfs_work_dir, numberOfDatanodes, null, hdfs_conf_path, isSecured,
        embededKDC, krb5ConfPath, hdfsKeyTab, spengoKeyTab);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          hdfsQuasiService.destroy(hdfs_work_dir);
        } catch (ClusterException e) {
          LOG.error("Error in destroying embedded KDC");
        }
      }
    });
  }

  public static String getEmbeddedKRB5ConfPath(String hdfs_work_dir) {
    return HDFSQuasiService.getEmbeddedKDCPath(hdfs_work_dir) + File.separator + "krb5.conf";
  }

  public static String getEmbeddedKDCPath(String hdfs_work_dir) {
    return hdfs_work_dir + File.separator + HDFSQuasiService.KDC_DIR_PREFIX;
  }

  public static String getDeafultHDFSConfPath(String hdfs_work_dir) {
    return hdfs_work_dir + File.separator + HDFSQuasiService.HDFS_DIR_PREFIX + File.separator
        + "hadoop.xml";
  }

  public void create_new(String hdfs_work_dir, int numberOfDatanodes, Configuration conf,
      String hdfsConfOutPath, boolean isSecured, boolean embededKDC, String krb5ConfPath,
      String hdfsKeyTab, String spengoKeyTab, String... userPrincipals)
      throws HDFSQuasiServiceException {
    String realm = "EXAMPLE.COM";
    this.setWorkingDir(hdfs_work_dir + File.separator + HDFSQuasiService.HDFS_DIR_PREFIX);
    this.setNumberOfDataNodes(numberOfDatanodes);
    if (conf == null) {
      // create one and set
      this.setConf(new Configuration());
    } else {
      this.setConf(conf);
    }
    this.setSecureCluster(isSecured);
    if (this.isSecureCluster()) {
      // set secured cluster related stuff
      LOG.info("Setting up secured HDFS");
      if (embededKDC) {
        // Launch KDC from here
        LOG.info("Using embedded KDC");
        this.kdcWorkDir = HDFSQuasiService.getEmbeddedKDCPath(hdfs_work_dir);
        // to be used to construct keytab path
        String keyTabDir = HDFSQuasiService.getEmbeddedKeyTabDir(kdcWorkDir);
        String krb5ConfOutPath = null;
        if (krb5ConfPath == null) {
          // in this case write KRB5 conf to this path
          krb5ConfPath = this.kdcWorkDir + File.separator + "krb5.conf";
        }
        List<String> usersList =
            Arrays.stream(userPrincipals).map(U -> U.toString()).collect(Collectors.toList());

        usersList.add(HDFSQuasiService.getHDFSPrincipal(realm));
        usersList.add(HDFSQuasiService.getHTTPPrincipal(realm));
        String[] users = usersList.stream().toArray(String[]::new);

        try {
          launchKDCService(kdcWorkDir, keyTabDir, krb5ConfPath, users);
        } catch (ClusterException e) {
          throw new HDFSQuasiServiceException("Cannot start embedded KDC ", e);
        } catch (InterruptedException e) {
          throw new HDFSQuasiServiceException("Cannot start embedded KDC ", e);
        }
        hdfsKeyTab = new File(keyTabDir, "hdfs.keytab").getAbsolutePath();
        spengoKeyTab = new File(keyTabDir, "HTTP.keytab").getAbsolutePath();
        //
        // KDCQuasiService kdcQuasiService = new KDCQuasiService();
        // try {
        // LOG.info("KDC path " + HDFSQuasiService.getEmbeddedKDCPath(hdfs_work_dir));
        // kdcQuasiService.create(HDFSQuasiService.getEmbeddedKDCPath(hdfs_work_dir));
        // this.setKRB5Conf(kdcQuasiService.getInstance().getKrb5conf().getAbsolutePath());
        // String keyTabDir = kdcQuasiService.getKeyTabDir();
        // List<PrincipalKeytab> hdfsPK = kdcQuasiService
        // .addPrincipal("hdfs.keytab", HDFSQuasiService.getHDFSPrincipal(realm),
        // HDFSQuasiService.getHTTPPrincipal(realm));
        // //PrincipalKeytab httpPK = kdcQuasiService.addPrincipal("http.keytab",
        // HDFSQuasiService.getHTTPPrincipal(realm));
        // setSecuredConfigurations(this.getConf(), realm,
        // hdfsPK.get(0).keytabFile,hdfsPK.get(0).keytabFile);
        // } catch (KDCQuasiServiceException e) {
        // throw new HDFSQuasiServiceException("Cannot start embedded KDC ", e);
        // } catch (Exception e) {
        // throw new HDFSQuasiServiceException("Failure in setting secured configuation for HDFS",
        // e);
        // }
      }

      // read keytab path and set KRB5 conf
      this.setKRB5Conf(krb5ConfPath);
      try {
        setSecuredConfigurations(this.getConf(), realm, hdfsKeyTab, spengoKeyTab);
      } catch (Exception e) {
        throw new HDFSQuasiServiceException("Failure in setting secured configuation for HDFS", e);
      }
    }
    MiniDFSCluster dfsCluster = createCluster();
    writePID();
    writeConf(dfsCluster.getConfiguration(0), hdfsConfOutPath);
  }

  public static String getEmbeddedKeyTabDir(String kdcWorkDir) {
    return kdcWorkDir + File.separator + KDCQuasiService.KDC_DIR_PREFIX + File.separator
        + KDCQuasiService.KEYTAB_DIR_PREFIX;
  }

  public void setKRB5Conf(String krb5ConfPath) {
    System.setProperty("java.security.krb5.conf", krb5ConfPath);
  }

  public static String getHDFSPrincipal(String realm) {
    return HDFSQuasiService.SUPER_USER + "/localhost";// + realm;
  }

  public static String getHTTPPrincipal(String realm) {
    return "HTTP" + "/localhost";
    // + realm;
  }

  private void launchKDCService(String workDir, String keytabDir, String krb5ConfOutPath,
      String... principals) throws ClusterException, InterruptedException {
    List<String> arguments = new ArrayList<String>();
    arguments.add(workDir);
    String principalList = "";
    for (String principal : principals) {
      principalList = principalList + "," + principal;
    }
    // to remove first comma
    principalList = principalList.substring(1);
    arguments.add(principalList);
    arguments.add(keytabDir);
    arguments.add(krb5ConfOutPath);
    LaunchCluster.launch(ClusterComponent.KDC, arguments);
    for (int check = 0; check < 3; check++) {
      if (LaunchCluster.isServiceStarted(ClusterComponent.KDC, workDir))
        break;
      Thread.sleep(10000L);
    }
  }

  private Configuration setSecuredConfigurations(Configuration conf, String realm,
      String hdfsKeyTabFilePath, String spengoKeytabPath) throws Exception {
    // Set HDFS configuration to Kerberos
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, KERBEROS_AUTH);

    String hdfsPrincipal = SUPER_USER_NAME + "/localhost@" + realm;
    // this.hdfsKeytabFile = new File(kdcQuasiService.getKeyTabDir(), SUPER_USER_NAME + ".keytab");
    //
    // kdcQuasiService.addPrincipal(hdfsKeytabFile, SUPER_USER_NAME + "/localhost",
    // "HTTP/localhost");

    String spnegoPrincipal = "HTTP/localhost@" + realm;

    conf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    conf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, hdfsKeyTabFilePath);

    conf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    conf.set(DFS_DATANODE_KEYTAB_FILE_KEY, hdfsKeyTabFilePath);
    conf.set(DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, spnegoPrincipal);
    conf.set(DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY, spengoKeytabPath);

    conf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication,privacy");
    conf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    conf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY, 10);

    conf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, AUTHENTICATION_PRIVACY);

    File parentPath = new File(hdfsKeyTabFilePath).getParentFile();
    String keystoresDir = parentPath.getAbsolutePath();
    String sslConfDir = ".";
    try {
      sslConfDir = KeyStoreTestUtil.getClasspathDir(this.getClass());
    } catch (NullPointerException ex) {
      LOG.error("Unable to get classpath dir");
    }
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);
    // conf.set(DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY,
    // KeyStoreTestUtil.getClientSSLConfigFileName());
    // conf.set(DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
    // KeyStoreTestUtil.getServerSSLConfigFileName());
    return conf;
  }

  private void writeConf(Configuration configuration, String hdfsConfOutPath) {
    try {
      configuration.writeXml(new FileWriter(hdfsConfOutPath));
    } catch (IOException e) {
      LOG.error("Error in writing configuration at " + hdfsConfOutPath);
    }
  }

  /**
   * Compulsory for every service to override and call
   *
   * @throws IOException
   */
  private void writePID() throws HDFSQuasiServiceException {
    // Note: may fail in some JVM implementations
    // therefore fallback has to be provided
    try {
      FileWriter pidWriter = new FileWriter(this.getWorkingDir() + File.separator + "service.pid");
      String jvmName = ManagementFactory.getRuntimeMXBean().getName();
      if (jvmName.contains("@")) {
        jvmName = jvmName.split("@")[0];
      }
      pidWriter.write(jvmName);
      pidWriter.close();
      LOG.info("HDFS service Pid : " + jvmName);
    } catch (IOException e) {
      throw new HDFSQuasiServiceException(e.getMessage(), e);
    }
  }

  public static String getPID(String workDir) throws KDCQuasiServiceException {
    String kdc_work_dir = workDir + File.separator + HDFS_DIR_PREFIX;
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

  /**
   * Service version
   *
   * @param work_Dir
   */
  public static void destroy(String work_Dir) throws ClusterException {
    cleanUpDir(work_Dir);
  }

  private static void cleanUpDir(String work_dir) throws ClusterException {
    String hdfs_working_dir = work_dir + File.separator + HDFS_DIR_PREFIX;
    File hdfs_dir = new File(hdfs_working_dir);
    LOG.info("Deleting HDFS dir. " + hdfs_dir.getAbsolutePath());
    if (hdfs_dir.exists()) {
      try {
        FileUtils.forceDelete(hdfs_dir);
      } catch (IOException e) {
        LOG.error("Error in cleaning up HDFS dir " + e.getMessage());
      }
    }
    String kdc_working_dir = work_dir + File.separator + KDC_DIR_PREFIX;
    File kdc_dir = new File(kdc_working_dir);
    LOG.info("Deleting Embedded KDC dir. " + kdc_dir.getAbsolutePath());
    if (kdc_dir.exists()) {
      // stop the service
      ArrayList arguments = new ArrayList();
      arguments.add(kdc_dir.getAbsolutePath());
      LOG.error("Trying to destroy KDC " + arguments.get(0));
      LaunchCluster.destroy(ClusterComponent.KDC, arguments);
      try {
        FileUtils.forceDelete(kdc_dir);
      } catch (IOException e) {
        LOG.error("Error in cleaning up Embedded KDC dir " + e.getMessage());
      }
    }
  }

  public static boolean isStarted(String workDir) throws KDCQuasiServiceException {
    String kdc_work_dir = workDir + File.separator + HDFS_DIR_PREFIX;
    String pidFilePath = kdc_work_dir + File.separator + "service.pid";
    File pidFile = new File(pidFilePath);
    String pid = null;
    if (pidFile.exists())
      return true;
    return false;
  }

  private MiniDFSCluster createCluster() throws HDFSQuasiServiceException {
    MiniDFSCluster hdfsCluster = null;

    File baseDir = new File(getWorkingDir()).getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);
    this.conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());

    LOG.info("Using base dir " + baseDir.getAbsolutePath());

    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(this.conf);
    builder.numDataNodes(getNumberOfDataNodes());
    try {
      hdfsCluster = builder.build();
    } catch (IOException e) {
      LOG.error("Error in creating mini DFS cluster ", e);
      throw new HDFSQuasiServiceException("Error in creating mini DFS cluster ", e);
    }
    ListIterator<DataNode> itr = hdfsCluster.getDataNodes().listIterator();
    LOG.info("NameNode: " + hdfsCluster.getNameNode().getNameNodeAddressHostPortString());
    while (itr.hasNext()) {
      DataNode dn = itr.next();
      LOG.info("DataNode: " + dn.getDisplayName());
    }
    return hdfsCluster;
  }

  private static void help() {
    StringBuilder helpMsgBuilder = new StringBuilder();
    helpMsgBuilder.append("Missing arguments.\n");
    helpMsgBuilder.append("1. HDFS launch Dir - All HDFS cluster dir.\n");
    helpMsgBuilder.append("2. Number of Datanodes.[Optional]\n");
    helpMsgBuilder.append(
        "3. HDFS Conf file output path - Where config of launched cluster should be written.[Optional]\n");
    helpMsgBuilder.append("4. Secure cluster - Secure the cluster with kerberos[Optional]\n");
    helpMsgBuilder.append(
        "5. KRB5 conf file path - Path KRB5.conf file. In case of internal KDC, here KRB5 conf will be returned\n");
    helpMsgBuilder.append(
        "6. HDFS keytab file path - Path HDFS keytab file (Principal hdfs/_HOST). This will make cluster secured with kerberos[Optional]\n");
    helpMsgBuilder.append(
        "7. Hadoop SPENGO keytab file path - Path SPENGO keytab file (Principal HTTP/_HOST). This will make cluster secured with kerberos[Optional]\n");
    System.out.println(helpMsgBuilder.toString());
  }

  public boolean isSecureCluster() {
    return secureCluster;
  }

  public void setSecureCluster(boolean secureCluster) {
    this.secureCluster = secureCluster;
  }
}
