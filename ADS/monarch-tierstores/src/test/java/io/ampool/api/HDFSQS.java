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


public class HDFSQS {

  /*
   * //public static final HDFSQuasiService hdfsQuasiService = new HDFSQuasiService(); public static
   * final String PARENT_DIR = System.getProperty("java.io.tmpdir") + File.separator +
   * "securedhdfs"; public static final String dfsPath = PARENT_DIR + File.separator + "dfsDir";
   * public static final String kdcWorkDir = PARENT_DIR + File.separator + "kdc"; public static
   * final String krb5ConfPath = kdcWorkDir + File.separator + "krb5.conf"; public static final
   * String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";
   * 
   * public static final String securityConfFilePath = PARENT_DIR + File.separator +
   * "client-security.xml"; public static final String hdfsStoreHomeDir = "/my-securedhdfsStore";
   * 
   * public final int numberOfDatanodes = 3; public static final String SEPARATOR = "#"; public
   * static final String HOSTNAME = "localhost"; public static final String USER_PRINCIPAL =
   * "ampool" + "/" + HOSTNAME; public static final String USER_KEY_TAB_PATH = PARENT_DIR +
   * File.separator + HDFSQS.SECURED_USER + ".keytab";
   * 
   * public static final String KERBEROS_AUTH = "kerberos"; private static final String
   * DFS_DATA_TRANSFER_PROTECTION_KEY = "dfs.data.transfer.protection"; public static final String
   * AUTHENTICATION_PRIVACY = "authentication,privacy"; public static final String KDC_WORK_DIR =
   * System.getProperty("java.io.tmpdir") + File.separator + "minikdc";
   * 
   * private MiniDFSCluster hdfsCluster; private KDCQuasiService kdcQuasiService = null; private
   * Configuration conf = null; private boolean isSecured = false; private int numberOfDataNodes =
   * -1; private String[] hosts = null; private String DFS_CLUTSER_DIR = null; public static final
   * String SUPER_USER_NAME = "hdfs"; private String hdfsPrincipal = null; private File
   * hdfsKeytabFile = null;
   * 
   * public static final String SECURED_USER = "ampool";
   * 
   * public static final String KERBEROS_NAME_RULES = "RULE:[1:$1@$0](.*)s/(.*)@.
   *//*
     * $1/g" + " RULE:[2:$1/$2@$0](" + SUPER_USER_NAME + ".*)s/(" + SUPER_USER_NAME + ").
     *//*
       * $1/g" + " RULE:[2:$1/$2@$0](" + SECURED_USER + ".*)s/(" + SECURED_USER + ").
       *//*
         * $1/g" + " DEFAULT";
         * 
         * protected static final Log LOG = LogFactory.getLog(HDFSQS.class);
         * 
         * public Configuration getConf() { return conf; }
         * 
         * public MiniDFSCluster getInstance() { return hdfsCluster; }
         * 
         * public KDCQuasiService getKdcQuasiService() { return kdcQuasiService; }
         * 
         * public boolean isSecured() { return isSecured; }
         * 
         * public int getNumberOfDataNodes() { return numberOfDataNodes; }
         * 
         * public String[] getHosts() { return hosts; }
         * 
         * public String getHdfsPrincipal() { return hdfsPrincipal; }
         * 
         * public File getHdfsKeytabFile() { return hdfsKeytabFile; }
         * 
         * public void create(int numberOfDataNodes, String dfsDirPath) throws Exception {
         * this.create(numberOfDataNodes, dfsDirPath, new Configuration()); }
         * 
         * public void create(int numberOfDataNodes, String dfsDirPath, Configuration conf) throws
         * Exception { this.create(numberOfDataNodes, dfsDirPath, conf, false); }
         * 
         * public void create(int numberOfDataNodes, String dfsDirPath, boolean isSecured) throws
         * Exception { this.create(numberOfDataNodes, dfsDirPath, new Configuration(), isSecured); }
         * 
         * public void create(int numberOfDataNodes, String dfsDirPath, Configuration conf, boolean
         * isSecured) throws Exception { // Create simple cluster if (isSecured) { File kdcWorkDir =
         * new File(KDC_WORK_DIR); if (kdcWorkDir.exists()) { FileUtils.deleteDirectory(kdcWorkDir);
         * } KDCQuasiService kdcQuasiService = new KDCQuasiService();
         * kdcQuasiService.create(KDC_WORK_DIR); this.create(numberOfDataNodes, dfsDirPath, conf,
         * kdcQuasiService); } else { hdfsCluster = createHDFSCluster(numberOfDataNodes, dfsDirPath,
         * conf); } }
         * 
         * public void create(int numberOfDataNodes, String dfsDirPath, KDCQuasiService
         * kdcQuasiService) throws Exception { this.create(numberOfDataNodes, dfsDirPath, new
         * Configuration(), kdcQuasiService); }
         * 
         * public void create(int numberOfDataNodes, String dfsDirPath, Configuration conf,
         * KDCQuasiService kdcQuasiService) throws Exception { this.isSecured = isSecured;
         * this.kdcQuasiService = kdcQuasiService;
         * 
         * // Making cluster secured conf = setSecuredClusterConfig(conf, kdcQuasiService); //
         * Create secured cluster with given kdc hdfsCluster = createHDFSCluster(numberOfDataNodes,
         * dfsDirPath, conf); }
         * 
         * private MiniDFSCluster createHDFSCluster(int numberOfDataNodes, String dfsDirPath,
         * Configuration conf) throws IOException { this.conf = conf; this.numberOfDataNodes =
         * numberOfDataNodes; this.DFS_CLUTSER_DIR = dfsDirPath;
         * 
         * MiniDFSCluster hdfsCluster = null;
         * 
         * File baseDir = new File(DFS_CLUTSER_DIR).getAbsoluteFile();
         * FileUtil.fullyDelete(baseDir); conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR,
         * baseDir.getAbsolutePath());
         * 
         * LOG.info("Auth type in config is: " +
         * conf.get(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION));
         * UserGroupInformation.setLoginUser(null); UserGroupInformation.setConfiguration(conf);
         * KerberosName.setRules(KERBEROS_NAME_RULES); LOG.info("Security status in UGI:" +
         * UserGroupInformation.isSecurityEnabled());
         * 
         * MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
         * builder.numDataNodes(numberOfDataNodes); try { hdfsCluster = builder.build(); } catch
         * (IOException e) { LOG.error("Error in creating mini DFS cluster ", e); throw e; }
         * ListIterator<DataNode> itr = hdfsCluster.getDataNodes().listIterator();
         * System.out.println("NameNode: " +
         * hdfsCluster.getNameNode().getNameNodeAddressHostPortString()); while (itr.hasNext()) {
         * DataNode dn = itr.next(); System.out.println("DataNode: " + dn.getDisplayName()); }
         * return hdfsCluster; }
         * 
         * public void start() {
         * 
         * }
         * 
         * public void stop() { if (hdfsCluster != null) { for (DataNode dn :
         * hdfsCluster.getDataNodes()) { hdfsCluster.stopDataNode(dn.getDisplayName()); } } }
         * 
         * public void destroy() { if (hdfsCluster != null) { hdfsCluster.shutdown(true); File
         * baseDir = new File(DFS_CLUTSER_DIR).getAbsoluteFile(); if (baseDir.exists())
         * baseDir.delete(); } if (this.kdcQuasiService != null) { this.kdcQuasiService.stop(); if
         * (new File(KDC_WORK_DIR).exists()) { new File(KDC_WORK_DIR).delete(); } } }
         * 
         * private Configuration setSecuredClusterConfig(Configuration conf, KDCQuasiService
         * kdcQuasiService) throws Exception { // Set HDFS configuration to Kerberos
         * conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, KERBEROS_AUTH);
         * 
         * this.hdfsPrincipal = SUPER_USER_NAME + "/localhost@" + kdcQuasiService.getRealm();
         * this.hdfsKeytabFile = new File(kdcQuasiService.getKeyTabDir(), SUPER_USER_NAME +
         * ".keytab");
         * 
         * kdcQuasiService.addPrincipal(hdfsKeytabFile, SUPER_USER_NAME + "/localhost",
         * "HTTP/localhost");
         * 
         * String spnegoPrincipal = "HTTP/localhost@" + kdcQuasiService.getRealm();
         * 
         * conf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
         * conf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, hdfsKeytabFile.getAbsolutePath());
         * 
         * conf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
         * conf.set(DFS_DATANODE_KEYTAB_FILE_KEY, hdfsKeytabFile.getAbsolutePath());
         * conf.set(DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, spnegoPrincipal);
         * 
         * conf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
         * conf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication,privacy");
         * conf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
         * conf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
         * conf.set(DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");
         * conf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY, 10);
         * 
         * conf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, AUTHENTICATION_PRIVACY);
         * 
         * String keystoresDir = new File(kdcQuasiService.getKeyTabDir()).getAbsolutePath(); String
         * sslConfDir = KeyStoreTestUtil.getClasspathDir(this.getClass());
         * KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false); //
         * conf.set(DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY, //
         * KeyStoreTestUtil.getClientSSLConfigFileName()); //
         * conf.set(DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY, //
         * KeyStoreTestUtil.getServerSSLConfigFileName()); return conf; }
         * 
         * public void createHDFSDir(Path path) throws IOException { this.createUserDir(path,
         * SUPER_USER_NAME); }
         * 
         * public void createUserHomeDir(String userName) throws IOException { Path path = new
         * Path("/user/" + userName); createUserDir(path, userName); }
         * 
         * public void createUserDir(Path path, String userName) throws IOException { FileSystem fs
         * = hdfsCluster.getFileSystem(); fs.mkdirs(path, new FsPermission(FsAction.ALL,
         * FsAction.ALL, FsAction.ALL)); fs.setOwner(path, userName, userName); }
         * 
         * public PrincipalKeytab createUser(String userName) throws KDCQuasiServiceException {
         * PrincipalKeytab userPK = getKdcQuasiService().addPrincipal(userName + ".keytab",
         * userName); return userPK; }
         * 
         * public String getNameNodeAddressHostPortString() { return
         * getInstance().getNameNode().getNameNodeAddressHostPortString(); }
         * 
         * public static void main(String[] args) throws Exception { HDFSQS hdfsqs = new HDFSQS();
         * // Configuration config = new Configuration(); // hdfsqs.create(3,
         * "/tmp/"+System.currentTimeMillis(), config);
         * 
         * KDCQuasiService kdcQuasiService = new KDCQuasiService();
         * kdcQuasiService.create("/tmp/kdc");
         * 
         * File krb5Config = kdcQuasiService.getInstance().getKrb5conf();
         * FileUtils.copyFile(krb5Config, new File("/tmp/krb5.conf"));
         * 
         * Configuration config = new Configuration(); config.set(HADOOP_SECURITY_AUTH_TO_LOCAL,
         * HDFSQS.KERBEROS_NAME_RULES); KerberosName.setRules(HDFSQS.KERBEROS_NAME_RULES);
         * UserGroupInformation.setConfiguration(config); hdfsqs.create(3, dfsPath, config,
         * kdcQuasiService); hdfsqs.createUserDir(new Path(hdfsStoreHomeDir), HDFSQS.SECURED_USER);
         * hdfsqs.generateConfiguration(hdfsqs, securityConfFilePath);
         * 
         * }
         * 
         * private void generateConfiguration(HDFSQS hdfsQuasiService, String securityConfPath)
         * throws KDCQuasiServiceException, IOException { Configuration conf =
         * hdfsQuasiService.getConf(); PrincipalKeytab principalKeytab =
         * hdfsQuasiService.getKdcQuasiService() .addPrincipal(new File(USER_KEY_TAB_PATH),
         * USER_PRINCIPAL); String xmlConfig = "<configuration>\n" + "     <property>\n" +
         * "             <name>hadoop.security.authentication</name>\n" +
         * "             <value>kerberos</value>\n" + "     </property>\n" + "     <property>\n" +
         * "              <name>dfs.namenode.kerberos.principal</name>\n" +
         * "              <!-- Copy value from /etc/hadoop/hdfs-site.xml (dfs.namenode.kerberos.principal)-->\n"
         * + "               <value> " + conf.get("dfs.namenode.kerberos.principal") + " </value>\n"
         * + "     </property>\n" + "     <property>\n" +
         * "              <name>gemfirexd.kerberos.principal</name>\n" + "              <value>" +
         * USER_PRINCIPAL + "@" + hdfsQuasiService.getKdcQuasiService().getRealm() + "</value>\n" +
         * "              <!-- _HOST will be replaced by result of \"hostname -f\" on each. This way you can place same file on each node -->\n"
         * + "     </property>\n" + "     <property>\n" +
         * "              <name>gemfirexd.kerberos.keytab.file</name>\n" + "              <value>" +
         * principalKeytab.keytabFile + "</value>\n" +
         * "              <!-- Location of keytab. This location should be present on each node and should access to read for monarch -->\n"
         * + "     </property>\n";
         * 
         * String authToLocal = conf.get("hadoop.security.auth_to_local");
         * 
         * if (authToLocal != null) { xmlConfig = xmlConfig + "     <property>\n" +
         * "              <name>hadoop.security.auth_to_local</name>\n" + "              <value>" +
         * conf.get("hadoop.security.auth_to_local") + "</value>\n" +
         * "              <!-- Value same as hadoop.security.auth_to_local in core-site.xml -->\n" +
         * "      </property>\n"; } xmlConfig = xmlConfig + " </configuration> ";
         * 
         * FileWriter fileWriter = null; try { fileWriter = new FileWriter(securityConfPath, false);
         * fileWriter.write(xmlConfig); fileWriter.flush(); } finally { if (fileWriter != null)
         * fileWriter.close(); } }
         */

}
