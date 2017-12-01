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
package io.ampool.security;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.Oid;

import java.io.File;
import java.lang.reflect.Field;
import javax.security.auth.Subject;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * Kerberos implemetation for Geode members and client authentication Utils.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class KerberosUtils {

  public static String HOSTNAME_MARKER = "_HOST";

  public static final String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";
  /**
   * The java vendor name used in this platform.
   */
  public static final String JAVA_VENDOR_NAME = System.getProperty("java.vendor");

  /**
   * A public static variable to indicate the current java vendor is IBM java or not.
   */
  public static final boolean IBM_JAVA = JAVA_VENDOR_NAME.contains("IBM");

  private static final boolean windows = System.getProperty("os.name").startsWith("Windows");
  private static final boolean is64Bit = System.getProperty("os.arch").contains("64")
      || System.getProperty("os.arch").contains("s390x");
  private static final boolean aix = System.getProperty("os.name").equals("AIX");

  /* Return the OS login module class name */
  public static String getOSLoginModuleName() {
    if (IBM_JAVA) {
      if (windows) {
        return is64Bit ? "com.ibm.security.auth.module.Win64LoginModule"
            : "com.ibm.security.auth.module.NTLoginModule";
      } else if (aix) {
        return is64Bit ? "com.ibm.security.auth.module.AIX64LoginModule"
            : "com.ibm.security.auth.module.AIXLoginModule";
      } else {
        return "com.ibm.security.auth.module.LinuxLoginModule";
      }
    } else {
      return windows ? "com.sun.security.auth.module.NTLoginModule"
          : "com.sun.security.auth.module.UnixLoginModule";
    }
  }

  public static String getKrb5LoginModuleName() {
    return System.getProperty("java.vendor").contains("IBM")
        ? "com.ibm.security.auth.module.Krb5LoginModule"
        : "com.sun.security.auth.module.Krb5LoginModule";
  }

  public static void configureKRB5Path(String krb5ConfPath) {
    if (krb5ConfPath != null && !krb5ConfPath.trim().equalsIgnoreCase("")) {
      System.setProperty(JAVA_SECURITY_KRB5_CONF, krb5ConfPath);
    }
  }

  public static Oid getOidInstance(String oidName)
      throws ClassNotFoundException, GSSException, NoSuchFieldException, IllegalAccessException {
    Class<?> oidClass;
    if (IBM_JAVA) {
      if ("NT_GSS_KRB5_PRINCIPAL".equals(oidName)) {
        // IBM JDK GSSUtil class does not have field for krb5 principal oid
        return new Oid("1.2.840.113554.1.2.2.1");
      }
      oidClass = Class.forName("com.ibm.security.jgss.GSSUtil");
    } else {
      oidClass = Class.forName("sun.security.jgss.GSSUtil");
    }
    Field oidField = oidClass.getDeclaredField(oidName);
    return (Oid) oidField.get(oidClass);
  }

  public static LoginContext createLoginContext(String appName, Subject subject,
      Configuration loginConf) throws LoginException {
    return new LoginContext(appName, subject, null, loginConf);
  }

  public static boolean isValidFilePath(String filePath) {
    boolean isValid = false;
    if (filePath != null && filePath.trim().length() != 0) {
      if (new File(filePath.trim()).exists()) {
        isValid = true;
      }
    }
    return isValid;
  }
}
