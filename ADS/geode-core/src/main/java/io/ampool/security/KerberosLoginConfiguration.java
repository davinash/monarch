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

import javax.security.auth.login.AppConfigurationEntry;
import java.util.HashMap;
import java.util.Map;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.security.KerberosUtils;
import org.apache.geode.security.GemFireSecurityException;

/**
 *
 * A JAAS configuration that defines the login modules that we want to use for login.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class KerberosLoginConfiguration extends javax.security.auth.login.Configuration {

  private static String OS_LOGIN_MODULE_NAME;

  static {
    OS_LOGIN_MODULE_NAME = KerberosUtils.getOSLoginModuleName();
  }

  String keytabPrincipal = null;
  String keytabFile = null;
  boolean enableDebug = false;

  public KerberosLoginConfiguration() {
    this(false);
  }

  public KerberosLoginConfiguration(boolean enableDebug) {
    this(null, null, enableDebug);
  }

  public KerberosLoginConfiguration(String keytabPrincipal, String keytabFile) {
    this(keytabFile, keytabPrincipal, false);
  }

  public KerberosLoginConfiguration(String keytabPrincipal, String keytabFile,
      boolean enableDebug) {
    this.keytabPrincipal = keytabPrincipal;
    this.keytabFile = keytabFile;
    this.enableDebug = enableDebug;
  }

  public static final String USER_KERBEROS_CONFIG_NAME = "user-ticket-cache";
  public static final String KEYTAB_KERBEROS_CONFIG_NAME = "user-keytab";

  private Map<String, String> BASIC_JAAS_OPTIONS = new HashMap<String, String>();
  private AppConfigurationEntry OS_SPECIFIC_LOGIN = null;
  private Map<String, String> USER_KERBEROS_OPTIONS = new HashMap<String, String>();
  private AppConfigurationEntry USER_KERBEROS_LOGIN = null;
  private Map<String, String> KEYTAB_KERBEROS_OPTIONS = new HashMap<String, String>();
  private AppConfigurationEntry KEYTAB_KERBEROS_LOGIN = null;
  private AppConfigurationEntry[] USER_KERBEROS_CONF = null;
  private AppConfigurationEntry[] KEYTAB_KERBEROS_CONF = null;

  @Override
  public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
    initalizeConfig();
    if (USER_KERBEROS_CONFIG_NAME.equals(appName)) {
      return USER_KERBEROS_CONF;
    } else if (KEYTAB_KERBEROS_CONFIG_NAME.equals(appName)) {
      if (keytabFile == null || keytabPrincipal == null) {
        throw new GemFireSecurityException("Invalid configuration."
            + " You must specify keytab file and principal while logging in from keytab");
      }
      if (KerberosUtils.IBM_JAVA) {
        KEYTAB_KERBEROS_OPTIONS.put("useKeytab", prependFileAuthority(keytabFile));
      } else {
        KEYTAB_KERBEROS_OPTIONS.put("keyTab", keytabFile);
      }
      KEYTAB_KERBEROS_OPTIONS.put("principal", keytabPrincipal);
      return KEYTAB_KERBEROS_CONF;
    }
    return null;
  }

  private void initalizeConfig() {
    BASIC_JAAS_OPTIONS.put("debug", String.valueOf(enableDebug));

    if (KerberosUtils.IBM_JAVA) {
      USER_KERBEROS_OPTIONS.put("useDefaultCcache", "true");
    } else {
      USER_KERBEROS_OPTIONS.put("doNotPrompt", "true");
      USER_KERBEROS_OPTIONS.put("useTicketCache", "true");
    }
    String ticketCache = System.getenv("KRB5CCNAME");
    if (ticketCache != null) {
      if (KerberosUtils.IBM_JAVA) {
        // The first value searched when "useDefaultCcache" is used.
        System.setProperty("KRB5CCNAME", ticketCache);
      } else {
        USER_KERBEROS_OPTIONS.put("ticketCache", ticketCache);
      }
    }
    USER_KERBEROS_OPTIONS.put("renewTGT", "true");
    USER_KERBEROS_OPTIONS.putAll(BASIC_JAAS_OPTIONS);

    USER_KERBEROS_LOGIN = new AppConfigurationEntry(KerberosUtils.getKrb5LoginModuleName(),
        AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL, USER_KERBEROS_OPTIONS);

    if (KerberosUtils.IBM_JAVA) {
      KEYTAB_KERBEROS_OPTIONS.put("credsType", "both");
    } else {
      KEYTAB_KERBEROS_OPTIONS.put("doNotPrompt", "true");
      KEYTAB_KERBEROS_OPTIONS.put("useKeyTab", "true");
      KEYTAB_KERBEROS_OPTIONS.put("storeKey", "true");
    }
    KEYTAB_KERBEROS_OPTIONS.put("refreshKrb5Config", "true");
    KEYTAB_KERBEROS_OPTIONS.putAll(BASIC_JAAS_OPTIONS);

    OS_SPECIFIC_LOGIN = new AppConfigurationEntry(OS_LOGIN_MODULE_NAME,
        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, BASIC_JAAS_OPTIONS);

    USER_KERBEROS_CONF = new AppConfigurationEntry[] {OS_SPECIFIC_LOGIN, USER_KERBEROS_LOGIN};

    KEYTAB_KERBEROS_LOGIN = new AppConfigurationEntry(KerberosUtils.getKrb5LoginModuleName(),
        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, KEYTAB_KERBEROS_OPTIONS);

    KEYTAB_KERBEROS_CONF = new AppConfigurationEntry[] {KEYTAB_KERBEROS_LOGIN};
  }

  private static String prependFileAuthority(String keytabPath) {
    return keytabPath.startsWith("file://") ? keytabPath : "file://" + keytabPath;
  }
}
