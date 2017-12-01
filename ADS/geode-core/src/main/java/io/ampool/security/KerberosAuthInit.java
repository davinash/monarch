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

import static io.ampool.security.SecurityConfigurationKeysPublic.KERBEROS_KRB5_CONF_PATH;
import static io.ampool.security.SecurityConfigurationKeysPublic.KERBEROS_DEBUG_ENABLE;
import static io.ampool.security.SecurityConfigurationKeysPublic.KERBEROS_SECURITY_PASSWORD;
import static io.ampool.security.SecurityConfigurationKeysPublic.KERBEROS_SECURITY_USERNAME;
import static io.ampool.security.SecurityConfigurationKeysPublic.KERBEROS_SERVICE_KEYTAB_PATH;
import static io.ampool.security.SecurityConfigurationKeysPublic.KERBEROS_SERVICE_PRINCIPAL;
import static io.ampool.security.SecurityConfigurationKeysPublic.KERBEROS_SERVICE_TICKET;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import org.apache.geode.LogWriter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.security.AuthInitialize;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.logging.log4j.Logger;
import org.ietf.jgss.GSSException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedActionException;
import java.util.Properties;
import java.util.Set;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * Implementation of {@link org.apache.geode.security.AuthInitialize} for kerberos authentication.
 * It acquires kerberos service ticket for Geode/ampool service using kerberos TGT from Ticket Cache
 * or keytab if specified and passes it to server.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class KerberosAuthInit implements AuthInitialize {
  private static final Logger logger = LogService.getLogger();

  private boolean loadFromTicketCache = true;
  private String keytabPath = null;
  private String userPrincipal = null;
  private boolean enableDebug = false;
  public static final String TGT_SERVER_NAME_PREFIX = "krbtgt";


  public static KerberosAuthInit create() {
    return new KerberosAuthInit();
  }

  @Override
  public void init(LogWriter systemLogger, LogWriter securityLogger)
      throws AuthenticationFailedException {

  }

  @Override
  public Properties getCredentials(Properties securityProps, DistributedMember server,
      boolean isPeer) throws AuthenticationFailedException {

    // Login user from kerberos cache or from keytab based on variables set in security properties
    // and store ampool ticket in properties and return it
    if (isPeer) {
      loadPeerSpecificProperties(securityProps);
    } else {
      loadClientSpecificProperties(securityProps);
    }

    if (securityProps.containsKey(KERBEROS_DEBUG_ENABLE)) {
      logger.debug("Debug Mode: " + securityProps.getProperty(KERBEROS_DEBUG_ENABLE));
      enableDebug = Boolean.parseBoolean(securityProps.getProperty(KERBEROS_DEBUG_ENABLE));
    }

    String servicePrincipalName = null;
    if (securityProps.containsKey(KERBEROS_SERVICE_PRINCIPAL)
        && !securityProps.getProperty(KERBEROS_SERVICE_PRINCIPAL).trim().equalsIgnoreCase("")) {
      servicePrincipalName = securityProps.getProperty(KERBEROS_SERVICE_PRINCIPAL);
      // REPLACE _HOST with actual hostname
      servicePrincipalName =
          servicePrincipalName.replaceAll(KerberosUtils.HOSTNAME_MARKER, server.getHost());
      logger.info("Using Service Principal Name:" + servicePrincipalName);
    } else {
      String errorMsg = "You must specify a valid value for property '" + KERBEROS_SERVICE_PRINCIPAL
          + "' while using kerberos authentication";
      logger.error(errorMsg);
      throw new GemFireSecurityException(errorMsg);
    }

    Subject subject = null;
    try {
      subject = getSingedOnSubject();
    } catch (LoginException e) {
      String errorMsg = "Error while logging in";
      if (loadFromTicketCache) {
        errorMsg = errorMsg + " from Kerberos ticket cache";
      } else {
        errorMsg = errorMsg + " for '" + userPrincipal + "' from Keytab " + keytabPath;
      }
      logger.error(errorMsg);
      throw new AuthenticationFailedException(errorMsg, e);
    }

    validateSubject(subject);

    byte[] serviceTicket = acqurieServiceTicket(subject, servicePrincipalName);

    Properties propsWithTicket = securityProps;
    propsWithTicket.put(KERBEROS_SERVICE_TICKET, serviceTicket);
    return propsWithTicket;
  }

  @Override
  public Properties getCredentials(Properties securityProps) throws AuthenticationFailedException {

    // Login user from kerberos cache or from keytab based on variables set in security properties
    // and store ampool ticket in properties and return it
    loadClientSpecificProperties(securityProps);

    if (securityProps.containsKey(KERBEROS_DEBUG_ENABLE)) {
      logger.debug("Debug Mode: " + securityProps.getProperty(KERBEROS_DEBUG_ENABLE));
      enableDebug = Boolean.parseBoolean(securityProps.getProperty(KERBEROS_DEBUG_ENABLE));
    }

    String servicePrincipalName = null;
    if (securityProps.containsKey(KERBEROS_SERVICE_PRINCIPAL)
        && !securityProps.getProperty(KERBEROS_SERVICE_PRINCIPAL).trim().equalsIgnoreCase("")) {
      servicePrincipalName = securityProps.getProperty(KERBEROS_SERVICE_PRINCIPAL);
      // TODO: REPLACE _HOST with actual hostname
      // servicePrincipalName =
      // servicePrincipalName.replaceAll(KerberosUtils.HOSTNAME_MARKER, server.getHost());
      logger.info("Using Service Principal Name:" + servicePrincipalName);
    } else {
      String errorMsg = "You must specify a valid value for property '" + KERBEROS_SERVICE_PRINCIPAL
          + "' while using kerberos authentication";
      logger.error(errorMsg);
      throw new GemFireSecurityException(errorMsg);
    }

    Subject subject = null;
    try {
      subject = getSingedOnSubject();
    } catch (LoginException e) {
      String errorMsg = "Error while logging in";
      if (loadFromTicketCache) {
        errorMsg = errorMsg + " from Kerberos ticket cache";
      } else {
        errorMsg = errorMsg + " for '" + userPrincipal + "' from Keytab " + keytabPath;
      }
      logger.error(errorMsg);
      throw new AuthenticationFailedException(errorMsg, e);
    }

    validateSubject(subject);

    byte[] serviceTicket = acqurieServiceTicket(subject, servicePrincipalName);

    Properties propsWithTicket = securityProps;
    propsWithTicket.put(KERBEROS_SERVICE_TICKET, serviceTicket);
    return propsWithTicket;
  }

  private void loadClientSpecificProperties(Properties securityProps) {
    // Configure krb5 path
    if (securityProps.containsKey(KERBEROS_KRB5_CONF_PATH)) {
      String krb5Path = securityProps.getProperty(KERBEROS_KRB5_CONF_PATH);
      if (!KerberosUtils.isValidFilePath(krb5Path)) {
        String errorMsg = "Invalid krb5 conf path specified in '" + KERBEROS_KRB5_CONF_PATH + "' ";
        logger.error(errorMsg);
        throw new GemFireSecurityException(errorMsg);
      }
      logger.info("Using KRB5 conf: " + krb5Path);
      KerberosUtils.configureKRB5Path(krb5Path);
    }

    if (securityProps.containsKey(KERBEROS_SECURITY_USERNAME)) {
      logger.info(
          "Using User principal '" + securityProps.getProperty(KERBEROS_SECURITY_USERNAME) + "'");
      userPrincipal = securityProps.getProperty(KERBEROS_SECURITY_USERNAME);
    }

    if (securityProps.containsKey(KERBEROS_SECURITY_PASSWORD)) {
      if (userPrincipal != null) {
        loadFromTicketCache = false;
        keytabPath = securityProps.getProperty(KERBEROS_SECURITY_PASSWORD);
        if (!KerberosUtils.isValidFilePath(keytabPath)) {
          String errorMsg =
              "Unable to Login. Invalid keytab specified in '" + KERBEROS_SECURITY_PASSWORD + "' ";
          logger.error(errorMsg);
          throw new GemFireSecurityException(errorMsg);
        }
        logger.info("Using key tab file: " + keytabPath);
      } else {
        String errorMsg = "Invalid Configuration. You must specify '" + KERBEROS_SECURITY_USERNAME
            + "' while using login from keytab";
        logger.error(errorMsg);
        throw new GemFireSecurityException(errorMsg);
      }
    }
  }

  private void loadPeerSpecificProperties(Properties securityProps) {
    loadFromTicketCache = false;
    if (securityProps.containsKey(KERBEROS_SERVICE_PRINCIPAL)
        && !securityProps.getProperty(KERBEROS_SERVICE_PRINCIPAL).trim().equalsIgnoreCase("")) {
      userPrincipal = securityProps.getProperty(KERBEROS_SERVICE_PRINCIPAL);
      securityProps.put(KERBEROS_SECURITY_USERNAME, userPrincipal);
      // REPLACE _HOST with actual hostname
      String hostname = null;
      try {
        hostname = InetAddress.getLocalHost().getCanonicalHostName();
      } catch (UnknownHostException e) {
        logger.error("Error while getting hostname for the machine");
        throw new GemFireSecurityException("Error while getting hostname", e);
      }
      userPrincipal = userPrincipal.replaceAll(KerberosUtils.HOSTNAME_MARKER, hostname);
      logger.info("Using Server principal: " + userPrincipal);
    } else {
      String errorMsg = "Invalid Configuration." + " You must specify a valid value for '"
          + KERBEROS_SERVICE_PRINCIPAL + "' while using kerberos authentication";
      logger.error(errorMsg);
      throw new GemFireSecurityException(errorMsg);
    }

    if (securityProps.containsKey(KERBEROS_SERVICE_KEYTAB_PATH)) {
      keytabPath = securityProps.getProperty(KERBEROS_SERVICE_KEYTAB_PATH);
      if (!KerberosUtils.isValidFilePath(keytabPath)) {
        String errorMsg =
            "Invalid keytabPath path specified in '" + KERBEROS_SERVICE_KEYTAB_PATH + "' ";
        logger.error(errorMsg);
        throw new GemFireSecurityException(errorMsg);
      }
      securityProps.setProperty(KERBEROS_SECURITY_PASSWORD, keytabPath);
      logger.info("Using Keytab file: " + keytabPath);
    } else {
      String errorMsg = "Invalid Configuration." + " You must specify '"
          + KERBEROS_SERVICE_KEYTAB_PATH + "' while using kerberos authentication";
      logger.error(errorMsg);
      throw new GemFireSecurityException(errorMsg);
    }

    // Optional Params
    if (securityProps.containsKey(KERBEROS_KRB5_CONF_PATH)) {
      String krb5Path = securityProps.getProperty(KERBEROS_KRB5_CONF_PATH);
      if (!KerberosUtils.isValidFilePath(krb5Path)) {
        String errorMsg = "Invalid krb5 conf path specified in '" + KERBEROS_KRB5_CONF_PATH + "' ";
        logger.error(errorMsg);
        throw new GemFireSecurityException(errorMsg);
      }
      logger.info("Using KRB5 conf: " + krb5Path);
      KerberosUtils.configureKRB5Path(krb5Path);
    }
  }

  private void validateSubject(Subject subject) {
    Set<Object> set = subject.getPrivateCredentials();
    boolean foundTGT = false;
    if (set != null && set.size() > 0) {
      for (Object obj : set) {
        if (obj instanceof KerberosTicket) {
          KerberosTicket kt = (KerberosTicket) obj;
          String serverName = kt.getServer().getName();
          if (serverName.startsWith(TGT_SERVER_NAME_PREFIX)) {
            foundTGT = true;
          }
        }
      }
    }
    if (!foundTGT) {
      String errorMsg = null;
      if (loadFromTicketCache) {
        errorMsg = "Unable to load Kerberos TGT. Consider kinit.";
      } else {
        errorMsg = "Login failed for principal '" + userPrincipal + "' using keytab '" + keytabPath
            + "'. Specify correct keytab file path";
      }
      logger.error(errorMsg);
      throw new AuthenticationFailedException(errorMsg);
    }
  }

  /**
   * Loads Subject with TGT using {@link LoginContext} either from kerberos ticket cache or from
   * keytab if speicfied
   *
   * @throws LoginException
   */
  private Subject getSingedOnSubject() throws LoginException {
    Subject userSubject = new Subject();
    LoginContext login = null;
    if (loadFromTicketCache) {
      login = KerberosUtils.createLoginContext(KerberosLoginConfiguration.USER_KERBEROS_CONFIG_NAME,
          userSubject, new KerberosLoginConfiguration(enableDebug));
    } else {
      login =
          KerberosUtils.createLoginContext(KerberosLoginConfiguration.KEYTAB_KERBEROS_CONFIG_NAME,
              userSubject, new KerberosLoginConfiguration(userPrincipal, keytabPath, enableDebug));
    }
    login.login();
    return userSubject;
  }

  private byte[] acqurieServiceTicket(Subject userSubject, String servicePrincipalName) {
    byte[] serviceTicket = null;
    try {
      serviceTicket =
          KerberosTicketOperations.acquireServiceTicket(userSubject, servicePrincipalName);
    } catch (GSSException | PrivilegedActionException | IllegalAccessException
        | NoSuchFieldException | ClassNotFoundException e) {
      String errorMsg =
          "Error while acquiring service ticket for service '" + servicePrincipalName + "'";
      logger.error(errorMsg);
      throw new AuthenticationFailedException(errorMsg, e);
    }
    return serviceTicket;
  }

  @Override
  public void close() {
    // Close if anything. Not Required as of now
  }
}
