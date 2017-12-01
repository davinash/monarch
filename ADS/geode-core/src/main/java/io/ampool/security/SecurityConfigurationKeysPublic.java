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

/**
 * Security properties
 */

@InterfaceAudience.Private
@InterfaceStability.Stable
public interface SecurityConfigurationKeysPublic {

  // common config keys
  String KERBEROS_KRB5_CONF_PATH = "security-kerberos-krb5-conf-path";
  String KERBEROS_SERVICE_PRINCIPAL = "security-kerberos-service-principal";
  String KERBEROS_SERVICE_TICKET = "security-kerberos-service-ticket";
  String KERBEROS_DEBUG_ENABLE = "security-kerberos-debug-enable";

  // Server configuration keys
  String KERBEROS_SERVICE_KEYTAB_PATH = "security-kerberos-service-keytab-path";

  // Client configuration keys
  String KERBEROS_SECURITY_USERNAME = "security-username";
  String KERBEROS_SECURITY_PASSWORD = "security-password";

  public static String ENABLE_KERBEROS_AUTHC = "security-enable-kerberos-authc";
  public static String ENABLE_LDAP_AUTHC = "security-enable-ldap-authc";
  public static String ENABLE_JSON_AUTHZ = "security-enable-json-authz";
  public static String ENABLE_LDAP_AUTHZ = "security-enable-ldap-authz";
  public static String ENABLE_SENTRY_AUTHZ = "security-enable-sentry-authz";
  public static String CUSTOM_AUTHENTICATOR_CLASS = "security-custom-authenticator-class";


}
