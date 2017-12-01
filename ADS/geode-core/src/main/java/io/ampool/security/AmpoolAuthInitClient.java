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

import static io.ampool.security.SecurityConfigurationKeysPublic.ENABLE_KERBEROS_AUTHC;
import static io.ampool.security.SecurityConfigurationKeysPublic.ENABLE_LDAP_AUTHC;

import org.apache.geode.LogWriter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.security.AuthInitialize;
import org.apache.geode.security.AuthenticationFailedException;

import java.util.Properties;

public class AmpoolAuthInitClient implements AuthInitialize {

  public static AmpoolAuthInitClient create() {
    return new AmpoolAuthInitClient();
  }


  @Override
  public void close() {

  }

  @Override
  public void init(LogWriter systemLogger, LogWriter securityLogger)
      throws AuthenticationFailedException {

  }

  @Override
  public Properties getCredentials(Properties securityProps, DistributedMember server,
      boolean isPeer) throws AuthenticationFailedException {
    if (securityProps.containsKey(ENABLE_LDAP_AUTHC)
        && Boolean.parseBoolean(securityProps.getProperty(ENABLE_LDAP_AUTHC))) {
      return securityProps;
    } else if (securityProps.containsKey(ENABLE_KERBEROS_AUTHC)
        && Boolean.parseBoolean(securityProps.getProperty(ENABLE_KERBEROS_AUTHC))) {
      KerberosAuthInit krbAuthInit = KerberosAuthInit.create();
      // krbAuthInit.init();
      return krbAuthInit.getCredentials(securityProps, server, isPeer);
    }
    return securityProps;
  }
}
