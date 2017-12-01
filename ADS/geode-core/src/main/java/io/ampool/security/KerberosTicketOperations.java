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
import io.ampool.security.KerberosUtils;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import javax.security.auth.Subject;

/**
 * <em>INTERNAL</em> Kerberos ticket related operations.
 */

@InterfaceAudience.Private
@InterfaceStability.Stable
public class KerberosTicketOperations {
  private static final Logger logger = LogService.getLogger();


  // Begin the initiation of a security context with the target service.
  public static byte[] acquireServiceTicket(Subject subject, String servicePrincipalName)
      throws GSSException, PrivilegedActionException, IllegalAccessException, NoSuchFieldException,
      ClassNotFoundException {
    byte[] serviceTicket = null;

    // Kerberos version 5 OID
    Oid krb5Oid = KerberosUtils.getOidInstance("GSS_KRB5_MECH_OID");
    GSSManager manager = GSSManager.getInstance();
    GSSName serverName = manager.createName(servicePrincipalName,
        KerberosUtils.getOidInstance("NT_GSS_KRB5_PRINCIPAL")); // GSSName.NT_HOSTBASED_SERVICE);
    final GSSContext context =
        manager.createContext(serverName, krb5Oid, null, GSSContext.DEFAULT_LIFETIME);

    // The GSS context initiation has to be performed as a privileged action.
    serviceTicket = Subject.doAs(subject, new PrivilegedExceptionAction<byte[]>() {
      @Override
      public byte[] run() throws Exception {
        byte[] serviceTicket = null;
        byte[] token = new byte[0];
        // This is a one pass context initialisation.
        context.requestMutualAuth(false);
        context.requestCredDeleg(false);
        serviceTicket = context.initSecContext(token, 0, token.length);
        return serviceTicket;
      }
    });

    return serviceTicket;
  }

  // Completes the security context initialisation and returns the client name.
  public static String validateServiceTicket(Subject subject, final byte[] serviceTicket)
      throws GSSException, IllegalAccessException, NoSuchFieldException, ClassNotFoundException,
      PrivilegedActionException {
    // Kerberos version 5 OID
    Oid krb5Oid = KerberosUtils.getOidInstance("GSS_KRB5_MECH_OID");


    // Accept the context and return the client principal name.
    return Subject.doAs(subject, new PrivilegedExceptionAction<String>() {

      @Override
      public String run() throws Exception {
        String clientName = null;
        // Identify the server that communications are being made to.
        GSSManager manager = GSSManager.getInstance();
        GSSContext context = manager.createContext((GSSCredential) null);
        context.acceptSecContext(serviceTicket, 0, serviceTicket.length);
        clientName = context.getSrcName().toString();
        return clientName;
      }
    });
  }
}
