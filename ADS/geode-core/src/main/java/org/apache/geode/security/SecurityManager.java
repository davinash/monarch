/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.security;

import java.util.Properties;
import java.util.Set;

import org.apache.geode.distributed.DistributedSystem;

import javax.resource.NotSupportedException;

/**
 * User implementation of a authentication/authorization logic for Integrated Security. The
 * implementation will guard client/server, JMX, Pulse, GFSH commands
 *
 * @since Geode 1.0
 */
public interface SecurityManager {

  /**
   * Initialize the SecurityManager. This is invoked when a cache is created
   *
   * @param securityProps the security properties obtained using a call to
   *        {@link DistributedSystem#getSecurityProperties}
   * @throws AuthenticationFailedException if some exception occurs during the initialization
   */
  default void init(Properties securityProps) {}

  /**
   * Initialize only the authorizer part of the security manager.
   * 
   * @param securityProps the security properties obtained using a call to
   *        {@link DistributedSystem#getSecurityProperties}
   */
  default void authzInit(Properties securityProps) {}

  /**
   * Invalidate Authz cache
   */
  default void invalidateAuthzCache() throws NotSupportedException {}

  /**
   * Verify the credentials provided in the properties
   * 
   * @param credentials it contains the security-username and security-password as keys of the
   *        properties
   * @return a serializable principal object
   * @throws AuthenticationFailedException
   */
  Object authenticate(Properties credentials) throws AuthenticationFailedException;

  /**
   * Authorize the ResourcePermission for a given Principal
   * 
   * @param principal The principal that's requesting the permission
   * @param permission The permission requested
   * @return true if authorized, false if not
   */
  default boolean authorize(Object principal, ResourcePermission permission) {
    return true;
  }

  /**
   * Create a new role in the authorization DB
   * 
   * @param roleName
   */
  default void createRole(String roleName) throws Exception {
    throw new NotSupportedException();
  }

  /**
   * Drop a role from the authorization DB.
   * 
   * @param roleName
   */
  default void dropRole(String roleName) throws Exception {
    throw new NotSupportedException();
  }

  /**
   * Add a role to a group
   * 
   * @param roleName Name of the role to be added to group
   * @param groups Names of the group to be modified
   */
  default void addRoleToGroups(String roleName, Set<String> groups) throws Exception {
    throw new NotSupportedException();
  }

  /**
   * Delete a role from group.
   * 
   * @param roleName Name of the role to be removed from group.
   * @param groups Name of the group being modified.
   */
  default void deleteRoleFromGroups(String roleName, Set<String> groups) throws Exception {
    throw new NotSupportedException();
  }

  /**
   * Grant a privilege to a role.
   * 
   * @param roleName Name of the role to be modified.
   * @param privilege Privilege to be granted.
   */
  default void grantPrivileges(String roleName, ResourcePermission privilege) throws Exception {
    throw new NotSupportedException();
  }

  /**
   *
   * @param roleName
   * @param privilege
   */
  default void revokePrivileges(String roleName, ResourcePermission privilege) throws Exception {
    throw new NotSupportedException();
  }

  /**
   * Get the roles assigned to a group
   * 
   * @param groupName Name of the group.
   * @return A set of roles assigned to the group.
   */
  default Set<String> listRoles(String groupName) throws Exception {
    throw new NotSupportedException();
  }

  /**
   * Get the privileges assigned to a role
   * 
   * @param roleName Name of the role.
   * @return A set of permissions assigned to the role.
   */
  default Set<ResourcePermission> listPrivileges(String roleName) throws Exception {
    throw new NotSupportedException();
  }

  /**
   * Close any resources used by the SecurityManager, called when a cache is closed.
   */
  default void close() {}
}
