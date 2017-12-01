package io.ampool.security;

import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.ResourcePermission;

import java.util.Properties;
import java.util.Set;
import javax.resource.NotSupportedException;

/**
 * Authorization interface used from the AmpoolSecurityManager for Integrated Security. The
 * implementation will guard client/server, JMX, Pulse, GFSH commands
 *
 * @since Ampool 1.3.1
 */
public interface Authorizer {
  /**
   * Initialize the Authenticator. This is invoked from AmpoolSecurityManager when a cache is
   * created
   *
   * @param securityProps the security properties received in SecurityManager
   * @throws AuthenticationFailedException if some exception occurs during the initialization
   */
  void init(Properties securityProps);

  /**
   * Authorize the ResourcePermission for a given Principal
   *
   * @param principal The principal that's requesting the permission
   * @param permission The permission requested
   * @return true if authorized, false if not
   */
  boolean authorize(Object principal, ResourcePermission permission);

  /**
   * Create a new role in the authorization DB
   * 
   * @param roleName
   */
  default void createRole(String roleName) throws Exception {
    throw new NotSupportedException();
  }

  /**
   * Invalidate privilege cache
   */
  default void invalidateCache() throws NotSupportedException {
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
   * @param groups Name of the group to be modified
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
}
