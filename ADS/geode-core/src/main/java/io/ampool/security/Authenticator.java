package io.ampool.security;

import org.apache.geode.security.AuthenticationFailedException;

import java.util.Properties;

/**
 * Authentication interface used from the AmpoolSecurityManager for Integrated Security. The
 * implementation will guard client/server, JMX, Pulse, GFSH commands
 *
 * @since Ampool 1.3.1
 */
public interface Authenticator {
  /**
   * Initialize the Authenticator. This is invoked from AmpoolSecurityManager when a cache is
   * created
   *
   * @param securityProps the security properties received in SecurityManager
   * @throws AuthenticationFailedException if some exception occurs during the initialization
   */
  void init(Properties securityProps);

  /**
   * Verify the credentials provided in the properties
   *
   * @param credentials it contains the security-username and security-password as keys of the
   *        properties
   * @return a serializable principal object
   * @throws AuthenticationFailedException
   */
  Object authenticate(Properties credentials);
}
