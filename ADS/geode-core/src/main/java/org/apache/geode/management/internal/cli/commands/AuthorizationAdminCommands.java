package org.apache.geode.management.internal.cli.commands;

import static io.ampool.monarch.cli.MashCliStrings.AMPOOL_AUTHZ__SECURITY_PROPERTIES;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;

import io.ampool.monarch.cli.MashCliStrings;
import io.ampool.monarch.cli.MashLauncher;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.CreateMTableFunction;
import org.apache.geode.internal.cache.InvalidateAuthzCacheFunction;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResultException;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.SecurityManager;
import org.apache.logging.log4j.Logger;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Commands to administer the authorization metadata
 */
public class AuthorizationAdminCommands extends AbstractCommandsSupport {
  private static final Logger logger = LogService.getLogger();
  SecurityManager securityManager = null;
  public static final String GROUP_SEPARATOR = ",";
  private static final String PRIVILEGE_PART_DELIM = ":";



  private Result initSecurityManager(String secPropertiesFile, String errMsgPrifix) {
    if (secPropertiesFile == null || secPropertiesFile.length() == 0) {
      return ResultBuilder.createShellClientErrorResult(
          errMsgPrifix + "Invalid value for " + AMPOOL_AUTHZ__SECURITY_PROPERTIES);
    }

    Properties securityProperties = new Properties();
    try {
      securityProperties.load(new FileInputStream(secPropertiesFile));
    } catch (IOException e) {
      return ResultBuilder.createShellClientErrorResult(errMsgPrifix + e.getMessage());
    }
    String securityMangerClassName = securityProperties.getProperty(SECURITY_MANAGER);
    if (securityMangerClassName == null || securityMangerClassName.length() == 0) {
      return ResultBuilder.createShellClientErrorResult(errMsgPrifix + "Invalid value for property "
          + SECURITY_MANAGER + " in security properties file");
    }

    if (securityManager == null) {
      securityManager = SecurityService.getObjectOfTypeFromClassName(securityMangerClassName,
          SecurityManager.class);
    }

    if (securityManager == null) {
      return ResultBuilder
          .createShellClientErrorResult(errMsgPrifix + "Failed to instantiate " + SECURITY_MANAGER);
    }
    securityManager.authzInit(securityProperties);
    return null;
  }

  @CliCommand(value = MashCliStrings.CREATE_ROLE, help = MashCliStrings.CREATE_ROLE__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {MashCliStrings.TOPIC_AMPOOL_AUTHZ})
  public Result createRole(
      @CliOption(key = MashCliStrings.CREATE_ROLE__ROLE_NAME, mandatory = true,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.CREATE_ROLE__ROLE_NAME__HELP) String roleName,
      @CliOption(key = AMPOOL_AUTHZ__SECURITY_PROPERTIES,
          optionContext = ConverterHint.FILE_PATHSTRING, mandatory = false,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.AMPOOL_AUTHZ__SECURITY_PROPERTIES__HELP) String secPropertiesFile) {
    Result result = null;
    if (roleName == null || roleName.length() == 0) {
      return ResultBuilder.createShellClientErrorResult(MashCliStrings.CREATE_ROLE__ERROR
          + "Invalid value for " + MashCliStrings.CREATE_ROLE__ROLE_NAME);
    }
    if (secPropertiesFile == null || secPropertiesFile.trim().length() == 0) {
      secPropertiesFile = getGfsh().getEnvProperty(AMPOOL_AUTHZ__SECURITY_PROPERTIES);
    }
    if (secPropertiesFile == null || secPropertiesFile.trim().length() == 0) {
      secPropertiesFile = MashLauncher.getGlobalOption(AMPOOL_AUTHZ__SECURITY_PROPERTIES);
    }
    Result err = initSecurityManager(secPropertiesFile, MashCliStrings.CREATE_ROLE__ERROR);
    if (err != null)
      return err;
    try {
      securityManager.createRole(roleName);
    } catch (Exception e) {
      return ResultBuilder
          .createShellClientErrorResult(MashCliStrings.CREATE_ROLE__ERROR + e.getMessage());
    }
    securityManager.close();
    return ResultBuilder
        .createInfoResult(CliStrings.format(MashCliStrings.CREATE_ROLE__SUCCESS) + roleName);
  }

  @CliCommand(value = MashCliStrings.DROP_ROLE, help = MashCliStrings.DROP_ROLE__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {MashCliStrings.TOPIC_AMPOOL_AUTHZ})
  public Result deleteRole(
      @CliOption(key = MashCliStrings.DROP_ROLE__ROLE_NAME, mandatory = true,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.DROP_ROLE__ROLE_NAME__HELP) String roleName,
      @CliOption(key = AMPOOL_AUTHZ__SECURITY_PROPERTIES,
          optionContext = ConverterHint.FILE_PATHSTRING, mandatory = false,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.AMPOOL_AUTHZ__SECURITY_PROPERTIES__HELP) String secPropertiesFile) {
    Result result = null;
    if (roleName == null || roleName.length() == 0) {
      return ResultBuilder.createShellClientErrorResult(MashCliStrings.DROP_ROLE__ERROR
          + "Invalid value for " + MashCliStrings.DROP_ROLE__ROLE_NAME);
    }

    if (secPropertiesFile == null || secPropertiesFile.trim().length() == 0) {
      secPropertiesFile = getGfsh().getEnvProperty(AMPOOL_AUTHZ__SECURITY_PROPERTIES);
    }
    if (secPropertiesFile == null || secPropertiesFile.trim().length() == 0) {
      secPropertiesFile = MashLauncher.getGlobalOption(AMPOOL_AUTHZ__SECURITY_PROPERTIES);
    }

    Result err = initSecurityManager(secPropertiesFile, MashCliStrings.DROP_ROLE__ERROR);
    if (err != null)
      return err;
    try {
      securityManager.dropRole(roleName);
    } catch (Exception e) {
      return ResultBuilder
          .createShellClientErrorResult(MashCliStrings.DROP_ROLE__ERROR + e.getMessage());
    }
    securityManager.close();
    return ResultBuilder
        .createInfoResult(CliStrings.format(MashCliStrings.DROP_ROLE__SUCCESS) + roleName);
  }

  @CliCommand(value = MashCliStrings.GRANT_ROLE, help = MashCliStrings.GRANT_ROLE__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {MashCliStrings.TOPIC_AMPOOL_AUTHZ})
  public Result grantRole(
      @CliOption(key = MashCliStrings.GRANT_ROLE__ROLE_NAME, mandatory = true,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.GRANT_ROLE__ROLE_NAME__HELP) String roleName,
      @CliOption(key = MashCliStrings.GRANT_ROLE__GROUPS, mandatory = true,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.GRANT_ROLE__GROUPS__HELP) String groups,
      @CliOption(key = AMPOOL_AUTHZ__SECURITY_PROPERTIES,
          optionContext = ConverterHint.FILE_PATHSTRING, mandatory = false,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.AMPOOL_AUTHZ__SECURITY_PROPERTIES__HELP) String secPropertiesFile) {
    Result result = null;
    if (roleName == null || roleName.length() == 0) {
      return ResultBuilder.createShellClientErrorResult(MashCliStrings.GRANT_ROLE__ERROR
          + "Invalid value for " + MashCliStrings.GRANT_ROLE__ROLE_NAME);
    }

    if (groups == null || groups.length() == 0) {
      return ResultBuilder.createShellClientErrorResult(MashCliStrings.GRANT_ROLE__ERROR
          + "Invalid value for " + MashCliStrings.GRANT_ROLE__GROUPS);
    }

    Set<String> groupsSet = new HashSet(Arrays.asList(groups.split(GROUP_SEPARATOR)));

    if (secPropertiesFile == null || secPropertiesFile.trim().length() == 0) {
      secPropertiesFile = getGfsh().getEnvProperty(AMPOOL_AUTHZ__SECURITY_PROPERTIES);
    }
    if (secPropertiesFile == null || secPropertiesFile.trim().length() == 0) {
      secPropertiesFile = MashLauncher.getGlobalOption(AMPOOL_AUTHZ__SECURITY_PROPERTIES);
    }

    Result err = initSecurityManager(secPropertiesFile, MashCliStrings.GRANT_ROLE__ERROR);
    if (err != null)
      return err;
    try {
      securityManager.addRoleToGroups(roleName, groupsSet);
    } catch (Exception e) {
      return ResultBuilder
          .createShellClientErrorResult(MashCliStrings.GRANT_ROLE__ERROR + e.getMessage());
    }
    securityManager.close();
    return ResultBuilder
        .createInfoResult(CliStrings.format(MashCliStrings.GRANT_ROLE__SUCCESS) + roleName);
  }

  @CliCommand(value = MashCliStrings.REVOKE_ROLE, help = MashCliStrings.REVOKE_ROLE__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {MashCliStrings.TOPIC_AMPOOL_AUTHZ})
  public Result revokeRole(
      @CliOption(key = MashCliStrings.REVOKE_ROLE__ROLE_NAME, mandatory = true,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.REVOKE_ROLE__ROLE_NAME__HELP) String roleName,
      @CliOption(key = MashCliStrings.REVOKE_ROLE__GROUPS, mandatory = true,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.REVOKE_ROLE__GROUPS__HELP) String groups,
      @CliOption(key = AMPOOL_AUTHZ__SECURITY_PROPERTIES,
          optionContext = ConverterHint.FILE_PATHSTRING, mandatory = false,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.AMPOOL_AUTHZ__SECURITY_PROPERTIES__HELP) String secPropertiesFile) {
    Result result = null;
    if (roleName == null || roleName.length() == 0) {
      return ResultBuilder.createShellClientErrorResult(MashCliStrings.REVOKE_ROLE__ERROR
          + "Invalid value for " + MashCliStrings.REVOKE_ROLE__ROLE_NAME);
    }

    if (groups == null || groups.length() == 0) {
      return ResultBuilder.createShellClientErrorResult(MashCliStrings.REVOKE_ROLE__ERROR
          + "Invalid value for " + MashCliStrings.REVOKE_ROLE__GROUPS);
    }

    Set<String> groupsSet = new HashSet(Arrays.asList(groups.split(GROUP_SEPARATOR)));

    if (secPropertiesFile == null || secPropertiesFile.trim().length() == 0) {
      secPropertiesFile = getGfsh().getEnvProperty(AMPOOL_AUTHZ__SECURITY_PROPERTIES);
    }
    if (secPropertiesFile == null || secPropertiesFile.trim().length() == 0) {
      secPropertiesFile = MashLauncher.getGlobalOption(AMPOOL_AUTHZ__SECURITY_PROPERTIES);
    }
    Result err = initSecurityManager(secPropertiesFile, MashCliStrings.REVOKE_ROLE__ERROR);
    if (err != null)
      return err;
    try {
      securityManager.deleteRoleFromGroups(roleName, groupsSet);
    } catch (Exception e) {
      return ResultBuilder
          .createShellClientErrorResult(MashCliStrings.REVOKE_ROLE__ERROR + e.getMessage());
    }
    securityManager.close();
    return ResultBuilder
        .createInfoResult(CliStrings.format(MashCliStrings.REVOKE_ROLE__SUCCESS) + roleName);
  }

  @CliCommand(value = MashCliStrings.GRANT_PRIVILEGE, help = MashCliStrings.GRANT_PRIVILEGE__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {MashCliStrings.TOPIC_AMPOOL_AUTHZ})
  public Result grantPrivilege(
      @CliOption(key = MashCliStrings.GRANT_PRIVILEGE__ROLE_NAME, mandatory = true,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.GRANT_PRIVILEGE__ROLE_NAME__HELP) String roleName,
      @CliOption(key = MashCliStrings.GRANT_PRIVILEGE__PRIVILEGE, mandatory = true,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.GRANT_PRIVILEGE__PRIVILEGE__HELP) String privilege,
      @CliOption(key = AMPOOL_AUTHZ__SECURITY_PROPERTIES,
          optionContext = ConverterHint.FILE_PATHSTRING, mandatory = false,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.AMPOOL_AUTHZ__SECURITY_PROPERTIES__HELP) String secPropertiesFile) {
    Result result = null;
    if (roleName == null || roleName.length() == 0) {
      return ResultBuilder.createShellClientErrorResult(MashCliStrings.GRANT_PRIVILEGE__ERROR
          + "Invalid value for " + MashCliStrings.GRANT_PRIVILEGE__ROLE_NAME);
    }

    if (privilege == null || privilege.length() == 0) {
      return ResultBuilder.createShellClientErrorResult(MashCliStrings.GRANT_PRIVILEGE__ERROR
          + "Invalid value for " + MashCliStrings.GRANT_PRIVILEGE__PRIVILEGE);
    }

    ResourcePermission permission = null;
    try {
      permission = ConvertStringToResourcePermission(privilege);
    } catch (IllegalArgumentException iae) {
      return ResultBuilder
          .createShellClientErrorResult(MashCliStrings.GRANT_PRIVILEGE__ERROR + iae.getMessage());
    }

    if (secPropertiesFile == null || secPropertiesFile.trim().length() == 0) {
      secPropertiesFile = getGfsh().getEnvProperty(AMPOOL_AUTHZ__SECURITY_PROPERTIES);
    }
    if (secPropertiesFile == null || secPropertiesFile.trim().length() == 0) {
      secPropertiesFile = MashLauncher.getGlobalOption(AMPOOL_AUTHZ__SECURITY_PROPERTIES);
    }
    Result err = initSecurityManager(secPropertiesFile, MashCliStrings.GRANT_PRIVILEGE__ERROR);
    if (err != null)
      return err;
    try {
      securityManager.grantPrivileges(roleName, permission);
    } catch (Exception e) {
      return ResultBuilder
          .createShellClientErrorResult(MashCliStrings.GRANT_PRIVILEGE__ERROR + e.getMessage());
    }
    securityManager.close();
    return ResultBuilder
        .createInfoResult(CliStrings.format(MashCliStrings.GRANT_PRIVILEGE__SUCCESS) + roleName);
  }

  @CliCommand(value = MashCliStrings.REVOKE_PRIVILEGE, help = MashCliStrings.REVOKE_PRIVILEGE__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {MashCliStrings.TOPIC_AMPOOL_AUTHZ})
  public Result revokePrivilege(
      @CliOption(key = MashCliStrings.REVOKE_PRIVILEGE__ROLE_NAME, mandatory = true,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.REVOKE_PRIVILEGE__ROLE_NAME__HELP) String roleName,
      @CliOption(key = MashCliStrings.REVOKE_PRIVILEGE__PRIVILEGE, mandatory = true,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.REVOKE_PRIVILEGE__PRIVILEGE__HELP) String privilege,
      @CliOption(key = AMPOOL_AUTHZ__SECURITY_PROPERTIES,
          optionContext = ConverterHint.FILE_PATHSTRING, mandatory = false,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.AMPOOL_AUTHZ__SECURITY_PROPERTIES__HELP) String secPropertiesFile) {
    Result result = null;
    if (roleName == null || roleName.length() == 0) {
      return ResultBuilder.createShellClientErrorResult(MashCliStrings.REVOKE_PRIVILEGE__ERROR
          + "Invalid value for " + MashCliStrings.REVOKE_PRIVILEGE__ROLE_NAME);
    }

    if (privilege == null || privilege.length() == 0) {
      return ResultBuilder.createShellClientErrorResult(MashCliStrings.REVOKE_PRIVILEGE__ERROR
          + "Invalid value for " + MashCliStrings.REVOKE_PRIVILEGE__PRIVILEGE);
    }

    ResourcePermission permission = null;
    try {
      permission = ConvertStringToResourcePermission(privilege);
    } catch (IllegalArgumentException iae) {
      return ResultBuilder
          .createShellClientErrorResult(MashCliStrings.REVOKE_PRIVILEGE__ERROR + iae.getMessage());
    }

    if (secPropertiesFile == null || secPropertiesFile.trim().length() == 0) {
      secPropertiesFile = getGfsh().getEnvProperty(AMPOOL_AUTHZ__SECURITY_PROPERTIES);
    }
    if (secPropertiesFile == null || secPropertiesFile.trim().length() == 0) {
      secPropertiesFile = MashLauncher.getGlobalOption(AMPOOL_AUTHZ__SECURITY_PROPERTIES);
    }
    Result err = initSecurityManager(secPropertiesFile, MashCliStrings.REVOKE_PRIVILEGE__ERROR);
    if (err != null)
      return err;
    try {
      securityManager.revokePrivileges(roleName, permission);
    } catch (Exception e) {
      return ResultBuilder
          .createShellClientErrorResult(MashCliStrings.REVOKE_PRIVILEGE__ERROR + e.getMessage());
    }
    securityManager.close();
    return ResultBuilder
        .createInfoResult(CliStrings.format(MashCliStrings.REVOKE_PRIVILEGE__SUCCESS) + roleName);
  }

  @CliCommand(value = MashCliStrings.LIST_ROLES, help = MashCliStrings.LIST_ROLES__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {MashCliStrings.TOPIC_AMPOOL_AUTHZ})
  public Result listRoles(
      @CliOption(key = MashCliStrings.LIST_ROLES__GROUP, mandatory = false,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.LIST_ROLES__GROUP__HELP) String group,
      @CliOption(key = AMPOOL_AUTHZ__SECURITY_PROPERTIES,
          optionContext = ConverterHint.FILE_PATHSTRING, mandatory = false,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.AMPOOL_AUTHZ__SECURITY_PROPERTIES__HELP) String secPropertiesFile) {
    Result result = null;
    Set<String> roles;

    if (secPropertiesFile == null || secPropertiesFile.trim().length() == 0) {
      secPropertiesFile = getGfsh().getEnvProperty(AMPOOL_AUTHZ__SECURITY_PROPERTIES);
    }
    if (secPropertiesFile == null || secPropertiesFile.trim().length() == 0) {
      secPropertiesFile = MashLauncher.getGlobalOption(AMPOOL_AUTHZ__SECURITY_PROPERTIES);
    }
    Result err = initSecurityManager(secPropertiesFile, MashCliStrings.GRANT_ROLE__ERROR);
    if (err != null)
      return err;
    try {
      roles = securityManager.listRoles(group);
    } catch (Exception e) {
      return ResultBuilder
          .createShellClientErrorResult(MashCliStrings.LIST_ROLES__ERROR + e.getMessage());
    }
    TabularResultData trd = ResultBuilder.createTabularResultData();
    for (String role : roles) {
      trd.accumulate("Roles", role);
    }
    securityManager.close();
    return ResultBuilder.buildResult(trd);
  }

  @CliCommand(value = MashCliStrings.LIST_PRIVILEGES, help = MashCliStrings.LIST_PRIVILEGES__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {MashCliStrings.TOPIC_AMPOOL_AUTHZ})
  public Result listPrivileges(
      @CliOption(key = MashCliStrings.LIST_PRIVILEGES__ROLE_NAME, mandatory = true,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.LIST_PRIVILEGES__ROLE_NAME__HELP) String roleName,
      @CliOption(key = AMPOOL_AUTHZ__SECURITY_PROPERTIES,
          optionContext = ConverterHint.FILE_PATHSTRING, mandatory = false,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.AMPOOL_AUTHZ__SECURITY_PROPERTIES__HELP) String secPropertiesFile) {
    Result result = null;
    Set<ResourcePermission> privileges;

    if (secPropertiesFile == null || secPropertiesFile.trim().length() == 0) {
      secPropertiesFile = getGfsh().getEnvProperty(AMPOOL_AUTHZ__SECURITY_PROPERTIES);
    }
    if (secPropertiesFile == null || secPropertiesFile.trim().length() == 0) {
      secPropertiesFile = MashLauncher.getGlobalOption(AMPOOL_AUTHZ__SECURITY_PROPERTIES);
    }
    Result err = initSecurityManager(secPropertiesFile, MashCliStrings.LIST_PRIVILEGES__ERROR);
    if (err != null)
      return err;
    try {
      privileges = securityManager.listPrivileges(roleName);
    } catch (Exception e) {
      return ResultBuilder
          .createShellClientErrorResult(MashCliStrings.LIST_PRIVILEGES__ERROR + e.getMessage());
    }
    TabularResultData trd = ResultBuilder.createTabularResultData();
    for (ResourcePermission privilege : privileges) {
      trd.accumulate("Privileges", privilege);
    }
    securityManager.close();
    return ResultBuilder.buildResult(trd);
  }

  @CliCommand(value = MashCliStrings.INVALIDATE_PRIVILEGE_CACHE,
      help = MashCliStrings.INVALIDATE_PRIVILEGE_CACHE__HELP)
  @CliMetaData(relatedTopic = {MashCliStrings.TOPIC_AMPOOL_AUTHZ})
  public Result invalidateCachedPrivileges() {
    /* Invalidate the cache on locator */
    SecurityManager securityManager = SecurityService.getSecurityService().getSecurityManager();
    try {
      securityManager.invalidateAuthzCache();
    } catch (Exception e) {
      return ResultBuilder.createMTableErrorResult(e.getClass().toString());
    }

    Set<DistributedMember> targetMembers;
    /* Execute a function on all servers to invalidate the cache */
    try {
      targetMembers = CliUtil.findOneMatchingMember(null, null);
    } catch (CommandResultException ex) {
      logger.error("Error while finding members. Error: " + ex.getResult().nextLine());
      return ex.getResult();
    }
    ResultCollector<?, ?> rc =
        CliUtil.executeFunction(new InvalidateAuthzCacheFunction(), null, targetMembers);
    List results = (ArrayList) rc.getResult();
    for (Object result : results) {
      if (result instanceof Exception) {
        return ResultBuilder.createMTableErrorResult(
            MashCliStrings.INVALIDATE_PRIVILEGE_CACHE__ERROR + ((Exception) result).getMessage());
      }
    }
    return ResultBuilder.createInfoResult(MashCliStrings.INVALIDATE_PRIVILEGE_CACHE__SUCCESS);
  }

  private ResourcePermission ConvertStringToResourcePermission(String privilege)
      throws IllegalArgumentException {
    String[] parts = privilege.split(PRIVILEGE_PART_DELIM);
    if (parts[0].equalsIgnoreCase(ResourcePermission.Resource.CLUSTER.toString())) {
      if (parts.length < 2) {
        throw new IllegalArgumentException(
            "Privilege for cluster resource should be of format CLUSTER:<OPERATION> ");
      } else if (!parts[1].equalsIgnoreCase(ResourcePermission.Operation.MANAGE.toString())
          && !parts[1].equalsIgnoreCase(ResourcePermission.Operation.READ.toString())
          && !parts[1].equalsIgnoreCase(ResourcePermission.Operation.WRITE.toString())) {
        throw new IllegalArgumentException(
            "Privilege for cluster resource should be of format CLUSTER:<OPERATION> ");
      }
      return new ResourcePermission("CLUSTER", parts[1]);
    } else if (parts[0].equalsIgnoreCase(ResourcePermission.Resource.DATA.toString())) {
      if (parts.length < 2) {
        throw new IllegalArgumentException(
            "Privilege for cluster resource should be of format CLUSTER:<OPERATION> ");
      } else if (!parts[1].equalsIgnoreCase(ResourcePermission.Operation.MANAGE.toString())
          && !parts[1].equalsIgnoreCase(ResourcePermission.Operation.READ.toString())
          && !parts[1].equalsIgnoreCase(ResourcePermission.Operation.WRITE.toString())) {
        throw new IllegalArgumentException(
            "Privilege for cluster resource should be of format CLUSTER:<OPERATION> ");
      }
      switch (parts.length) {
        case 2:
          return new ResourcePermission("DATA", parts[1]);
        case 3:
          return new ResourcePermission("DATA", parts[1], parts[2]);
        case 4:
          return new ResourcePermission("DATA", parts[1], parts[2], parts[3]);
        default:
          throw new IllegalArgumentException("Invalid privilege string");
      }
    } else {
      throw new IllegalArgumentException("Privilege should start with \"CLUSTER\" or \"DATA\"");
    }
  }
}
