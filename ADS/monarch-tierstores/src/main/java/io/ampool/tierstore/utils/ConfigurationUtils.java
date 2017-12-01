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

package io.ampool.tierstore.utils;


import static io.ampool.security.SecurityConfigurationKeysPublic.ENABLE_KERBEROS_AUTHC;

import io.ampool.tierstore.config.CommonConfig;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.security.ResourceConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;

public class ConfigurationUtils {
  private static final Logger logger = LogService.getLogger();

  /**
   * Creates the hadoop configuration object from the properties specified for tierstore
   * 
   * @return configuration object
   */
  public static Configuration getConfiguration(final Properties props) throws IOException {
    Configuration conf = new Configuration();
    String hdfsSiteXMLPath = props.getProperty(CommonConfig.HDFS_SITE_XML_PATH);
    String hadoopSiteXMLPath = props.getProperty(CommonConfig.HADOOP_SITE_XML_PATH);
    if (hdfsSiteXMLPath != null) {
      conf.addResource(Paths.get(hdfsSiteXMLPath).toUri().toURL());
    }
    if (hadoopSiteXMLPath != null) {
      conf.addResource(Paths.get(hadoopSiteXMLPath).toUri().toURL());
    }

    props.entrySet().forEach((PROP) -> {
      conf.set(String.valueOf(PROP.getKey()), String.valueOf(PROP.getValue()));
    });

    // set secured properties
    String userName = props.getProperty(CommonConfig.USER_NAME);
    String keytabPath = props.getProperty(CommonConfig.KEYTAB_PATH);
    if (userName == null || keytabPath == null) {
      if (props.containsKey(ENABLE_KERBEROS_AUTHC)
          && Boolean.parseBoolean(props.getProperty(ENABLE_KERBEROS_AUTHC))) {
        userName = props.getProperty(ResourceConstants.USER_NAME);
        keytabPath = props.getProperty(ResourceConstants.PASSWORD);
      }
    }

    // use the username and keytab
    if (userName != null && keytabPath != null) {
      // set kerberos authentication
      conf.set("hadoop.security.authentication", "kerberos");
      UserGroupInformation.setConfiguration(conf);
      UserGroupInformation.loginUserFromKeytab(userName, keytabPath);
    }
    return conf;
  }

  public static Properties getSecurityProperties() {
    try {
      if (CacheFactory.getAnyInstance() != null
          && CacheFactory.getAnyInstance().getDistributedSystem() != null) {
        Properties securityProperties =
            CacheFactory.getAnyInstance().getDistributedSystem().getSecurityProperties();
        if (securityProperties.containsKey(ENABLE_KERBEROS_AUTHC)
            && Boolean.parseBoolean(securityProperties.getProperty(ENABLE_KERBEROS_AUTHC))) {
          return securityProperties;
        } else {
          return null;
        }
      } else {
        return null;
      }
    } catch (CacheClosedException c) {
      return null;
    }

  }
}
