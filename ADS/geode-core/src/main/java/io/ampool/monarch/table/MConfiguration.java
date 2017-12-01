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
package io.ampool.monarch.table;

import java.io.FileInputStream;
import java.nio.file.Path;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.conf.Configuration;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class MConfiguration extends Configuration {
  @SuppressWarnings("unused")
  // private static final Logger LOG =
  // LoggerFactory.getLogger(MConfiguration.class);

  private MConfiguration() {
    // Shouldn't be used other than in this class.
  }

  private MConfiguration(Configuration other) {
    super(other);
  }

  /**
   * Creates an instance of configuration.
   *
   * @return an instance of CConfiguration.
   */
  public static MConfiguration create() {
    // Create a new configuration instance, but do NOT initialize with
    // the Hadoop default properties.
    MConfiguration conf = new MConfiguration();
    conf.addResource("monarch-default.xml");
    conf.addResource("monarch-site.xml");
    return conf;
  }

  /**
   * Creates a new instance which clones all configurations from another {@link MConfiguration}.
   */
  public static MConfiguration copy(MConfiguration other) {
    return new MConfiguration(other);
  }

  /**
   * Add a configuration resource by path.
   * <p>
   * The properties of this resource will override properties of previously added resources, unless
   * they were marked <a href="#Final">final</a>.
   *
   * @param name resource to be added, the classpath is examined for a file with that name.
   */
  public void addResource(Path name) {
    try {
      addResource(new FileInputStream(name.toFile()));
    } catch (Exception e) {
      // LOG.error("Could not load resource {}", name.toAbsolutePath());
    }
  }

  /**
   * Returns Configuration's getBoolean with default as false.
   */
  public boolean getBoolean(String name) {
    return super.getBoolean(name, false);
  }
}
