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
package org.apache.geode.management.internal.cli.converters;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.ampool.monarch.cli.MashCliStrings;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

/**
 * A custom converter to display only Ampool tier-stores as hint (completion).
 *
 */
public class TierStorePathConverter implements Converter<String> {
  public static final String DEFAULT_APP_CONTEXT_PATH = "";
  /**
   * a placeholder to try and see if we can possibly auto-complete the tier-store
   * handler/reader/writer class-names from classpath; it will be expensive.. for now only lists the
   * available tier stores (with argument 0).
   */
  private static final Map<String, Integer> OPT_MAP =
      Collections.unmodifiableMap(new HashMap<String, Integer>(3) {
        {
          put(MashCliStrings.CREATE_TIERSTORE__HANDLER, 1);
          put(MashCliStrings.CREATE_TIERSTORE__READER, 2);
          put(MashCliStrings.CREATE_TIERSTORE__WRITER, 3);
        }
      });

  @Override
  public boolean supports(Class<?> type, String optionContext) {
    return String.class.equals(type) && ConverterHint.TIER_STORE_PATH.equals(optionContext);
  }

  @Override
  public String convertFromText(String value, Class<?> targetType, String optionContext) {
    return value;
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions, Class<?> targetType,
      String existingData, String optionContext, MethodTarget target) {
    if (String.class.equals(targetType) && ConverterHint.TIER_STORE_PATH.equals(optionContext)) {
      final Integer opt = OPT_MAP.get(target.getKey());
      Set<String> tierStorePaths = getAllTierStorePaths(opt == null ? 0 : opt);
      Gfsh gfsh = Gfsh.getCurrentInstance();
      String currentContextPath = "";
      if (gfsh != null) {
        currentContextPath = gfsh.getEnvProperty(Gfsh.ENV_APP_CONTEXT_PATH);
        if (currentContextPath != null && !DEFAULT_APP_CONTEXT_PATH.equals(currentContextPath)) {
          tierStorePaths.remove(currentContextPath);
          tierStorePaths.add(DEFAULT_APP_CONTEXT_PATH);
        }
      }

      for (String tierStorePath : tierStorePaths) {
        if (existingData != null) {
          if (tierStorePath.startsWith(existingData)) {
            completions.add(new Completion(tierStorePath));
          }
        } else {
          completions.add(new Completion(tierStorePath));
        }
      }
    }

    return !completions.isEmpty();
  }

  /**
   * Get the list of all Ampool tier store paths from the DS-bean.
   *
   * @param args arguments, if any
   * @return the set of all tier store paths
   */
  public Set<String> getAllTierStorePaths(final int args) {
    Set<String> tierPaths = Collections.emptySet();

    Gfsh gfsh = Gfsh.getCurrentInstance();
    if (gfsh != null && gfsh.isConnectedAndReady()) {
      final Map<String, String[]> tierMap =
          gfsh.getOperationInvoker().getDistributedSystemMXBean().listMemberTierStores(args);
      return tierMap.values().stream().flatMap(Stream::of).distinct().sorted()
          .collect(Collectors.toSet());
    }

    return tierPaths;
  }
}
