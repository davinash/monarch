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
 * A custom converter to display only Ampool tables as hint (completion). It hides all the default
 * Geode regions that are not Ampool tables (based on RegionDataOrder).
 *
 */
public class TablePathConverter implements Converter<String> {
  public static final String DEFAULT_APP_CONTEXT_PATH = "";
  /**
   * Initial map that tells which table-types are supported for the respective commands. In case the
   * command is not specified here (like delete) all Ampool tables will be displayed. For the
   * specified commands, only the respective table-types will be displayed as hint. The table types
   * are: - 1 : Ordered Versioned (MTable) - 2 : Unordered (MTable) - 3 : Immutable (FTable)
   *
   */
  private static final Map<String, String[]> CMD_MAP =
      Collections.unmodifiableMap(new HashMap<String, String[]>(4) {
        {
          put(MashCliStrings.MPUT,
              new String[] {"ROW_TUPLE_ORDERED_VERSIONED", "ROW_TUPLE_UNORDERED"});
          put(MashCliStrings.MGET,
              new String[] {"ROW_TUPLE_ORDERED_VERSIONED", "ROW_TUPLE_UNORDERED"});
          put(MashCliStrings.MDELETE,
              new String[] {"ROW_TUPLE_ORDERED_VERSIONED", "ROW_TUPLE_UNORDERED"});
          put(MashCliStrings.MSCAN, new String[] {});
          put(MashCliStrings.APPEND, new String[] {"IMMUTABLE"});
        }
      });

  @Override
  public boolean supports(Class<?> type, String optionContext) {
    return String.class.equals(type) && ConverterHint.TABLE_PATH.equals(optionContext);
  }

  @Override
  public String convertFromText(String value, Class<?> targetType, String optionContext) {
    return value;
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions, Class<?> targetType,
      String existingData, String optionContext, MethodTarget target) {
    if (String.class.equals(targetType) && ConverterHint.TABLE_PATH.equals(optionContext)) {
      final String[] orders = CMD_MAP.get(target.getKey());
      Set<String> tablePathSet = getAllTablePaths(orders);
      Gfsh gfsh = Gfsh.getCurrentInstance();
      String currentContextPath = "";
      if (gfsh != null) {
        currentContextPath = gfsh.getEnvProperty(Gfsh.ENV_APP_CONTEXT_PATH);
        if (currentContextPath != null && !DEFAULT_APP_CONTEXT_PATH.equals(currentContextPath)) {
          tablePathSet.remove(currentContextPath);
          tablePathSet.add(DEFAULT_APP_CONTEXT_PATH);
        }
      }

      for (String tablePath : tablePathSet) {
        if (existingData != null) {
          if (tablePath.startsWith(existingData)) {
            completions.add(new Completion(tablePath));
          }
        } else {
          completions.add(new Completion(tablePath));
        }
      }
    }

    return !completions.isEmpty();
  }

  /**
   * Get the list of all Ampool tables from the DS-bean.
   *
   * @param orders the required region data orders
   * @return the set of all table paths
   */
  public Set<String> getAllTablePaths(final String... orders) {
    Set<String> tablePathSet = Collections.emptySet();

    Gfsh gfsh = Gfsh.getCurrentInstance();
    if (gfsh != null && gfsh.isConnectedAndReady()) {
      String[] tablePaths =
          gfsh.getOperationInvoker().getDistributedSystemMXBean().listAllTablePaths(orders);
      return Stream.of(tablePaths).sorted().collect(Collectors.toSet());
    }

    return tablePathSet;
  }
}
