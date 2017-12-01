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

package org.apache.geode.management.internal.cli.commands;

import io.ampool.monarch.cli.MashCliStrings;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MDiskWritePolicy;
import io.ampool.monarch.table.MEvictionPolicy;
import io.ampool.monarch.table.MExpirationAction;
import io.ampool.monarch.table.MExpirationAttributes;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.TierStoreConfiguration;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.internal.TableType;
import io.ampool.monarch.types.TypeUtils;
import io.ampool.tierstore.TierStoreFactory;
import io.ampool.tierstore.internal.DefaultStore;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.CreateMTableFunction;
import org.apache.geode.internal.cache.CreateTierStoreFunction;
import org.apache.geode.internal.cache.DeleteMTableFunction;
import org.apache.geode.internal.cache.DescMTableFunction;
import org.apache.geode.internal.cache.DescribeTierStoreFunction;
import org.apache.geode.internal.cache.DestroyTierStoreFunction;
import org.apache.geode.internal.cache.GetMTablesFunction;
import org.apache.geode.internal.cache.GetTierStoresFunction;
import org.apache.geode.internal.cache.MGetMetadataFunction;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.domain.DiskStoreDetails;
import org.apache.geode.management.internal.cli.domain.FixedPartitionAttributesInfo;
import org.apache.geode.management.internal.cli.domain.RegionDescription;
import org.apache.geode.management.internal.cli.domain.RegionDescriptionPerMember;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.GetMemberInformationFunction;
import org.apache.geode.management.internal.cli.functions.GetRegionDescriptionFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.remote.CommandExecutionContext;
import org.apache.geode.management.internal.cli.result.CommandResultException;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.FileResult;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.ResultDataException;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.cli.util.RegionAttributesNames;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;
import org.apache.logging.log4j.Logger;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * The MtableCommands class encapsulates all MTable commands in Gfsh.
 * </p>
 * 
 * @see org.apache.geode.management.internal.cli.commands.AbstractCommandsSupport
 * @since 7.0
 */
@SuppressWarnings("unused")
public class MTableCommands extends AbstractCommandsSupport {


  private LogWrapper logWrapper = LogWrapper.getInstance();
  private static final Logger logger = LogService.getLogger();
  private static final GetMTablesFunction getMTablesFunction = new GetMTablesFunction();
  private static final GetRegionDescriptionFunction getRegionDescription =
      new GetRegionDescriptionFunction();
  private static final GetMemberInformationFunction getMemberInformation =
      new GetMemberInformationFunction();
  private static final GetTierStoresFunction getTierStoresFunction = new GetTierStoresFunction();
  private static final DescribeTierStoreFunction describeTierStoreFunction =
      new DescribeTierStoreFunction();


  @CliAvailabilityIndicator({MashCliStrings.CREATE_MTABLE, MashCliStrings.DELETE_MTABLE,
      MashCliStrings.DESCRIBE_MTABLE, MashCliStrings.LIST_MTABLE, MashCliStrings.CREATE_TIERSTORE,
      MashCliStrings.LIST_TIER_STORES, MashCliStrings.DESCRIBE_TIER_STORE,
      MashCliStrings.DESTROY_TIER_STORE})
  public boolean isMtableCommandsAvailable() {

    return (!CliUtil.isGfshVM() || (getGfsh() != null && getGfsh().isConnectedAndReady()));
  }

  @CliCommand(value = MashCliStrings.CREATE_MTABLE, help = MashCliStrings.CREATE_MTABLE__HELP)
  @CliMetaData(shellOnly = false,
      relatedTopic = {MashCliStrings.TOPIC_MTABLE, MashCliStrings.TOPIC_FTABLE})
  @ResourceOperation(resource = ResourcePermission.Resource.DATA,
      operation = ResourcePermission.Operation.MANAGE)
  public Result createMtable(
      @CliOption(key = MashCliStrings.CREATE_MTABLE__NAME, mandatory = true,
          optionContext = ConverterHint.MEMBERGROUP,
          help = MashCliStrings.CREATE_MTABLE__NAME__HELP) String name,
      @CliOption(key = MashCliStrings.CREATE_MTABLE__TYPE, mandatory = true,
          help = MashCliStrings.CREATE_MTABLE__TYPE__HELP) TableType tableType,
      @CliOption(key = MashCliStrings.CREATE_MTABLE_COLUMNS, mandatory = false,
          help = MashCliStrings.CREATE_MTABLE_COLUMNS__HELP) @CliMetaData(
              valueSeparator = ",") String[] groups,
      @CliOption(key = MashCliStrings.CREATE_MTABLE_SCHEMA,
          help = MashCliStrings.CREATE_MTABLE_SCHEMA__HELP) String schemaJson,
      @CliOption(key = MashCliStrings.CREATE_MTABLE__REDUNDANT_COPIES,
          unspecifiedDefaultValue = "0",
          help = MashCliStrings.CREATE_MTABLE__REDUNDANT_COPIES__HELP) Integer nRedundantCopies,
      @CliOption(key = MashCliStrings.CREATE_MTABLE__RECOVERY_DELAY,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.CREATE_MTABLE__RECOVERY_DELAY__HELP) Long recoveryDelay,
      @CliOption(key = MashCliStrings.CREATE_MTABLE__STARTUP_RECOVERY_DELAY,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.CREATE_MTABLE__STARTUP_RECOVERY_DELAY__HELP) Long startupRecoveryDelay,
      @CliOption(key = MashCliStrings.CREATE_MTABLE__MAX_VERSIONS,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.CREATE_MTABLE__MAX_VERSIONS__HELP) Integer nMaxVersions,
      @CliOption(key = MashCliStrings.CREATE_MTABLE__DISK_PERSISTENCE,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.CREATE_MTABLE__DISK_PERSISTENCE__HELP) Boolean enableDiskPersistence,
      @CliOption(key = MashCliStrings.CREATE_MTABLE__DISK_WRITE_POLICY,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.CREATE_MTABLE__DISK_WRITE_POLICY__HELP) MDiskWritePolicy mDiskWritePolicy,
      @CliOption(key = MashCliStrings.CREATE_MTABLE__DISKSTORE,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.CREATE_MTABLE__DISKSTORE__HELP) String diskStoreName,
      @CliOption(key = MashCliStrings.CREATE_MTABLE__SPLITS, unspecifiedDefaultValue = "113",
          help = MashCliStrings.CREATE_MTABLE__SPLITS__HELP) Integer nSplits,
      @CliOption(key = MashCliStrings.CREATE_MTABLE__EVICTION_POLICY,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.CREATE_MTABLE__EVICTION_POLICY__HELP) MEvictionPolicy evictionPolicy,
      @CliOption(key = MashCliStrings.CREATE_MTABLE__EXPIRATION_TIMEOUT,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.CREATE_MTABLE__EXPIRATION_TIMEOUT__HELP) Integer expirationTimeout,
      @CliOption(key = MashCliStrings.CREATE_MTABLE__EXPIRATION_ACTION,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.CREATE_MTABLE__EXPIRATION_ACTION__HELP) MExpirationAction expirationAction,
      @CliOption(key = MashCliStrings.CREATE_MTABLE__BLOCK_SIZE,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.CREATE_MTABLE__BLOCK_SIZE__HELP) Integer blockSize,
      @CliOption(key = MashCliStrings.CREATE_TABLE__LOCAL_MAX_MEMORY,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.CREATE_TABLE__LOCAL_MAX_MEMORY__HELP) Integer localMaxMemory,
      @CliOption(key = MashCliStrings.CREATE_TABLE__LOCAL_MAX_MEMORY_PCT,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.CREATE_TABLE__LOCAL_MAX_MEMORY_PCT__HELP) Integer localMaxMemoryPct,
      @CliMetaData(valueSeparator = ",") @CliOption(key = MashCliStrings.CREATE_MTABLE__TIERSTORES,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          optionContext = ConverterHint.TIER_STORE_PATH,
          help = MashCliStrings.CREATE_MTABLE__TIERSTORES__HELP) String tierStoreNames,
      @CliOption(key = MashCliStrings.TIER1_TIME_TO_EXPIRE,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.TIER1_TIME_TO_EXPIRE__HELP) String tier1TimeToExpire,
      @CliOption(key = MashCliStrings.TIER2_TIME_TO_EXPIRE,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.TIER2_TIME_TO_EXPIRE__HELP) String tier2TimeToExpire,
      @CliOption(key = MashCliStrings.TIER1_TIME_PARTITION_INTERVAL,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.TIER1_TIME_PARTITION_INTERVAL__HELP) String tier1TimePartitionInterval,
      @CliOption(key = MashCliStrings.TIER2_TIME_PARTITION_INTERVAL,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.TIER2_TIME_PARTITION_INTERVAL__HELP) String tier2TimePartitionInterval,
      @CliOption(key = MashCliStrings.CREATE_MTABLE__OBSERVER_COPROCESSORS,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.OBSERVER_COPROCESSORS__HELP) String coProcessors,
      @CliOption(key = MashCliStrings.CREATE_MTABLE__FTABLE_PARTITION_COLUMN_NAME,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.CREATE_MTABLE__FTABLE_PARTITION_COLUMN_NAME__HELP) String partitionColumnName,
      @CliOption(key = MashCliStrings.CREATE_MTABLE__CACHE_LOADER_CLASS,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.CREATE_MTABLE__CACHE_LOADER_CLASS_HELP) String cacheLoaderClass,
      @CliOption(key = MashCliStrings.CREATE_MTABLE__FTABLE_BLOCK_FORMAT,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.CREATE_MTABLE__FTABLE_BLOCK_FORMAT__HELP) FTableDescriptor.BlockFormat blockFormat) {
    try {
      boolean isImmutableTable = false;
      // validate MTable name
      if (StringUtils.isBlank(name)) {
        return ResultBuilder.createInfoResult("TableName cannot be blank.");
      }

      try {
        MTableUtils.isTableNameLegal(name);
      } catch (IllegalArgumentException e) {
        return ResultBuilder
            .createInfoResult("Invalid Table name : " + name + ". " + e.getMessage());
      }

      if (nRedundantCopies == null) {
        // initialize to defaults
        nRedundantCopies = 0;
      } else if (nRedundantCopies > 3) {
        return ResultBuilder.createGemFireErrorResult(
            CliStrings.format(MashCliStrings.CREATE_MTABLE__ERROR_REDUNDANT_COPIES));
      }
      if (nSplits == null) {
        // initialize to defaults
        nSplits = MTableDescriptor.DEFAULT_TOTAL_NUM_SPLITS;
      } else if (nSplits.intValue() > 512) {
        return ResultBuilder.createGemFireErrorResult(
            CliStrings.format(MashCliStrings.CREATE_MTABLE__SPLITS_ERROR));
      }

      // handle non applicable options
      switch (tableType) {
        case IMMUTABLE:
          isImmutableTable = true;
          if (nMaxVersions != null) {
            return ResultBuilder.createUserErrorResult(
                CliStrings.format(MashCliStrings.CREATE_MTABLE__IMMUTABLE_TYPE_NOT,
                    MashCliStrings.CREATE_MTABLE__MAX_VERSIONS));
          }

          if (evictionPolicy != null) {
            return ResultBuilder.createUserErrorResult(
                CliStrings.format(MashCliStrings.CREATE_MTABLE__IMMUTABLE_TYPE_NOT,
                    MashCliStrings.CREATE_MTABLE__EVICTION_POLICY));
          }

          if (expirationAction != null) {
            return ResultBuilder.createUserErrorResult(
                CliStrings.format(MashCliStrings.CREATE_MTABLE__IMMUTABLE_TYPE_NOT,
                    MashCliStrings.CREATE_MTABLE__EXPIRATION_ACTION));
          }

          if (expirationTimeout != null) {
            return ResultBuilder.createUserErrorResult(
                CliStrings.format(MashCliStrings.CREATE_MTABLE__IMMUTABLE_TYPE_NOT,
                    MashCliStrings.CREATE_MTABLE__EXPIRATION_TIMEOUT));
          }

          if (mDiskWritePolicy != null) {
            return ResultBuilder.createUserErrorResult(
                CliStrings.format(MashCliStrings.CREATE_MTABLE__IMMUTABLE_TYPE_NOT,
                    MashCliStrings.CREATE_MTABLE__DISK_WRITE_POLICY));
          }

          if (enableDiskPersistence != null) {
            return ResultBuilder.createUserErrorResult(
                CliStrings.format(MashCliStrings.CREATE_MTABLE__IMMUTABLE_TYPE_NOT,
                    MashCliStrings.CREATE_MTABLE__DISK_PERSISTENCE));
          }

          if (coProcessors != null) {
            return ResultBuilder.createUserErrorResult(
                CliStrings.format(MashCliStrings.CREATE_MTABLE__IMMUTABLE_TYPE_NOT,
                    MashCliStrings.CREATE_MTABLE__OBSERVER_COPROCESSORS));
          }

          if (cacheLoaderClass != null) {
            return ResultBuilder.createUserErrorResult(
                CliStrings.format(MashCliStrings.CREATE_MTABLE__IMMUTABLE_TYPE_NOT,
                    MashCliStrings.CREATE_MTABLE__CACHE_LOADER_CLASS));

          }

          if (blockSize == null) {
            blockSize = 1000;
          }
          if (blockFormat == null) {
            blockFormat = FTableDescriptor.DEFAULT_BLOCK_FORMAT;
          }
          break;
        case ORDERED_VERSIONED:
          if (nMaxVersions == null) {
            nMaxVersions = 1;
          }
          if (blockSize != null) {
            return ResultBuilder.createUserErrorResult(
                CliStrings.format(MashCliStrings.CREATE_MTABLE__IMMUTABLE_TYPE_FOR,
                    MashCliStrings.CREATE_MTABLE__BLOCK_SIZE));
          }
          if (blockFormat != null) {
            return ResultBuilder.createUserErrorResult(
                CliStrings.format(MashCliStrings.CREATE_MTABLE__IMMUTABLE_TYPE_FOR,
                    MashCliStrings.CREATE_MTABLE__FTABLE_BLOCK_FORMAT));
          }
          if (enableDiskPersistence == null) {
            enableDiskPersistence = false;
          }
          if (tierStoreNames != null) {
            return ResultBuilder.createUserErrorResult(
                CliStrings.format(MashCliStrings.CREATE_MTABLE__IMMUTABLE_TYPE_FOR,
                    MashCliStrings.CREATE_MTABLE__TIERSTORES));
          }
          if (tier1TimeToExpire != null) {
            return ResultBuilder.createUserErrorResult(
                CliStrings.format(MashCliStrings.CREATE_MTABLE__IMMUTABLE_TYPE_FOR,
                    MashCliStrings.TIER1_TIME_TO_EXPIRE));
          }
          if (tier2TimeToExpire != null) {
            return ResultBuilder.createUserErrorResult(
                CliStrings.format(MashCliStrings.CREATE_MTABLE__IMMUTABLE_TYPE_FOR,
                    MashCliStrings.TIER2_TIME_TO_EXPIRE));
          }
          if (tier1TimePartitionInterval != null) {
            return ResultBuilder.createUserErrorResult(
                CliStrings.format(MashCliStrings.CREATE_MTABLE__IMMUTABLE_TYPE_FOR,
                    MashCliStrings.TIER1_TIME_PARTITION_INTERVAL));
          }
          if (tier2TimePartitionInterval != null) {
            return ResultBuilder.createUserErrorResult(
                CliStrings.format(MashCliStrings.CREATE_MTABLE__IMMUTABLE_TYPE_FOR,
                    MashCliStrings.TIER2_TIME_PARTITION_INTERVAL));
          }
          break;
        case UNORDERED:
          if (nMaxVersions == null) {
            nMaxVersions = 1;
          }
          if (blockSize != null) {
            return ResultBuilder.createUserErrorResult(
                CliStrings.format(MashCliStrings.CREATE_MTABLE__IMMUTABLE_TYPE_FOR,
                    MashCliStrings.CREATE_MTABLE__BLOCK_SIZE));
          }
          if (blockFormat != null) {
            return ResultBuilder.createUserErrorResult(
                CliStrings.format(MashCliStrings.CREATE_MTABLE__IMMUTABLE_TYPE_FOR,
                    MashCliStrings.CREATE_MTABLE__FTABLE_BLOCK_FORMAT));
          }
          if (enableDiskPersistence == null) {
            enableDiskPersistence = false;
          }
          if (tierStoreNames != null) {
            return ResultBuilder.createUserErrorResult(
                CliStrings.format(MashCliStrings.CREATE_MTABLE__IMMUTABLE_TYPE_FOR,
                    MashCliStrings.CREATE_MTABLE__TIERSTORES));
          }
          if (tier1TimeToExpire != null) {
            return ResultBuilder.createUserErrorResult(
                CliStrings.format(MashCliStrings.CREATE_MTABLE__IMMUTABLE_TYPE_FOR,
                    MashCliStrings.TIER1_TIME_TO_EXPIRE));
          }
          if (tier2TimeToExpire != null) {
            return ResultBuilder.createUserErrorResult(
                CliStrings.format(MashCliStrings.CREATE_MTABLE__IMMUTABLE_TYPE_FOR,
                    MashCliStrings.TIER2_TIME_TO_EXPIRE));
          }
          if (tier1TimePartitionInterval != null) {
            return ResultBuilder.createUserErrorResult(
                CliStrings.format(MashCliStrings.CREATE_MTABLE__IMMUTABLE_TYPE_FOR,
                    MashCliStrings.TIER1_TIME_PARTITION_INTERVAL));
          }
          if (tier2TimePartitionInterval != null) {
            return ResultBuilder.createUserErrorResult(
                CliStrings.format(MashCliStrings.CREATE_MTABLE__IMMUTABLE_TYPE_FOR,
                    MashCliStrings.TIER2_TIME_PARTITION_INTERVAL));
          }
          break;
      }

      /**
       * Adding option --schema-json that creation of table with column types. For now keeping both
       * options --columns for all binary types and --schema-json. If both are specified together
       * give an error.
       */
      if (groups != null && groups.length > 0 && !StringUtils.isBlank(schemaJson)) {
        return ResultBuilder.createUserErrorResult(MashCliStrings.CREATE_MTABLE___HELP_1);
      }

      TableDescriptor tableDescriptor = null;
      // based on table type create table descriptor
      if (tableType != TableType.IMMUTABLE) {
        if (tableType == TableType.ORDERED_VERSIONED) {
          tableDescriptor = new MTableDescriptor(MTableType.ORDERED_VERSIONED);
        } else {
          tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
        }
      } else {
        // create ftable descriptor
        tableDescriptor = new FTableDescriptor();

        // Set block size for ftable
        ((FTableDescriptor) tableDescriptor).setBlockSize(blockSize);
        ((FTableDescriptor) tableDescriptor).setBlockFormat(blockFormat);
      }
      tableDescriptor.setRedundantCopies(nRedundantCopies);

      if (groups != null && groups.length > 0) {
        List<String> columnNames = Arrays.asList(groups);
        logger.info("Creating table using columns= {} with `BINARY` column-type.", columnNames);
        for (int i = 0; i < columnNames.size(); i++) {
          tableDescriptor.addColumn(Bytes.toBytes(columnNames.get(i)));
        }
      } else if (!StringUtils.isBlank(schemaJson)) {
        final String error = TypeUtils.addColumnsFromJson(schemaJson, tableDescriptor);
        if (error != null) {
          logger.error("Failed to create table= {}; Error= {}", name, error);
          return ResultBuilder.createGemFireErrorResult(error);
        }
        logger.info("Successfully created table= {} with schema= {}", name, schemaJson);
      } else {
        return ResultBuilder.createUserErrorResult(MashCliStrings.CREATE_MTABLE___HELP);
      }
      Set<DistributedMember> targetMembers;
      String[] groups1 = null;
      try {
        targetMembers = CliUtil.findOneMatchingMember(groups1, null);
      } catch (CommandResultException ex) {
        logger.error("Error while finding members. Error: " + ex.getResult().nextLine());
        return ex.getResult();
      }

      if (tableType != TableType.IMMUTABLE) {
        ((MTableDescriptor) tableDescriptor).setMaxVersions(nMaxVersions);
      }

      if (nSplits == null) {
        tableDescriptor.setTotalNumOfSplits(MTableDescriptor.DEFAULT_TOTAL_NUM_SPLITS);
      } else if (nSplits != null && ((nSplits.intValue() > 0) && (nSplits.intValue() <= 512))) {
        tableDescriptor.setTotalNumOfSplits(nSplits);
      } else {
        return ResultBuilder.createGemFireErrorResult(
            CliStrings.format(MashCliStrings.CREATE_MTABLE__SPLITS_ERROR));
      }

      if (isImmutableTable) {
        if (diskStoreName != null) {
          ((FTableDescriptor) tableDescriptor).setRecoveryDiskStore(diskStoreName);
        }
      } else if (enableDiskPersistence) {
        // if not immutable i.e ftable then setas for ftable defaults are set through
        // default ftableDescriptor constructor
        if (mDiskWritePolicy == null) {
          mDiskWritePolicy = MDiskWritePolicy.ASYNCHRONOUS;
        }
        tableDescriptor.enableDiskPersistence(mDiskWritePolicy);
        if (diskStoreName != null) {
          if (!diskStoreExists(CacheFactory.getAnyInstance(), diskStoreName)) {
            throw new IllegalArgumentException(CliStrings.format(
                CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_DISKSTORE_UNKNOWN_DISKSTORE_0,
                new Object[] {diskStoreName}));
          }
          tableDescriptor.setDiskStore(diskStoreName);
        }
      }

      if (evictionPolicy != null) {
        tableDescriptor.setEvictionPolicy(evictionPolicy);
      }

      if (tableType == TableType.IMMUTABLE
          && ((FTableDescriptor) tableDescriptor)
              .getEvictionPolicy() == MEvictionPolicy.OVERFLOW_TO_TIER
          && isImmutableTable && tierStoreNames != null) {
        // find stores and add them
        // TODO revisit
        LinkedHashMap<String, TierStoreConfiguration> tiers =
            new LinkedHashMap<String, TierStoreConfiguration>();
        String[] tierNames = tierStoreNames.split(Pattern.quote(","));
        int length = tierNames.length;
        if (length > 2) {
          return ResultBuilder.createUserErrorResult(
              CliStrings.format(MashCliStrings.NUMBER_OF_TIERSTORES_LIMIT, length));
        }

        if (tierNames[0] != null && !tierNames[0].isEmpty()) {
          Properties properties1 = new Properties();
          if (tier1TimeToExpire != null) {
            properties1.setProperty(TierStoreConfiguration.TIER_TIME_TO_EXPIRE, tier1TimeToExpire);
          }
          if (tier1TimePartitionInterval != null) {
            properties1.setProperty(TierStoreConfiguration.TIER_PARTITION_INTERVAL,
                tier1TimePartitionInterval);
          }
          TierStoreConfiguration tierStoreConfiguration = new TierStoreConfiguration();
          tierStoreConfiguration.setTierProperties(properties1);
          tiers.put(tierNames[0], tierStoreConfiguration);
        }

        if (length > 1) {
          if (tierNames[1] != null && !tierNames[1].isEmpty()) {
            Properties properties2 = new Properties();
            if (tier2TimeToExpire != null) {
              properties2.setProperty(TierStoreConfiguration.TIER_TIME_TO_EXPIRE,
                  tier2TimeToExpire);
            }
            if (tier2TimePartitionInterval != null) {
              properties2.setProperty(TierStoreConfiguration.TIER_PARTITION_INTERVAL,
                  tier2TimePartitionInterval);
            }
            TierStoreConfiguration tierStoreConfiguration = new TierStoreConfiguration();
            tierStoreConfiguration.setTierProperties(properties2);
            tiers.put(tierNames[1], tierStoreConfiguration);

          }
        }
        ((FTableDescriptor) tableDescriptor).addTierStores(tiers);
      }

      if (expirationTimeout != null && !isImmutableTable) {
        if (expirationAction != null) {
          ((MTableDescriptor) tableDescriptor).setExpirationAttributes(
              new MExpirationAttributes(expirationTimeout, expirationAction));
        } else {
          ((MTableDescriptor) tableDescriptor).setExpirationAttributes(
              new MExpirationAttributes(expirationTimeout, MExpirationAction.DESTROY));
        }
      }

      if (localMaxMemory != null && localMaxMemoryPct != null) {
        return ResultBuilder
            .createUserErrorResult("Only one of " + MashCliStrings.CREATE_TABLE__LOCAL_MAX_MEMORY
                + " and " + MashCliStrings.CREATE_TABLE__LOCAL_MAX_MEMORY_PCT + " can be used");
      }

      if (localMaxMemory != null) {
        if (localMaxMemory < 0) {
          return ResultBuilder.createUserErrorResult(CliStrings
              .format(MashCliStrings.CREATE_TABLE__LOCAL_MAX_MEMORY__INVALID, localMaxMemory));
        }
        tableDescriptor.setLocalMaxMemory(localMaxMemory);
      }

      if (localMaxMemoryPct != null) {
        if (localMaxMemoryPct <= 0 || localMaxMemoryPct > 90) {
          return ResultBuilder.createUserErrorResult(CliStrings.format(
              MashCliStrings.CREATE_TABLE__LOCAL_MAX_MEMORY_PCT__INVALID, localMaxMemoryPct));
        }
        tableDescriptor.setLocalMaxMemoryPct(localMaxMemoryPct);
      }

      if (recoveryDelay != null) {
        tableDescriptor.setRecoveryDelay(recoveryDelay);
      }

      if (startupRecoveryDelay != null) {
        tableDescriptor.setStartupRecoveryDelay(startupRecoveryDelay);
      }

      if (tableType != TableType.IMMUTABLE && coProcessors != null) {
        String[] coProc = coProcessors.split(",");
        for (int i = 0; i < coProc.length; i++) {
          ((MTableDescriptor) tableDescriptor).addCoprocessor(coProc[i]);
        }
      }

      if (tableType == TableType.IMMUTABLE && partitionColumnName != null) {
        ((FTableDescriptor) tableDescriptor).setPartitioningColumn(partitionColumnName);
      }

      if (tableType != TableType.IMMUTABLE && cacheLoaderClass != null) {
        ((MTableDescriptor) tableDescriptor).setCacheLoaderClassName(cacheLoaderClass);
      }

      TabularResultData tabularData = ResultBuilder.createTabularResultData();
      boolean accumulatedData = false;
      ResultCollector<?, ?> rc = CliUtil.executeFunction(new CreateMTableFunction(),
          new Object[] {name, tableDescriptor}, targetMembers);

      List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());
      XmlEntity xmlEntity = null;
      for (CliFunctionResult result : results) {
        if (!result.isSuccessful()) {
          return ResultBuilder
              .createUserErrorResult("Table Creation Failed " + result.getMessage());
        }
        if (result.getThrowable() != null) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("Result", "ERROR: " + result.getThrowable().getClass().getName()
              + ": " + result.getThrowable().getMessage());
          accumulatedData = true;
          tabularData.setStatus(Result.Status.ERROR);
        } else if (result.getMessage() != null) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("Result", result.getMessage());
          accumulatedData = true;

          if (xmlEntity == null) {
            xmlEntity = result.getXmlEntity();
          }
        }
      }
      if (!accumulatedData) {

        return ResultBuilder.createUserErrorResult(
            CliStrings.format(MashCliStrings.CREATE_MTABLE__ALREADY_EXISTS_ERROR, name));
      }

    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (NullPointerException e) {
      return ResultBuilder.createGemFireErrorResult(
          CliStrings.format(MashCliStrings.CREATE_MTABLE__DISKSTORE__ERROR));

    } catch (Exception e) {
      logWrapper.fine("Exception D " + e.getMessage());
      return ResultBuilder.createGemFireErrorResult(
          CliStrings.format(MashCliStrings.CREATE_MTABLE__ERROR_WHILE_CREATING_REASON_0,
              new Object[] {e.getMessage()}));
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult(
          CliStrings.format(MashCliStrings.CREATE_MTABLE__ERROR_WHILE_CREATING_REASON_0,
              new Object[] {th.getMessage()}));
    }

    Result infoResult = ResultBuilder
        .createInfoResult(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS) + name);

    return infoResult;
  }

  private boolean diskStoreExists(Cache cache, String diskStoreName) {
    ManagementService managementService = ManagementService.getExistingManagementService(cache);
    DistributedSystemMXBean dsMXBean = managementService.getDistributedSystemMXBean();
    Map<String, String[]> diskstore = dsMXBean.listMemberDiskstore();

    Set<Entry<String, String[]>> entrySet = diskstore.entrySet();

    for (Entry<String, String[]> entry : entrySet) {
      String[] value = entry.getValue();
      if (CliUtil.contains(value, diskStoreName)) {
        return true;
      }
    }

    return false;
  }

  // TODO Implement - get list of stores available and check if name exists
  private boolean tierStoreExists(Cache cache, String tierStoreNames) {

    List<String> diskStore = MTableUtils.getTierStores();

    List<String> strings = Arrays.asList(tierStoreNames.split(Pattern.quote(",")));
    return diskStore.containsAll(strings);
  }

  protected boolean ifStorePresent(final List<DiskStoreDetails> diskStoreList, String storeName)
      throws ResultDataException {
    if (!diskStoreList.isEmpty()) {
      final TabularResultData diskStoreData = ResultBuilder.createTabularResultData();

      for (final DiskStoreDetails diskStoreDetails : diskStoreList) {
        if (diskStoreDetails.getName().equals(storeName)) {
          return true;
        }
      }

      return false;
    } else {
      return false;
    }
  }


  @CliCommand(value = MashCliStrings.DELETE_MTABLE, help = MashCliStrings.DELETE_MTABLE__HELP)
  @CliMetaData(shellOnly = false,
      relatedTopic = {MashCliStrings.TOPIC_MTABLE, MashCliStrings.TOPIC_FTABLE})
  @ResourceOperation(resource = ResourcePermission.Resource.DATA,
      operation = ResourcePermission.Operation.MANAGE)
  public Result deleteMtable(@CliOption(key = MashCliStrings.DELETE_MTABLE__NAME, mandatory = true,
      optionContext = ConverterHint.TABLE_PATH,
      help = MashCliStrings.DELETE_MTABLE__NAME__HELP) String name) {
    try {
      Set<DistributedMember> targetMembers;
      String[] groups = null;
      try {
        targetMembers = CliUtil.findOneMatchingMember(groups, null);
      } catch (CommandResultException ex) {
        logger.error("Error while finding members. Error: " + ex.getResult().nextLine());
        return ex.getResult();
      }
      TabularResultData tabularData = ResultBuilder.createTabularResultData();
      boolean accumulatedData = false;
      ResultCollector<?, ?> rc =
          CliUtil.executeFunction(new DeleteMTableFunction(), new Object[] {name}, targetMembers);
      List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());
      XmlEntity xmlEntity = null;
      for (CliFunctionResult result : results) {
        if (result.getThrowable() != null) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("Result", "ERROR: " + result.getThrowable().getClass().getName()
              + ": " + result.getThrowable().getMessage());
          accumulatedData = true;
          tabularData.setStatus(Result.Status.ERROR);
        } else if (result.getMessage() != null) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("Result", result.getMessage());
          accumulatedData = true;

          if (xmlEntity == null) {
            xmlEntity = result.getXmlEntity();
          }
        }
      }
      if (!accumulatedData) {
        return ResultBuilder
            .createUserErrorResult(CliStrings.format(MashCliStrings.MTABLE_NOT_FOUND, name));
      }
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult(
          CliStrings.format(MashCliStrings.DELETE_MTABLE__ERROR_WHILE_DELETING_REASON_0,
              new Object[] {th.getMessage()}));
    }
    return ResultBuilder
        .createInfoResult(CliStrings.format(MashCliStrings.DELETE_MTABLE__SUCCESS, name));
  }


  @CliCommand(value = MashCliStrings.DESCRIBE_MTABLE, help = MashCliStrings.DESCRIBE_MTABLE__HELP)
  @CliMetaData(shellOnly = false,
      relatedTopic = {MashCliStrings.TOPIC_MTABLE, MashCliStrings.TOPIC_FTABLE})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public Result describeMtable(@CliOption(key = MashCliStrings.DESCRIBE_MTABLE__NAME,
      mandatory = true, optionContext = ConverterHint.TABLE_PATH,
      help = MashCliStrings.DESCRIBE_MTABLE__NAME__HELP) String name) {
    Result result = null;
    try {
      if (name == null || name.isEmpty()) {
        return ResultBuilder.createUserErrorResult("Please provide a mtable name");
      }
      if (name.equals(Region.SEPARATOR)) {
        return ResultBuilder.createUserErrorResult(MashCliStrings.INVALID_MTABLE_NAME);
      }
      Cache cache = CacheFactory.getAnyInstance();
      ResultCollector<?, ?> rc =
          CliUtil.executeFunction(getRegionDescription, name, CliUtil.getAllMembers(cache));
      List<?> resultList = (List<?>) rc.getResult();
      // The returned result could be a region description with per member and /or single local
      // region
      Object[] results = resultList.toArray();
      List<RegionDescription> regionDescriptionList = new ArrayList<RegionDescription>();

      for (int i = 0; i < results.length; i++) {
        if (results[i] instanceof RegionDescriptionPerMember) {
          RegionDescriptionPerMember regionDescPerMember = (RegionDescriptionPerMember) results[i];
          if (regionDescPerMember != null) {
            RegionDescription regionDescription = new RegionDescription();
            regionDescription.add(regionDescPerMember);
            for (int j = i + 1; j < results.length; j++) {
              if (results[j] != null && results[j] instanceof RegionDescriptionPerMember) {
                RegionDescriptionPerMember preyRegionDescPerMember =
                    (RegionDescriptionPerMember) results[j];
                if (regionDescription.add(preyRegionDescPerMember)) {
                  results[j] = null;
                }
              }
            }
            regionDescriptionList.add(regionDescription);
          }
        } else if (results[i] instanceof Throwable) {
          Throwable t = (Throwable) results[i];
          LogWrapper.getInstance().info(t.getMessage(), t);
        }
      }

      if (regionDescriptionList.isEmpty()) {
        return ResultBuilder
            .createUserErrorResult(CliStrings.format(MashCliStrings.MTABLE_NOT_FOUND, name));
      }

      CompositeResultData crd = ResultBuilder.createCompositeResultData();
      Iterator<RegionDescription> iters = regionDescriptionList.iterator();

      while (iters.hasNext()) {
        RegionDescription regionDescription = iters.next();

        // No point in displaying the scope for PR's
        if (regionDescription.isPartition()) {
          regionDescription.getCndRegionAttributes().remove(RegionAttributesNames.SCOPE);
        } else {
          String scope =
              regionDescription.getCndRegionAttributes().get(RegionAttributesNames.SCOPE);
          if (scope != null) {
            scope = scope.toLowerCase().replace('_', '-');
            regionDescription.getCndRegionAttributes().put(RegionAttributesNames.SCOPE, scope);
          }
        }
        CompositeResultData.SectionResultData regionSection = crd.addSection();
        regionSection.addSeparator('-');
        regionSection.addData("Name", regionDescription.getName());

        String dataPolicy =
            regionDescription.getDataPolicy().toString().toLowerCase().replace('_', ' ');
        regionSection.addData("Data Policy", dataPolicy);

        String memberType = "";

        if (regionDescription.isAccessor()) {
          memberType = CliStrings.DESCRIBE_REGION__ACCESSOR__MEMBER;
        } else {
          memberType = CliStrings.DESCRIBE_REGION__HOSTING__MEMBER;
        }
        regionSection.addData(memberType,
            CliUtil.convertStringSetToString(regionDescription.getHostingMembers(), '\n'));
        regionSection.addSeparator('.');

        TabularResultData commonNonDefaultAttrTable = regionSection.addSection().addTable();

        commonNonDefaultAttrTable.setHeader(CliStrings
            .format(CliStrings.DESCRIBE_REGION__NONDEFAULT__COMMONATTRIBUTES__HEADER, memberType));
        // Common Non Default Region Attributes
        Map<String, String> cndRegionAttrsMap = regionDescription.getCndRegionAttributes();

        // Common Non Default Eviction Attributes
        Map<String, String> cndEvictionAttrsMap = regionDescription.getCndEvictionAttributes();

        // Common Non Default Partition Attributes
        Map<String, String> cndPartitionAttrsMap = regionDescription.getCndPartitionAttributes();

        // Get the MTable specific descriptions
        Set<DistributedMember> targetMembers;
        String[] groups = null;
        try {
          targetMembers = CliUtil.findOneMatchingMember(groups, null);
        } catch (CommandResultException ex) {
          logger.error("Error while finding members. Error: " + ex.getResult().nextLine());
          return ex.getResult();
        }
        logWrapper.fine("cndRegionAttrsMap   " + cndRegionAttrsMap);
        logWrapper.fine("cndEvictionAttrsMap   " + cndEvictionAttrsMap);
        logWrapper.fine("cndPartitionAttrsMap   " + cndPartitionAttrsMap);

        List<Map<String, Map<String, String>>> resultMaps =
            (List<Map<String, Map<String, String>>>) CliUtil
                .executeFunction(new DescMTableFunction(), new Object[] {name}, targetMembers)
                .getResult();
        final Map<String, Map<String, String>> finalMap = new LinkedHashMap<>(resultMaps.size());
        for (Map<String, Map<String, String>> resultMap : resultMaps) {
          finalMap.putAll(resultMap);
        }
        if (resultMaps.size() != 0) {
          /* swap size and total-block-count in case of FTable.. */
          final Map<String, String> attrMap = finalMap.get(DescMTableFunction.ATTRIBUTES);
          final String totalCount;
          if (attrMap != null
              && ((totalCount = attrMap.get(DescMTableFunction.F_TOTAL_BLOCK_COUNT)) != null)) {
            final String size = cndRegionAttrsMap.get("size");
            cndRegionAttrsMap.put("size", totalCount);
            attrMap.put(DescMTableFunction.F_TOTAL_BLOCK_COUNT, size);
          }
          writeCommonAttributesToTable(commonNonDefaultAttrTable,
              MashCliStrings.DESCRIBE_MTABLE__ATTRIBUTE__TYPE__REGION, cndRegionAttrsMap);

          writeCommonAttributesToTable(commonNonDefaultAttrTable,
              CliStrings.DESCRIBE_REGION__ATTRIBUTE__TYPE__EVICTION, cndEvictionAttrsMap);
          writeCommonAttributesToTable(commonNonDefaultAttrTable,
              CliStrings.DESCRIBE_REGION__ATTRIBUTE__TYPE__PARTITION, cndPartitionAttrsMap);
          for (final Entry<String, Map<String, String>> entry : finalMap.entrySet()) {
            writeCommonAttributesToTable(commonNonDefaultAttrTable, entry.getKey(),
                entry.getValue());
          }
        } else {
          return ResultBuilder.createGemFireErrorResult(
              CliStrings.format(MashCliStrings.DESCRIBE_MTABLE__ERROR_WHILE_DESCRIBING_REASON_0,
                  "Not an Ampool Table: " + name));
        }

        // Member-wise non default Attributes
        Map<String, RegionDescriptionPerMember> regDescPerMemberMap =
            regionDescription.getRegionDescriptionPerMemberMap();
        Set<String> members = regDescPerMemberMap.keySet();

        TabularResultData table = regionSection.addSection().addTable();

        boolean setHeader = false;
        for (String member : members) {
          RegionDescriptionPerMember regDescPerMem = regDescPerMemberMap.get(member);
          Map<String, String> ndRa = regDescPerMem.getNonDefaultRegionAttributes();
          Map<String, String> ndEa = regDescPerMem.getNonDefaultEvictionAttributes();
          Map<String, String> ndPa = regDescPerMem.getNonDefaultPartitionAttributes();

          // Get all the member-specific non-default attributes by removing the common keys
          ndRa.keySet().removeAll(cndRegionAttrsMap.keySet());
          ndEa.keySet().removeAll(cndEvictionAttrsMap.keySet());
          ndPa.keySet().removeAll(cndPartitionAttrsMap.keySet());

          logWrapper.fine("ndRa   " + ndRa);
          logWrapper.fine("ndEa   " + ndEa);
          logWrapper.fine("ndPa   " + ndPa);

          // Scope is not valid for PR's
          if (regionDescription.isPartition()) {
            if (ndRa.get(RegionAttributesNames.SCOPE) != null) {
              ndRa.remove(RegionAttributesNames.SCOPE);
            }
          }

          List<FixedPartitionAttributesInfo> fpaList = regDescPerMem.getFixedPartitionAttributes();

          logWrapper.fine("fpaList   " + fpaList);

          if (!(ndRa.isEmpty() && ndEa.isEmpty() && ndPa.isEmpty()) || fpaList != null) {
            setHeader = true;
            boolean memberNameAdded = false;
            memberNameAdded = writeAttributesToTable(table,
                CliStrings.DESCRIBE_REGION__ATTRIBUTE__TYPE__REGION, ndRa, member, memberNameAdded);
            memberNameAdded =
                writeAttributesToTable(table, CliStrings.DESCRIBE_REGION__ATTRIBUTE__TYPE__EVICTION,
                    ndEa, member, memberNameAdded);
            memberNameAdded = writeAttributesToTable(table,
                CliStrings.DESCRIBE_REGION__ATTRIBUTE__TYPE__PARTITION, ndPa, member,
                memberNameAdded);

            writeFixedPartitionAttributesToTable(table, "", fpaList, member, memberNameAdded);

          }
        }

        if (setHeader == true) {
          table.setHeader(CliStrings.format(
              CliStrings.DESCRIBE_REGION__NONDEFAULT__PERMEMBERATTRIBUTES__HEADER, memberType));
        }
      }

      result = ResultBuilder.buildResult(crd);


    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult(
          CliStrings.format(MashCliStrings.DESCRIBE_MTABLE__ERROR_WHILE_DESCRIBING_REASON_0,
              new Object[] {th.getMessage()}));
    }
    return result;
  }

  @CliCommand(value = MashCliStrings.SHOW__DISTRIBUTION,
      help = MashCliStrings.SHOW__DISTRIBUTION__HELP)
  @CliMetaData(shellOnly = false,
      relatedTopic = {MashCliStrings.TOPIC_MTABLE, MashCliStrings.TOPIC_FTABLE})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public Result showDistribution(
      @CliOption(key = MashCliStrings.SHOW__DISTRIBUTION__NAME, mandatory = true,
          optionContext = ConverterHint.TABLE_PATH,
          help = MashCliStrings.SHOW__DISTRIBUTION__NAME__HELP) String name,
      @CliOption(key = MashCliStrings.SHOW__DISTRIBUTION__INCLUDE_SECONDARY,
          unspecifiedDefaultValue = "false",
          help = MashCliStrings.SHOW__DISTRIBUTION__INCLUDE_SECONDARY__HELP) boolean includeSecondary) {
    Result result = null;
    try {
      if (name == null || name.isEmpty()) {
        return ResultBuilder.createUserErrorResult("Please provide a table name");
      }
      if (name.equals(Region.SEPARATOR)) {
        return ResultBuilder.createUserErrorResult(MashCliStrings.INVALID_MTABLE_NAME);
      }

      Cache cache = CacheFactory.getAnyInstance();
      MGetMetadataFunction.Args args = new MGetMetadataFunction.Args(name);

      ResultCollector<?, ?> rc = CliUtil.executeFunction(MGetMetadataFunction.GET_METADATA_FUNCTION,
          args, CliUtil.getAllNormalMembers(cache));
      List<Object[]> output = (List<Object[]>) rc.getResult();

      Map<ServerLocation, TreeSet<Pair<Integer, Long>>> primaryMap = new TreeMap();
      Map<ServerLocation, TreeSet<Pair<Integer, Long>>> secondaryMap = new TreeMap();

      for (Object[] objects : output) {
        Map<Integer, Pair<ServerLocation, Long>> bucketToServerLocationMap =
            (Map<Integer, Pair<ServerLocation, Long>>) objects[0];
        bucketToServerLocationMap.forEach((bucketId, pair) -> {
          TreeSet<Pair<Integer, Long>> pairs = new TreeSet<>(getComparator());
          if (primaryMap.containsKey(pair.getFirst())) {
            pairs = primaryMap.get(pair.getFirst());
          }
          pairs.add(new Pair(bucketId, pair.getSecond()));
          primaryMap.put(pair.getFirst(), pairs);
        });

        // secondary map population
        if (includeSecondary) {
          Map<Integer, Set<Pair<ServerLocation, Long>>> secondaryBucketsMap = (Map) objects[1];
          bucketToServerLocationMap.forEach((bucketId, pair) -> {

            TreeSet<Pair<Integer, Long>> pairs = new TreeSet<>(getComparator());

            if (secondaryMap.containsKey(pair.getFirst())) {
              pairs = secondaryMap.get(pair.getFirst());
            }
            pairs.add(new Pair(bucketId, pair.getSecond()));
            secondaryMap.put(pair.getFirst(), pairs);
          });
        }
      }

      CompositeResultData resultData = ResultBuilder.createCompositeResultData();
      CompositeResultData.SectionResultData miscData = resultData.addSection("misc");
      miscData.addData("TableName", name);

      CompositeResultData.SectionResultData keyCountData = resultData.addSection("primary");
      keyCountData.setHeader(MashCliStrings.SHOW__DISTRIBUTION__PRIMARY);
      TabularResultData table = keyCountData.addTable();

      AtomicBoolean isFirstRecord = new AtomicBoolean();
      primaryMap.forEach(((serverLocation, pairs) -> {
        isFirstRecord.set(true);
        pairs.forEach(bucketCountPair -> {
          if (isFirstRecord.get()) {
            table.accumulate(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_SERVER_NAME,
                serverLocation.toString());
            isFirstRecord.set(false);
          } else {
            table.accumulate(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_SERVER_NAME, "");
          }
          table.accumulate(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_BUCKET_ID,
              bucketCountPair.getFirst());
          table.accumulate(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_KEY_COUNT,
              bucketCountPair.getSecond());
        });
      }));

      if (includeSecondary) {
        CompositeResultData.SectionResultData secondarykeyCountData =
            resultData.addSection("secondary");
        secondarykeyCountData.setHeader(MashCliStrings.SHOW__DISTRIBUTION__SECONDARY);
        TabularResultData secondaryTable = secondarykeyCountData.addTable();
        secondaryMap.forEach(((serverLocation, pairs) -> {
          isFirstRecord.set(true);
          pairs.forEach(bucketCountPair -> {
            if (isFirstRecord.get()) {
              secondaryTable.accumulate(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_SERVER_NAME,
                  serverLocation.toString());
              isFirstRecord.set(false);
            } else {
              secondaryTable.accumulate(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_SERVER_NAME,
                  "");
            }
            secondaryTable.accumulate(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_BUCKET_ID,
                bucketCountPair.getFirst());
            secondaryTable.accumulate(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_KEY_COUNT,
                bucketCountPair.getSecond());
          });
        }));
      }

      result = ResultBuilder.buildResult(resultData);
    } catch (Throwable t) {
      t.printStackTrace();
      logger.error(t);
    }
    return result;
  }

  private Comparator<Pair<Integer, Long>> getComparator() {
    return new Comparator<Pair<Integer, Long>>() {
      @Override
      public int compare(Pair<Integer, Long> o1, Pair<Integer, Long> o2) {
        int firstCompare = o1.getFirst().compareTo(o2.getFirst());
        if (firstCompare == 0) {
          return o1.getSecond().compareTo(o2.getSecond());
        }
        return firstCompare;
      }
    };
  }

  public static void convertMetaDataFunctionResult(
      Map<Integer, Pair<ServerLocation, Long>> primaryMap,
      Map<Integer, Set<Pair<ServerLocation, Long>>> secondaryMap, List<Object[]> output) {
    for (Object[] objects : output) {

      Map bucketToServerLocationMap = (Map) objects[0];

      primaryMap.putAll(bucketToServerLocationMap);
      /*
       * if (secondaryMap != null) { Map<Integer, Set<Pair<ServerLocation, Long>>> map = (Map)
       * objects[1]; logger.error("what is value2 " + (map).keySet()); for (Map.Entry<Integer,
       * Set<Pair<ServerLocation, Long>>> entry : map.entrySet()) { if
       * (secondaryMap.containsKey(entry.getKey())) {
       * secondaryMap.get(entry.getKey()).addAll(entry.getValue()); } else {
       * secondaryMap.put(entry.getKey(), entry.getValue()); } } }
       */
    }
  }


  protected TreeMap<String, String> toMTableStatsResult(final List<Map<String, String>> result) {
    final CompositeResultData MTableData = ResultBuilder.createCompositeResultData();
    TreeMap<String, String> MTabletypeMap = new TreeMap<String, String>();

    final CompositeResultData.SectionResultData MTableSection = MTableData.addSection();
    java.util.Iterator<Map<String, String>> it = result.iterator();
    while (it.hasNext()) {
      Map<String, String> resultMap = it.next();
      for (Map.Entry<String, String> entry : resultMap.entrySet()) {
        MTableSection.addData(entry.getKey(), entry.getValue());
        MTabletypeMap.put(entry.getKey(), entry.getValue());
      }
    }

    LogWrapper logWrapper = LogWrapper.getInstance();
    logWrapper.fine("MTableData  " + MTableData);
    return (MTabletypeMap);


  }

  /**
   * Interceptor used by gfsh to intercept execution of deploy command at "shell".
   */
  public static class Interceptor extends AbstractCliAroundInterceptor {
    private final DecimalFormat numFormatter = new DecimalFormat("###,##0.00");

    @Override
    public Result preExecution(GfshParseResult parseResult) {
      String[] files = new String[3];
      Map<String, String> paramValueMap = parseResult.getParamValueStrings();

      String storeProps = paramValueMap.get("props-file");
      files[0] = (storeProps == null) ? null : storeProps.trim();

      String readerOpts = paramValueMap.get("reader-opts");
      files[1] = (readerOpts == null) ? null : readerOpts.trim();

      String writerOpts = paramValueMap.get("writer-opts");
      files[2] = (writerOpts == null) ? null : writerOpts.trim();

      FileResult fileResult;
      try {
        fileResult = new FileResult(files);
      } catch (FileNotFoundException fnfex) {
        return ResultBuilder.createInfoResult(fnfex.getMessage());
      } catch (IOException ioex) {
        return ResultBuilder
            .createInfoResult("I/O error when reading properties file: " + ioex.getMessage());
      }
      return fileResult;
    }

    @Override
    public Result postExecution(GfshParseResult parseResult, Result commandResult) {
      return null;
    }
  }

  @CliCommand(value = {MashCliStrings.CREATE_TIERSTORE},
      help = MashCliStrings.CREATE_TIERSTORE__HELP)
  @CliMetaData(
      interceptor = "org.apache.geode.management.internal.cli.commands.MTableCommands$Interceptor",
      relatedTopic = {MashCliStrings.TOPIC_FTABLE, MashCliStrings.TOPIC_FTABLE})
  @ResourceOperation(resource = ResourcePermission.Resource.DATA,
      operation = ResourcePermission.Operation.MANAGE)
  public Result createTierStore(
      @CliOption(key = {MashCliStrings.CREATE_TIERSTORE__NAME}, mandatory = true,
          help = MashCliStrings.CREATE_TIERSTORE__NAME__HELP) String name,
      @CliOption(key = {MashCliStrings.CREATE_TIERSTORE__HANDLER}, mandatory = true,
          help = MashCliStrings.CREATE_TIERSTORE__HANDLER__HELP) String handler,
      @CliOption(key = {MashCliStrings.CREATE_TIERSTORE__PROPSFILE},
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.CREATE_TIERSTORE__PROPSFILE__HELP) String propsFile,
      @CliOption(key = {MashCliStrings.CREATE_TIERSTORE__READER}, mandatory = true,
          help = MashCliStrings.CREATE_TIERSTORE__READER__HELP) String reader,
      @CliOption(key = {MashCliStrings.CREATE_TIERSTORE__READEROPTS},
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.CREATE_TIERSTORE__READEROPTS__HELP) String readerOptsFile,
      @CliOption(key = {MashCliStrings.CREATE_TIERSTORE__WRITER}, mandatory = true,
          help = MashCliStrings.CREATE_TIERSTORE__WRITER__HELP) String writer,
      @CliOption(key = {MashCliStrings.CREATE_TIERSTORE__WRITEROPTS},
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = MashCliStrings.CREATE_TIERSTORE__WRITEROPTS__HELP) String writerOptsFile) {

    byte[][] shellBytesData = CommandExecutionContext.getBytesFromShell();
    String[] fileNames = CliUtil.bytesToNames(shellBytesData);
    byte[][] fileBytes = CliUtil.bytesToData(shellBytesData);

    Map<String, Object> properties = new LinkedHashMap();
    if (StringUtils.isBlank(name)) {
      return ResultBuilder.createInfoResult("Store name cannot be blank.");
    }
    properties.put(TierStoreFactory.STORE_NAME_PROP, name);

    if (StringUtils.isBlank(handler)) {
      return ResultBuilder.createInfoResult("Store handler cannot be blank.");
    }
    properties.put(TierStoreFactory.STORE_HANDLER_PROP, handler);

    int fileIdx = 0;
    if (propsFile != null) {
      if (StringUtils.isBlank(propsFile)) {
        return ResultBuilder.createInfoResult("store properties file path can not be blank.");
      }
      Properties storeOpts = readProperties(fileBytes[fileIdx++]);
      if (storeOpts == null) {
        return ResultBuilder.createInfoResult("Failed to read store options file: " + propsFile);
      }
      properties.put(TierStoreFactory.STORE_OPTS_PROP, storeOpts);
    } else {
      properties.put(TierStoreFactory.STORE_OPTS_PROP, new Properties());
    }

    if (StringUtils.isBlank(reader)) {
      return ResultBuilder.createInfoResult("Store reader cannot be blank.");
    }
    properties.put(TierStoreFactory.STORE_READER_PROP, reader);

    if (readerOptsFile != null) {
      if (StringUtils.isBlank(readerOptsFile)) {
        return ResultBuilder.createInfoResult("reader properties file path can not be blank.");
      }
      Properties readerOpts = readProperties(fileBytes[fileIdx++]);
      if (readerOpts == null) {
        return ResultBuilder
            .createInfoResult("Failed to read reader options file: " + readerOptsFile);
      }
      properties.put(TierStoreFactory.STORE_READER_PROPS_PROP, readerOpts);
    } else {
      properties.put(TierStoreFactory.STORE_READER_PROPS_PROP, new Properties());
    }

    if (StringUtils.isBlank(writer)) {
      return ResultBuilder.createInfoResult("Store writer cannot be blank.");
    }
    properties.put(TierStoreFactory.STORE_WRITER_PROP, writer);

    if (writerOptsFile != null) {
      if (StringUtils.isBlank(writerOptsFile)) {
        return ResultBuilder.createInfoResult("writer properties file path can not be blank.");
      }
      Properties writerOpts = readProperties(fileBytes[fileIdx++]);
      if (writerOpts == null) {
        return ResultBuilder
            .createInfoResult("Failed to read writer options file: " + writerOptsFile);
      }
      properties.put(TierStoreFactory.STORE_WRITER_PROPS_PROP, writerOpts);
    } else {
      properties.put(TierStoreFactory.STORE_WRITER_PROPS_PROP, new Properties());
    }

    ResultCollector<?, ?> rc = null;
    Set<DistributedMember> targetMembers;
    try {
      targetMembers = CliUtil.findOneMatchingMember((String[]) null, null);
    } catch (CommandResultException crex) {
      return crex.getResult();
    }

    rc = CliUtil.executeFunction(new CreateTierStoreFunction(), properties, targetMembers);
    List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());
    XmlEntity xmlEntity = null;
    for (CliFunctionResult result : results) {
      if (result.getThrowable() != null) {
        return ResultBuilder.buildResult(
            ResultBuilder.createErrorResultData().setHeader(MashCliStrings.CREATE_TIERSTORE__FAILURE
                + name + "\n" + result.getThrowable().getMessage()));
      } else if (result.getMessage() != null) {
        if (result.isSuccessful() != true) {
          return ResultBuilder.buildResult(ResultBuilder.createErrorResultData().setHeader(
              MashCliStrings.CREATE_TIERSTORE__FAILURE + name + "\n" + result.getMessage()));
        }
      }
    }
    return ResultBuilder
        .createInfoResult(CliStrings.format(MashCliStrings.CREATE_TIERSTORE__SUCCESS) + name);
  }

  private Properties readProperties(byte[] propsBytes) {
    Properties props = new Properties();
    try {
      props.load(new ByteArrayInputStream(propsBytes));
    } catch (IOException e) {
      return null;
    }
    return props;
  }

  @CliCommand(value = {MashCliStrings.LIST_MTABLE}, help = MashCliStrings.LIST_MTABLE__HELP)
  @CliMetaData(shellOnly = false,
      relatedTopic = {MashCliStrings.TOPIC_MTABLE, MashCliStrings.TOPIC_FTABLE})
  @ResourceOperation(resource = ResourcePermission.Resource.DATA,
      operation = ResourcePermission.Operation.READ)
  public Result listRegion(
      @CliOption(key = {MashCliStrings.LIST_MTABLE__GROUP},
          optionContext = ConverterHint.MEMBERGROUP,
          help = MashCliStrings.LIST_MTABLE__GROUP__HELP) String group,
      @CliOption(key = {MashCliStrings.LIST_MTABLE__MEMBER},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = MashCliStrings.LIST_MTABLE__MEMBER__HELP) String memberNameOrId) {
    Result result = null;
    try {
      Set<String> mtableNamesSet = new LinkedHashSet<String>();
      ResultCollector<?, ?> rc = null;
      Set<DistributedMember> targetMembers;
      try {
        targetMembers = CliUtil.findAllMatchingMembers(group, memberNameOrId);
      } catch (CommandResultException crex) {
        return crex.getResult();
      }
      rc = CliUtil.executeFunction(getMTablesFunction, null, targetMembers);
      List<HashMap<String, TableType>> resultList = (ArrayList) rc.getResult();
      if (resultList != null && !resultList.isEmpty()) {
        HashMap<String, TableType> finalValueMap = new HashMap<>();
        for (int i = 0; i < resultList.size(); i++) {
          Iterator<Entry<String, TableType>> memberIters = resultList.get(i).entrySet().iterator();
          while (memberIters.hasNext()) {
            Entry<String, TableType> entry = memberIters.next();
            finalValueMap.put(entry.getKey(), entry.getValue());
          }
        }
        if (finalValueMap.size() > 0) {
          TabularResultData resultData = ResultBuilder.createTabularResultData();
          Iterator<Entry<String, TableType>> valueItr = finalValueMap.entrySet().iterator();
          Entry<String, TableType> entry;
          while (valueItr.hasNext()) {
            entry = valueItr.next();
            resultData.accumulate("Name", entry.getKey());
            resultData.accumulate("Type", entry.getValue());
          }
          result = ResultBuilder.buildResult(resultData);
        } else {
          result = ResultBuilder.createInfoResult(MashCliStrings.LIST_MTABLE__MSG__NOT_FOUND);
        }
      }
    } catch (FunctionInvocationTargetException e) {
      result = ResultBuilder.createGemFireErrorResult(MashCliStrings
          .format(CliStrings.COULD_NOT_EXECUTE_COMMAND_TRY_AGAIN, MashCliStrings.LIST_MTABLE));
    } catch (Exception e) {
      result = ResultBuilder.createGemFireErrorResult(
          MashCliStrings.LIST_MTABLE__MSG__ERROR + " : " + e.getMessage());
    }
    return result;
  }

  @CliCommand(value = {MashCliStrings.LIST_TIER_STORES},
      help = MashCliStrings.LIST_TIER_STORES__HELP)
  @CliMetaData(shellOnly = false, relatedTopic = {MashCliStrings.TOPIC_FTABLE})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public Result listTierStores(
      @CliOption(key = {MashCliStrings.LIST_TIER_STORES__GROUP},
          optionContext = ConverterHint.MEMBERGROUP,
          help = MashCliStrings.LIST_TIER_STORES__GROUP__HELP) String[] group,
      @CliOption(key = {MashCliStrings.LIST_TIER_STORES__MEMBER},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = MashCliStrings.LIST_TIER_STORES__MEMBER__HELP) String[] memberNameOrId) {
    Result result = null;
    try {
      Set<DistributedMember> targetMembers;
      try {
        targetMembers = CliUtil.findOneMatchingMember(group, memberNameOrId);
      } catch (CommandResultException crex) {
        return crex.getResult();
      }
      ResultCollector<?, ?> rc =
          CliUtil.executeFunction(getTierStoresFunction, null, targetMembers);
      List<HashSet<String>> resultList = (List<HashSet<String>>) rc.getResult();
      if (resultList != null && !resultList.isEmpty()) {
        Set<String> finalValueSet = new HashSet<>();
        for (int i = 0; i < resultList.size(); i++) {
          Iterator<String> memberIters = resultList.get(i).iterator();
          while (memberIters.hasNext()) {
            String next = memberIters.next();
            finalValueSet.add(next);
          }
        }
        if (finalValueSet.size() > 0) {
          TabularResultData resultData = ResultBuilder.createTabularResultData();
          Iterator<String> valueItr = finalValueSet.iterator();
          String entry;
          while (valueItr.hasNext()) {
            entry = valueItr.next();
            resultData.accumulate("Name", entry);
          }
          result = ResultBuilder.buildResult(resultData);
        } else {
          result = ResultBuilder.createInfoResult(MashCliStrings.LIST_TIER_STORES__MSG__NOT_FOUND);
        }
      }
    } catch (FunctionInvocationTargetException e) {
      result = ResultBuilder.createGemFireErrorResult(MashCliStrings
          .format(CliStrings.COULD_NOT_EXECUTE_COMMAND_TRY_AGAIN, MashCliStrings.LIST_TIER_STORES));
    } catch (Exception e) {
      result = ResultBuilder.createGemFireErrorResult(
          MashCliStrings.LIST_TIER_STORES__MSG__ERROR + " : " + e.getMessage());
    }
    return result;
  }

  @CliCommand(value = {MashCliStrings.DESCRIBE_TIER_STORE},
      help = MashCliStrings.DESCRIBE_TIER_STORE__HELP)
  @CliMetaData(shellOnly = false, relatedTopic = {MashCliStrings.TOPIC_FTABLE})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public Result describeTierStore(@CliOption(key = {MashCliStrings.DESCRIBE_TIER_STORE__NAME},
      mandatory = true, optionContext = ConverterHint.TIER_STORE_PATH,
      help = MashCliStrings.DESCRIBE_TIER_STORE__NAME__HELP) String name) {
    Result result = null;
    if (name == null || name.isEmpty()) {
      return ResultBuilder.createUserErrorResult("Please provide a tier store name");
    }

    try {
      Set<DistributedMember> targetMembers;
      try {
        targetMembers = CliUtil.findOneMatchingMember(null, null);
      } catch (CommandResultException crex) {
        return crex.getResult();
      }
      ResultCollector<?, ?> rc =
          CliUtil.executeFunction(describeTierStoreFunction, name, targetMembers);
      Map<String, Object> resultMap = null;
      Object oResult = ((List) rc.getResult()).get(0);
      if (oResult instanceof Exception) {
        result = ResultBuilder.createInfoResult(MashCliStrings.DESCRIBE_TIER_STORE__MSG__ERROR
            + " : " + ((Exception) oResult).getMessage());
      } else {
        resultMap = (Map<String, Object>) oResult;
        CompositeResultData crd = ResultBuilder.createCompositeResultData();
        CompositeResultData.SectionResultData srd = crd.addSection();
        srd.addSeparator('-');

        srd.addData("Name", resultMap.get(TierStoreFactory.STORE_NAME_PROP));
        srd.addData("Handler class", resultMap.get(TierStoreFactory.STORE_HANDLER_PROP));
        srd.addData("Reader class", resultMap.get(TierStoreFactory.STORE_READER_PROP));
        srd.addData("Writer class", resultMap.get(TierStoreFactory.STORE_WRITER_PROP));
        srd.addSeparator('.');

        Properties storeProps = (Properties) resultMap.get(TierStoreFactory.STORE_OPTS_PROP);
        TabularResultData storePropsTab = srd.addSection().addTable();
        storePropsTab.setHeader("Properties");

        if (storeProps != null) {
          writeCommonAttributesToTable(storePropsTab, "Store Properties", (Map) storeProps);
        }

        Properties readerProps =
            (Properties) resultMap.get(TierStoreFactory.STORE_READER_PROPS_PROP);

        if (readerProps != null) {
          writeCommonAttributesToTable(storePropsTab, "Reader Properties", (Map) readerProps);
        }

        Properties writerProps =
            (Properties) resultMap.get(TierStoreFactory.STORE_WRITER_PROPS_PROP);

        if (writerProps != null) {
          writeCommonAttributesToTable(storePropsTab, "Writer Properties", (Map) writerProps);
        }
        result = ResultBuilder.buildResult(crd);
      }
    } catch (FunctionInvocationTargetException e) {
      result = ResultBuilder.createInfoResult(MashCliStrings.format(
          CliStrings.COULD_NOT_EXECUTE_COMMAND_TRY_AGAIN, MashCliStrings.DESCRIBE_TIER_STORE));
    } catch (Exception e) {
      result = ResultBuilder.createInfoResult(
          MashCliStrings.DESCRIBE_TIER_STORE__MSG__ERROR + " : " + e.getMessage());
    }
    return result;
  }

  @CliCommand(value = {MashCliStrings.DESTROY_TIER_STORE},
      help = MashCliStrings.DESTROY_TIER_STORE__HELP)
  @CliMetaData(shellOnly = false, relatedTopic = {MashCliStrings.TOPIC_FTABLE})
  @ResourceOperation(resource = ResourcePermission.Resource.DATA,
      operation = ResourcePermission.Operation.MANAGE)
  public Result destroyTierStore(@CliOption(key = {MashCliStrings.DESTROY_TIER_STORE__NAME},
      mandatory = true, help = MashCliStrings.DESTROY_TIER_STORE__NAME__HELP) String name) {
    Result result = null;
    if (name == null || name.isEmpty()) {
      return ResultBuilder.createUserErrorResult("Please provide a tier store name");
    }

    if (name.equals(DefaultStore.STORE_NAME)) {
      return ResultBuilder
          .createUserErrorResult(MashCliStrings.DESTROY_TIER_STORE__DEFAULT_STORE_MSG__ERROR);
    }

    try {
      Set<DistributedMember> targetMembers;
      try {
        targetMembers = CliUtil.findOneMatchingMember(null, null);
      } catch (CommandResultException crex) {
        return crex.getResult();
      }
      ResultCollector<?, ?> rc =
          CliUtil.executeFunction(new DestroyTierStoreFunction(), name, targetMembers);
      Map<String, Object> resultMap = null;
      Object oResult = ((List) rc.getResult()).get(0);
      if (oResult instanceof Exception) {
        result = ResultBuilder.buildResult(ResultBuilder.createErrorResultData()
            .setHeader(MashCliStrings.DESTROY_TIER_STORE__MSG__ERROR + ": "
                + ((Exception) oResult).getMessage()));
      } else {
        result = ResultBuilder.createInfoResult(MashCliStrings.DESTROY_TIER_STORE__MSG__SUCCESS);
      }
    } catch (FunctionInvocationTargetException e) {
      result = ResultBuilder.createInfoResult(MashCliStrings.format(
          CliStrings.COULD_NOT_EXECUTE_COMMAND_TRY_AGAIN, MashCliStrings.DESTROY_TIER_STORE));
    } catch (Exception e) {
      result = ResultBuilder.buildResult(ResultBuilder.createErrorResultData()
          .setHeader(MashCliStrings.DESTROY_TIER_STORE__MSG__ERROR + ": " + e.getMessage()));
    }
    return result;
  }

  private void writeCommonAttributesToTable(TabularResultData table, String attributeType,
      Map<String, String> attributesMap) {
    if (!attributesMap.isEmpty()) {
      Set<String> attributes = attributesMap.keySet();
      boolean isTypeAdded = false;
      final String blank = "";

      Iterator<String> iters = attributes.iterator();

      while (iters.hasNext()) {
        String attributeName = iters.next();
        String attributeValue = attributesMap.get(attributeName);
        String type, memName;

        if (!isTypeAdded) {
          type = attributeType;
          isTypeAdded = true;
        } else {
          type = blank;
        }
        /* do not split on comma (,) for schema as it has comma in it.. should be generic?? */
        String[] attributeValues = DescMTableFunction.SCHEMA.equals(attributeType)
            ? new String[] {attributeValue} : attributeValue.split(",");
        writeCommonAttributeToTable(table, type, attributeName, attributeValues);
      }
    }
  }


  private void writeCommonAttributeToTable(TabularResultData table, String attributeType,
      String attributeName, String[] attributeValues) {
    final String blank = "";

    if (attributeValues != null) {
      boolean isFirstValue = true;
      for (String value : attributeValues) {
        if (isFirstValue) {
          isFirstValue = false;
          table.accumulate(CliStrings.DESCRIBE_REGION__ATTRIBUTE__TYPE, attributeType);
          table.accumulate(CliStrings.DESCRIBE_REGION__ATTRIBUTE__NAME, attributeName);
          table.accumulate(CliStrings.DESCRIBE_REGION__ATTRIBUTE__VALUE, value);
        } else {
          table.accumulate(CliStrings.DESCRIBE_REGION__ATTRIBUTE__TYPE, blank);
          table.accumulate(CliStrings.DESCRIBE_REGION__ATTRIBUTE__NAME, blank);
          table.accumulate(CliStrings.DESCRIBE_REGION__ATTRIBUTE__VALUE, value);
        }
      }

    }
  }

  private boolean writeAttributesToTable(TabularResultData table, String attributeType,
      Map<String, String> attributesMap, String member, boolean isMemberNameAdded) {
    if (!attributesMap.isEmpty()) {
      Set<String> attributes = attributesMap.keySet();
      boolean isTypeAdded = false;
      final String blank = "";

      Iterator<String> iters = attributes.iterator();

      while (iters.hasNext()) {
        String attributeName = iters.next();
        String attributeValue = attributesMap.get(attributeName);
        String type, memName;

        if (!isTypeAdded) {
          type = attributeType;
          isTypeAdded = true;
        } else {
          type = blank;
        }

        if (!isMemberNameAdded) {
          memName = member;
          isMemberNameAdded = true;
        } else {
          memName = blank;
        }

        writeAttributeToTable(table, memName, type, attributeName, attributeValue);
      }
    }

    return isMemberNameAdded;
  }

  public void writeAttributeToTable(TabularResultData table, String member, String attributeType,
      String attributeName, String attributeValue) {

    final String blank = "";
    if (attributeValue != null) {
      // Tokenize the attributeValue
      String[] attributeValues = attributeValue.split(",");
      boolean isFirstValue = true;

      for (String value : attributeValues) {
        if (isFirstValue) {
          table.accumulate(CliStrings.DESCRIBE_REGION__MEMBER, member);
          table.accumulate(CliStrings.DESCRIBE_REGION__ATTRIBUTE__TYPE, attributeType);
          table.accumulate(CliStrings.DESCRIBE_REGION__ATTRIBUTE__NAME, attributeName);
          table.accumulate(CliStrings.DESCRIBE_REGION__ATTRIBUTE__VALUE, value);
          isFirstValue = false;
        } else {
          table.accumulate(CliStrings.DESCRIBE_REGION__MEMBER, blank);
          table.accumulate(CliStrings.DESCRIBE_REGION__ATTRIBUTE__TYPE, blank);
          table.accumulate(CliStrings.DESCRIBE_REGION__ATTRIBUTE__NAME, blank);
          table.accumulate(CliStrings.DESCRIBE_REGION__ATTRIBUTE__VALUE, value);
        }
      }
    }
  }

  private boolean writeFixedPartitionAttributesToTable(TabularResultData table,
      String attributeType, List<FixedPartitionAttributesInfo> fpaList, String member,
      boolean isMemberNameAdded) {

    if (fpaList != null) {
      boolean isTypeAdded = false;
      final String blank = "";

      Iterator<FixedPartitionAttributesInfo> fpaIter = fpaList.iterator();
      String type, memName;

      while (fpaIter.hasNext()) {
        FixedPartitionAttributesInfo fpa = fpaIter.next();
        StringBuilder fpaBuilder = new StringBuilder();
        fpaBuilder.append(fpa.getPartitionName());
        fpaBuilder.append(',');

        if (fpa.isPrimary()) {
          fpaBuilder.append("Primary");
        } else {
          fpaBuilder.append("Secondary");
        }
        fpaBuilder.append(',');
        fpaBuilder.append(fpa.getNumBuckets());

        if (!isTypeAdded) {
          type = attributeType;
          isTypeAdded = true;
        } else {
          type = blank;
        }

        if (!isMemberNameAdded) {
          memName = member;
          isMemberNameAdded = true;
        } else {
          memName = blank;
        }

        writeAttributeToTable(table, memName, type, "Fixed Partition", fpaBuilder.toString());
      }
    }

    return isMemberNameAdded;
  }
}
