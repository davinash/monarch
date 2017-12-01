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
package io.ampool.monarch.cli;


import java.text.MessageFormat;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.tierstore.internal.DefaultStore;

/**
 * -* Contains 'String' constants used as key to the Localized strings to be used in classes under
 * <code>org.apache.geode.management.internal.cli</code> for Command Line Interface (CLI).
 *
 * @since 7.0
 */
/*-
 * NOTES:
 * 1. CONVENTIONS: Defining constants for Command Name, option, argument, help:
 * 1.1 Command Name:
 *     Command name in BOLD. Multiple words separated by single underscore ('_')
 *     E.g. COMPACT_DISK_STORE
 * 1.2 Command Help Text:
 *     Command name in BOLD followed by double underscore ('__') followed by HELP
 *     in BOLD.
 *     E.g.COMPACT_DISK_STORE__HELP
 * 1.3 Command Option:
 *     Command name in BOLD followed by double underscore ('__') followed by option
 *     name in BOLD - multiple words concatenated by removing space.
 *     E.g. for option "--disk-dirs" - COMPACT_DISK_STORE__DISKDIRS
 * 1.4 Command Option Help:
 *     As mentioned in 1.3, followed by double underscore ('__') followed by HELP
 *     in BOLD.
 *     E.g. COMPACT_DISK_STORE__DISKDIRS__HELP
 * 1.5 Info/Error Message used in a command:
 *     Command name in BOLD followed by double underscore ('__') followed by MSG
 *     in BOLD followed by brief name for the message (Similar to used in LocalizedStrings).
 *     E.g. COMPACT_DISK_STORE__MSG__ERROR_WHILE_COMPACTING = "Error occurred while compacting disk store."
 * 1.6 Parameterized messages are supposed to be handled by users. It's
 *     recommended to use the same conventions as used in LocalizedStrings.
 *     E.g. COMPACT_DISK_STORE__MSG__ERROR_WHILE_COMPACTING_	_0 = "Error occurred while compacting disk store {0}."
 *
 * 2. Defining Topic constants:
 * 2.1 The constants' names should begin with "TOPIC_"
 *     E.g. TOPIC_GEMFIRE_REGION
 * 2.2 Topic brief description should be defined with suffix "__DESC".
 *     E.g. TOPIC_GEMFIRE_REGION__DESC
 *
 * 3. Order for adding constants: It should be alphabetically sorted at least
 *    on the first name within the current group
 */

@InterfaceAudience.Private
@InterfaceStability.Stable
public class MashCliStrings {

  /*-*************************************************************************
   *************                  T O P I C S                  ***************
   ***************************************************************************/
  public static final String TOPIC_MTABLE = "Ampool MTable";
  public static final String TOPIC_FTABLE = "Ampool FTable";
  public static final String TOPIC_AMPOOL_AUTHZ = "Ampool authorization";
  public static final String REST_INSTANCE = "geode-api";
  public static final String AMPOOL_REST_INSTANCE = "ampool-api";
  public static final String PRODUCT = "Ampool ADS";
  public static final String AMPOOL_PRODUCT = "Ampool Active Data Store (ADS)";

  /* 'create table' command */
  public static final String CREATE_MTABLE = "create table";
  public static final String CREATE_MTABLE__NAME = "name";
  public static final String CREATE_MTABLE__NAME__HELP =
      "Name of the table to be created. Valid name includes only alphanumeric chars [A-Za-z0-9_-]";
  public static final String CREATE_MTABLE__TYPE = "type";
  public static final String CREATE_MTABLE__TYPE__HELP =
      "Table Type: MTable (ORDERED_VERSIONED/ UNORDERED) or FTable (IMMUTABLE).";
  public static final String CREATE_MTABLE__HELP = "create a table";
  public static final String CREATE_MTABLE_COLUMNS = "columns";
  public static final String CREATE_MTABLE_COLUMNS__HELP =
      "Names of columns to be added. A comma separated list of column names e.g Age,Salary, etc. Don't use quotes while specifying column names";
  public static final String CREATE_MTABLE_SCHEMA = "schema-json";
  public static final String CREATE_MTABLE_SCHEMA__HELP =
      "The schema of the table to be created. A valid JSON with column-name and respective string representation of column-type.";
  public static final String CREATE_MTABLE___HELP =
      "Either option `--columns` or `--schema` must be provided.";
  public static final String CREATE_MTABLE___HELP_1 =
      "Only one option `--columns` or `--schema` should be provided.";
  public static final String CREATE_MTABLE__ERROR_WHILE_CREATING_REASON_0 =
      "An error occurred while creating the table: \"{0}\"";
  public static final String CREATE_MTABLE__SUCCESS = "Successfully created the table: ";
  public static final String CREATE_MTABLE__REDUNDANT_COPIES = "ncopies";
  public static final String CREATE_MTABLE__REDUNDANT_COPIES__HELP =
      "Sets the Redundant copies of the data for a table.Please choose a value between 0 and 3 (inclusive) ";
  public static final String CREATE_MTABLE__RECOVERY_DELAY = "recovery-delay";
  public static final String CREATE_MTABLE__RECOVERY_DELAY__HELP =
      "Number of milliseconds to wait after a member failure before recovering redundancy. Default "
          + "value is -1, which disables recovery.";
  public static final String CREATE_MTABLE__STARTUP_RECOVERY_DELAY = "startup-recovery-delay";
  public static final String CREATE_MTABLE__STARTUP_RECOVERY_DELAY__HELP =
      "Number of milliseconds to wait after a member joins before recovering redundancy. Default "
          + "value is 0 i.e no delay";
  public static final String CREATE_MTABLE__MAX_VERSIONS = "versions";
  public static final String CREATE_MTABLE__MAX_VERSIONS__HELP =
      "Set the Maximum number of versions to be kept in for versioning. Once the max limit is reached oldest version of that row will be deleted default max version is 1. ";
  public static final String CREATE_MTABLE__ERROR_REDUNDANT_COPIES =
      "An error occurred while creating the table: Please choose a value between 0 and 3 (inclusive) for redundant copies";
  public static final String CREATE_MTABLE__DISKSTORE = "disk-store";
  public static final String CREATE_MTABLE__DISKSTORE__HELP =
      "Disk Store to be used by this table. \"list disk-stores\" can be used to display existing disk stores.";
  public static final String CREATE_MTABLE__DISKSTORE__ERROR =
      "An error occurred while creating the table: Please pass an existing disk store name";
  public static final String CREATE_MTABLE__TIERSTORES = "tier-stores";
  public static final String CREATE_MTABLE__TIERSTORES__HELP =
      "Tier Store hierarchy to be used by this table. \"list tier-stores\" can be used to display existing tier stores.";
  public static final String CREATE_MTABLE__TIERSTORES__ERROR =
      "An error occurred while creating the table: Please pass an existing tier store name";
  public static final String NUMBER_OF_TIERSTORES_LIMIT = "maximum of 2 tier-stores can be defined";

  public static final String CREATE_MTABLE__DISK_PERSISTENCE = "disk-persistence";
  public static final String CREATE_MTABLE__DISK_PERSISTENCE__HELP =
      "Enables disk persistence for a table.";
  public static final String CREATE_MTABLE__DISK_PERSISTENCE__ERROR =
      "An error occurred while creating the table";

  public static final String CREATE_MTABLE__DISK_WRITE_POLICY = "disk-write-policy";
  public static final String CREATE_MTABLE__DISK_WRITE_POLICY__HELP =
      "Disk write policy used for disk persistence of table.";
  public static final String CREATE_MTABLE__DISK_WRITE_POLICY__ERROR =
      "An error occurred while creating the table";

  public static final String CREATE_MTABLE__SPLITS = "nbuckets";
  public static final String CREATE_MTABLE__SPLITS__HELP =
      "Sets the number of configured hash based (unordered table case) or range based (ordered table case) buckets (splits) for a table. Default value is 113.";
  public static final String CREATE_MTABLE__SPLITS_ERROR =
      "An error occurred while creating the table: Please choose a value between 1 and 512 (both inclusive) for total buckets (splits)";

  public static final String CREATE_MTABLE__BLOCK_SIZE = "block-size";
  public static final String CREATE_MTABLE__BLOCK_SIZE__HELP =
      "Sets block size for the FTable records. Blocksize is used for storing number of records in one block. Default value is 1000. [Applicable only for type: IMMUTABLE]";

  public static final String CREATE_MTABLE__EVICTION_POLICY = "eviction-policy";
  public static final String CREATE_MTABLE__EVICTION_POLICY__HELP =
      "Eviction policy used for when heap space runs low.";
  public static final String CREATE_MTABLE__EVICTION_POLICY__ERROR =
      "An error occurred while creating the table";

  public static final String CREATE_MTABLE__EXPIRATION_TIMEOUT = "expiration-timeout";
  public static final String CREATE_MTABLE__EXPIRATION_TIMEOUT__HELP =
      "Time in seconds after which a row will get expired.";
  public static final String CREATE_MTABLE__EXPIRATION_TIMEOUT__ERROR =
      "An error occurred while creating the table";

  public static final String TIER1_TIME_TO_EXPIRE = "tier1-time-to-expire";
  public static final String TIER1_TIME_TO_EXPIRE__HELP =
      "Time in hours for tier1 after which a row will get expired.";

  public static final String TIER2_TIME_TO_EXPIRE = "tier2-time-to-expire";
  public static final String TIER2_TIME_TO_EXPIRE__HELP =
      "Time in hours for tier2 after which a row will get expired.";

  public static final String TIER1_TIME_PARTITION_INTERVAL = "tier1-time-partition-interval";
  public static final String TIER1_TIME_PARTITION_INTERVAL__HELP =
      "Time in hours for tier1 partitoning interval.";

  public static final String TIER2_TIME_PARTITION_INTERVAL = "tier2-time-partition-interval";
  public static final String TIER2_TIME_PARTITION_INTERVAL__HELP =
      "Time in hours for tier2 partitoning interval.";

  public static final String CREATE_MTABLE__OBSERVER_COPROCESSORS = "observer-coprocessors";
  public static final String OBSERVER_COPROCESSORS__HELP =
      "fully qualified class names of the observer coprocessors (comma separate if more than one)";
  public static final String COPROCESSORS__HELP__ERROR =
      "observer coprocessors are supported only with MTables";

  public static final String CREATE_MTABLE__CACHE_LOADER_CLASS = "cache-loader";
  public static final String CREATE_MTABLE__CACHE_LOADER_CLASS_HELP =
      "fully qualified class names of the cache loader class";
  public static final String CREATE_MTABLE__CACHE_LOADER_CLASS__ERROR =
      "observer coprocessors are supported only with MTables";


  public static final String CREATE_MTABLE__FTABLE_PARTITION_COLUMN_NAME =
      "partitioning-column-name";
  public static final String CREATE_MTABLE__FTABLE_PARTITION_COLUMN_NAME__HELP =
      "Set column name as String from schema that is used for partitioning the data ( FTable Specific )";

  public static final String CREATE_MTABLE__FTABLE_BLOCK_FORMAT = "block-format";
  public static final String CREATE_MTABLE__FTABLE_BLOCK_FORMAT__HELP =
      "Set the block-format for closed blocks. (FTable Specific)";

  public static final String CREATE_MTABLE__EXPIRATION_ACTION = "expiration-action";
  public static final String CREATE_MTABLE__EXPIRATION_ACTION__HELP =
      "The action that should take place when a row expires";
  public static final String CREATE_MTABLE__EXPIRATION_ACTION__ERROR =
      "An error occurred while creating the table";

  public static final String CREATE_TABLE__LOCAL_MAX_MEMORY = "local-max-memory";
  public static final String CREATE_TABLE__LOCAL_MAX_MEMORY__HELP =
      "Sets the maximum amount of memory, in megabytes, to be used by the table on a server. (Default: 90% of available heap)";
  public static final String CREATE_TABLE__LOCAL_MAX_MEMORY__INVALID =
      "Invalid value for local-max-memory: \"{0}\"";

  public static final String CREATE_TABLE__LOCAL_MAX_MEMORY_PCT = "local-max-memory-pct";
  public static final String CREATE_TABLE__LOCAL_MAX_MEMORY_PCT__HELP =
      "Sets the maximum amount of memory as a percentage of max heap, to be used by the table on a server. (Default: 90% of   available heap)";
  public static final String CREATE_TABLE__LOCAL_MAX_MEMORY_PCT__INVALID =
      "Invalid value for local-max-memory-pct: \"{0}\"";

  public static final String CREATE_MTABLE__ALREADY_EXISTS_ERROR = "Table \"{0}\" Already Exists";
  public static final String CREATE_MTABLE__IMMUTABLE_TYPE_FOR =
      "Parameter(s) \"{0}\" can be used only for creating an Immutable table.";
  public static final String CREATE_MTABLE__IMMUTABLE_TYPE_NOT =
      "Parameter(s) \"{0}\" can not be used for creating an Immutable table.";
  public static final String CREATE_MTABLE__UNORDERED_TYPE_NOT =
      "Parameter(s) \"{0}\" can not be used for creating an UnOrdered table.";

  /* 'delete table' command */
  public static final String DELETE_MTABLE = "delete table";
  public static final String DELETE_MTABLE__NAME = "name";
  public static final String DELETE_MTABLE__NAME__HELP = "Name of the table to be deleted.";
  public static final String DELETE_MTABLE__HELP = "delete a table";
  public static final String DELETE_MTABLE__ERROR_WHILE_DELETING_REASON_0 =
      "An error occurred while deleting the table: \"{0}\"";
  public static final String DELETE_MTABLE__SUCCESS = "Successfully deleted the table: \"{0}\"";

  /* 'describe table' command */
  public static final String DESCRIBE_MTABLE = "describe table";
  public static final String DESCRIBE_MTABLE__NAME = "name";
  public static final String DESCRIBE_MTABLE__NAME__HELP = "Name of the table to be described.";
  public static final String DESCRIBE_MTABLE__HELP = "describe a table";
  public static final String DESCRIBE_MTABLE__ERROR_WHILE_DESCRIBING_REASON_0 =
      "An error occurred while describing the table: \"{0}\"";
  public static final String INVALID_MTABLE_NAME = "Invalid table name";
  public static final String MTABLE_NOT_FOUND = "Table \"{0}\" not found";
  public static final String DESCRIBE_MTABLE__ATTRIBUTE__TYPE__REGION = "Table";
  public static final String DESCRIBE_MTABLE__ATTRIBUTE__TYPE__COLUMN = "Columns";
  public static final String DESCRIBE_MTABLE__ATTRIBUTE__TYPE__TYPE = "Type";
  public static final String DESCRIBE_MTABLE__ATTRIBUTE__TYPE__STATS = "Stats";

  /* 'show distribution' command */
  public static final String SHOW__DISTRIBUTION = "show distribution";
  public static final String SHOW__DISTRIBUTION__NAME = "name";
  public static final String SHOW__DISTRIBUTION__NAME__HELP = "Name of the table.";
  public static final String SHOW__DISTRIBUTION__HELP = "Shows distribution of key for the table";
  public static final String SHOW__DISTRIBUTION__INCLUDE_SECONDARY = "include-secondary-buckets";
  public static final String SHOW__DISTRIBUTION__INCLUDE_SECONDARY__HELP =
      "Show distribution for secondary buckets";
  public static final String SHOW__DISTRIBUTION__COL_NAME_SERVER_NAME = "Server";
  public static final String SHOW__DISTRIBUTION__COL_NAME_BUCKET_ID = "Bucket-id";
  public static final String SHOW__DISTRIBUTION__COL_NAME_KEY_COUNT = "Key-count";
  public static final String SHOW__DISTRIBUTION__PRIMARY = "Primary buckets information";
  public static final String SHOW__DISTRIBUTION__SECONDARY = "Secondary buckets information";

  /* 'list tier-stores' command */
  public static final String LIST_TIER_STORES = "list tier-stores";
  public static final String LIST_TIER_STORES__HELP = "Display tier stores of members.";
  public static final String LIST_TIER_STORES__GROUP = "group";
  public static final String LIST_TIER_STORES__GROUP__HELP =
      "Group of members for which tier stores will be displayed.";
  public static final String LIST_TIER_STORES__MEMBER = "member";
  public static final String LIST_TIER_STORES__MEMBER__HELP =
      "Name/Id of the member for which tier stores will be displayed.";
  public static final String LIST_TIER_STORES__MSG__NOT_FOUND = "No tier stores found";
  public static final String LIST_TIER_STORES__MSG__ERROR =
      "Error occurred while fetching list of tier stores.";

  /* 'list tables' command */
  public static final String LIST_MTABLE = "list tables";
  public static final String LIST_MTABLE__HELP = "Display tables of members.";
  public static final String LIST_MTABLE__GROUP = "group";
  public static final String LIST_MTABLE__GROUP__HELP =
      "Group of members for which tables will be displayed.";
  public static final String LIST_MTABLE__MEMBER = "member";
  public static final String LIST_MTABLE__MEMBER__HELP =
      "Name/Id of the member for which tables will be displayed.";
  public static final String LIST_MTABLE__MSG__NOT_FOUND = "No Tables Found";
  public static final String LIST_MTABLE__MSG__ERROR =
      "Error occurred while fetching list of tables.";

  /* show stats */
  public static final String SHOW_STATS = "show stats";
  public static final String SHOW_STATS__HELP =
      "Display or export stats for the entire distributed system, a member, a region or a table.";
  public static final String SHOW_STATS__REGION = "table";
  public static final String SHOW_STATS__REGION__HELP =
      "Name of the table whose metrics will be displayed/exported.";
  public static final String SHOW_STATS__MEMBER = "member";
  public static final String SHOW_STATS__MEMBER__HELP =
      "Name of the member whose metrics will be displayed/exported.";
  public static final String SHOW_STATS__CATEGORY = "categories";
  public static final String SHOW_STATS__CATEGORY__HELP =
      "Categories available based upon the parameters specified are:\n"
          + "- no parameters specified: cluster, cache, diskstore, query\n"
          + "- region specified: cluster, table, partition, diskstore, callback, eviction\n"
          + "- member specified: member, jvm, region, serialization, communication, function, transaction, diskstore, lock, eviction, distribution, offheap\n"
          + "- member and region specified: region, partition, diskstore, callback, eviction";
  public static final String SHOW_STATS__FILE = "file";
  public static final String SHOW_STATS__FILE__HELP =
      "Name of the file to which statistics will be written.";
  public static final String SHOW_STATS__ERROR = "Unable to retrieve statistics : {0} ";
  public static final String SHOW_STATS__TYPE__HEADER = "Category";
  public static final String SHOW_STATS__METRIC__HEADER = "Stat";
  public static final String SHOW_STATS__VALUE__HEADER = "Value";
  public static final String SHOW_STATS__CACHESERVER__PORT = "port";
  public static final String SHOW_STATS__CACHESERVER__PORT__HELP =
      "Port number of the Cache Server whose statistics are to be displayed/exported. This can only be used along with the --member parameter.";
  public static final String SHOW_STATS__CANNOT__USE__CACHESERVERPORT =
      "If the --port parameter is specified, then the --member parameter must also be specified.";
  public static final String SHOW_STATS__CACHE__SERVER__NOT__FOUND =
      "Statistics for the Cache Server with port : {0} and member : {1} not found.\n Please check the port number and the member name/id";

  /* table append command */
  public static final String APPEND = "tappend";
  public static final String APPEND__HELP = "Add a row in a FTable.";
  public static final String APPEND__MTABLENAME = "table";
  public static final String APPEND__MTABLENAME__HELP =
      "FTable into which the entry will be appended.";
  public static final String APPEND__VALUE_JSON = "value-json";
  public static final String APPEND__VALUE_JSON__HELP =
      "JSON text from which to create the row. Example: {'column_1':'123', 'column_2':['1','2','3'], 'column_3':{'c1':'value_1'}}";
  public static final String APPEND__MSG__MTABLENAME_EMPTY = "FTable name is either empty or Null";
  public static final String APPEND__ERROR_MSG = "Error while performing append";
  public static final String APPEND__SUCCESS_MSG = "Append completed successfully";
  public static final String APPEND__MSG__MTABLENAME_NOT_FOUND_ON_ALL_MEMBERS =
      "Immutable Table <{0}> not found in any of the members";

  /* table put command */
  public static final String MPUT = "tput";
  public static final String MPUT__HELP = "Add/Update a row in an MTable.";
  public static final String MPUT__KEY__HELP = "String to create the key.";
  public static final String MPUT__MTABLENAME = "table";
  public static final String MPUT__MTABLENAME__HELP = "MTable into which the entry will be put.";
  public static final String MPUT__VALUE__HELP =
      "Value is required for parameter \"value: Comma separated key=value pairs of column values\". Example: colum1=value1,column2=value2,column3=value3.\n"
          + "Only simple types are supported, values with \",\", \"=\" and \" \"(space) are not supported.";
  public static final String MPUT__VALUE_JSON = "value-json";
  public static final String MPUT__VALUE_JSON__HELP =
      "JSON text from which to create the row. Example: {'column_1':'123', 'column_2':['1','2','3'], 'column_3':{'key_1':'value_1'}}";
  public static final String MPUT__MSG___VALUE_EMPTY =
      "Value is either empty or Null. Either `value` or `value-json` must be provided.";
  public static final String MPUT__VALUE_JSON__HELP_1 =
      "Only one option `--value` or `--value-json` should be provided.";
  public static final String MPUT__MSG__MTABLENAME_EMPTY = "MTable name is either empty or Null";
  public static final String MPUT__ERROR_MSG =
      "Error while performing put for key : {0}. Error: {1}";
  public static final String MPUT__MSG__MTABLENAME_NOT_FOUND_ON_ALL_MEMBERS =
      "MTable <{0}> not found in any of the members";
  public static final String MPUT__TIMESTAMP = "timestamp";
  public static final String MPUT__TIMESTAMP__HELP =
      "TimeStamp for which the particular record has to be added";
  public static final String MPUT__SUCCESS_MSG = "Put completed successfully";

  /* table delete command */
  public static final String MDELETE = "tdelete";
  public static final String MDELETE__HELP =
      "Delete values of specified columns in a table. Entire row will be deleted if no columns are specified";
  public static final String MDELETE__KEY = "key";
  public static final String MDELETE__KEY__HELP = "Key of the entry to be deleted.";
  public static final String MDELETE__MTABLENAME = "table";
  public static final String MDELETE__MTABLENAME__HELP =
      "The table from which the entry will be deleted.";
  public static final String MDELETE_COLUMN = "column";
  public static final String MDELETE_COLUMN__HELP =
      "Value is required for parameter \"value: Comma separated column names\". Example: Age,Gender where Age and Gender are column names in the table";
  public static final String MDELETE__MSG__MTABLENAME_EMPTY = "Table name is either empty or Null";
  public static final String MDELETE__KEY__EMPTY = "Key is either empty or Null";
  public static final String MDELETE__ERROR_MSG =
      "Error while performing delete for key : {0}. Error: {1}";
  public static final String MDELETE__TIMESTAMP = "timestamp";
  public static final String MDELETE__TIMESTAMP__HELP =
      "TimeStamp for which the particular record has to be deleted";

  /* table get command */
  public static final String MGET = "tget";
  public static final String MGET__HELP = "Display an entry in an MTable.";
  public static final String MGET__KEY__HELP = "String to create the key";
  public static final String MGET__MTABLENAME = "table";
  public static final String MGET__MTABLENAME__HELP = "MTable from which to get the entry.";
  public static final String MGET__MSG__MTABLENAME_EMPTY = "MTable name is either empty or Null";
  public static final String MGET__ERROR_MSG =
      "Error while performing get for key : {0}. Error: {1}";
  public static final String MGET__ERROR_NOT_FOUND = "Key \"{0}\" not found";
  public static final String MGET__TIMESTAMP = "timestamp";
  public static final String MGET__TIMESTAMP__HELP =
      "TimeStamp for which the particular record has to be retrieved";
  public static final String MGET__KEY__EMPTY = "Key is either empty or Null";

  /* table scan command */
  public static final String MSCAN = "tscan";
  public static final String MSCAN__HELP =
      "Returns rows from the given table within given startkey and endkey.";
  public static final String MSCAN__MTABLENAME = "table";
  public static final String MSCAN__START_KEY = "startkey";
  public static final String MSCAN__START_KEY_HELP =
      "startkey for the scan for MTable. Not applicable for FTable and UNORDERED table";
  public static final String MSCAN__MSG_INVALID_KEY = "Invalid <{0}> provided.";
  public static final String MSCAN__MSG_INVALID_KEY_BINARY =
      "Invalid <{0}> provided. Key must be sequence of valid bytes separated by comma.";
  public static final String MSCAN__END_KEY = "stopkey";
  public static final String MSCAN__END_KEY_HELP =
      "stopkey for the scan for MTable. Not applicable for FTable and UNORDERED table";
  public static final String MSCAN__KEY_TYPE = "key-type";
  public static final String MSCAN__MAX_LIMIT = "max-limit";
  public static final String MSCAN__MSG_INVALID_KEY_TYPE =
      "Invalid value provided for key-type. It must be one of STRING/BINARY";
  public static final String MSCAN__MSG_KEY_TYPE_HELP =
      "The type to be used to convert start/stop key to byte-array. Default is STRING. Using BINARY will convert comma separated bytes into byte-array.";
  public static final String MSCAN__MSG_MAX_LIMIT_HELP =
      "Max number of records to be retrieved from a table.";
  public static final String MSCAN__MTABLENAME__HELP =
      "The table from which entries are to be fetched.";
  public static final String MSCAN__MSG__MTABLENAME_EMPTY = "Table name is either empty or Null";
  public static final String MSCAN__MSG__MTABLENAME_NOT_FOUND_ON_ALL_MEMBERS =
      "Table <{0}> not found in any of the members";
  public static final String MSCAN__MSG__TYPE_NOT =
      "Parameter(s) \"{0}\" can not be used while scanning UNORDERED or IMMUTABLE table.";
  public static final String EXPORT_DATA__FILE = "file";
  public static final String EXPORT_DATA__FILE__HELP =
      "File to which the exported data will be written. The file must have an extension of \".csv\".";
  public static final String MTABLE_DATA_FILE_EXTENSION = ".csv";
  public static final String INVALID_FILE_EXTENTION =
      "Invalid file type, the file extension must be \"{0}\"";
  public static final String FILE_EXISTS =
      "File \"{0}\" already exists, please specify some other file or delete existing file. ";
  public static final String MSCAN__EXCCEPTION = "Failed to execute " + MSCAN + " command.";

  /* MTable/ FTable hints Description */
  public static final String TOPIC_MTABLE__DESC =
      "Ampool tables are the core building block of the Ampool distributed system. "
          + "In an MTable, data is organized into a collection of columns based on a schema, and all data operations (put, get, scan and delete) are allowed.";
  public static final String TOPIC_FTABLE__DESC =
      "Ampool tables are the core building block of the Ampool distributed system. "
          + "In a FTable, which is an immutable table type, data is organized into a collection of columns based on a schema, however, only append, scan and delete operations are allowed.";

  /* start server */
  public static final String AMPOOL_PROPERTIES_0_NOT_FOUND_MESSAGE =
      "Warning: The Ampool properties file {0} could not be found.";
  public static final String START_AMPOOL_SERVER__PROPERTIES = "ampool-server-launch-args";
  public static final String START_AMPOOL_SERVER__PROPERTIES__HELP =
      "The ampool_server_launch.properties file for configuring the Cache Server's distributed system. The file's path can be absolute or relative to the MASH working directory. Argument passed in the file on which the server will run. For example, -Dfoo.bar=true will set the system property \"foo.bar\" to \"true\".\"";

  public static final String CREATE_TIERSTORE = "create tier-store";
  public static final String CREATE_TIERSTORE__HELP =
      "Creates a tier store which can be used to store Ftable data after eviction from memory";
  public static final String CREATE_TIERSTORE__NAME = "name";
  public static final String CREATE_TIERSTORE__NAME__HELP = "Name of the tier store";
  public static final String CREATE_TIERSTORE__HANDLER = "handler";
  public static final String CREATE_TIERSTORE__HANDLER__HELP =
      "Fully qualified class name of the class that implements the tier store interface for this store";
  public static final String CREATE_TIERSTORE__PROPSFILE = "props-file";
  public static final String CREATE_TIERSTORE__PROPSFILE__HELP = "Tier store specific properties";
  public static final String CREATE_TIERSTORE__READER = "reader";
  public static final String CREATE_TIERSTORE__READER__HELP =
      "Fully qualified name of the class which implements the TierStoreReader interface";
  public static final String CREATE_TIERSTORE__READEROPTS = "reader-opts";
  public static final String CREATE_TIERSTORE__READEROPTS__HELP = "Reader specific options";
  public static final String CREATE_TIERSTORE__WRITER = "writer";
  public static final String CREATE_TIERSTORE__WRITER__HELP =
      "Fully qualified name of the class which implements the TierStoreWriter interface";
  public static final String CREATE_TIERSTORE__WRITEROPTS = "writer-opts";
  public static final String CREATE_TIERSTORE__WRITEROPTS__HELP = "Writer specific options";
  public static final String CREATE_TIERSTORE__SUCCESS = "Successfully created tier store: ";
  public static final String CREATE_TIERSTORE__FAILURE = "Failed to create tier store: ";
  public static final String DESCRIBE_TIER_STORE = "describe tier-store";
  public static final String DESCRIBE_TIER_STORE__HELP = "Get details about a tier store";
  public static final String DESCRIBE_TIER_STORE__NAME = "name";
  public static final String DESCRIBE_TIER_STORE__NAME__HELP = "Name of the tier store";
  public static final String DESTROY_TIER_STORE__HELP = "destroys a tier store";
  public static final String DESTROY_TIER_STORE__NAME = "name";
  public static final String DESTROY_TIER_STORE__NAME__HELP =
      "Name of the tier store to be destroyed";
  public static final String DESTROY_TIER_STORE__MSG__SUCCESS = "Tier store destroyed";
  public static String DESCRIBE_TIER_STORE__MSG__ERROR = "Failed to get info about tier store";
  public static final String DESTROY_TIER_STORE = "destroy tier-store";
  public static String DESTROY_TIER_STORE__MSG__ERROR = "Failed to destroy tier store";
  public static String DESTROY_TIER_STORE__DEFAULT_STORE_MSG__ERROR =
      DefaultStore.STORE_NAME + " is a default Ampool Tier Store and hence cannot be detroyed";

  public static final String AMPOOL_AUTHZ__SECURITY_PROPERTIES = "security-properties-file";
  public static final String AMPOOL_AUTHZ__SECURITY_PROPERTIES__HELP =
      "Path to security properties file to be used for the operation";

  public static final String CREATE_ROLE = "create role";
  public static final String CREATE_ROLE__HELP = "Create a role in authorization store";
  public static final String CREATE_ROLE__ERROR = "Failed to create role ";
  public static final String CREATE_ROLE__ROLE_NAME = "role-name";
  public static final String CREATE_ROLE__ROLE_NAME__HELP = "Name of the new role";
  public static final String CREATE_ROLE__SUCCESS = "Role create successfully: ";

  public static final String DROP_ROLE = "drop role";
  public static final String DROP_ROLE__HELP = "Drop a role from authorization store";
  public static final String DROP_ROLE__ERROR = "Failed to drop role ";
  public static final String DROP_ROLE__ROLE_NAME = "role-name";
  public static final String DROP_ROLE__ROLE_NAME__HELP = "Name of the role to be dropped";
  public static final String DROP_ROLE__SUCCESS = "Role dropped successfully: ";

  public static final String GRANT_ROLE = "grant role";
  public static final String GRANT_ROLE__HELP = "Grant a role to groups";
  public static final String GRANT_ROLE__ERROR = "Failed to grant role ";
  public static final String GRANT_ROLE__ROLE_NAME = "role-name";
  public static final String GRANT_ROLE__ROLE_NAME__HELP = "Role name to be granted";
  public static final String GRANT_ROLE__GROUPS = "groups";
  public static final String GRANT_ROLE__GROUPS__HELP =
      "List of groups to which role is to be granted, separated with comma(,)";
  public static final String GRANT_ROLE__SUCCESS = "Role granted successfully: ";

  public static final String REVOKE_ROLE = "revoke role";
  public static final String REVOKE_ROLE__HELP = "Revoke a role from groups";
  public static final String REVOKE_ROLE__ERROR = "Failed to revoke role ";
  public static final String REVOKE_ROLE__ROLE_NAME = "role-name";
  public static final String REVOKE_ROLE__ROLE_NAME__HELP = "Role name to be revoked";
  public static final String REVOKE_ROLE__GROUPS = "groups";
  public static final String REVOKE_ROLE__GROUPS__HELP =
      "List of groups from which role is to be revoked, separated with comma(,)";
  public static final String REVOKE_ROLE__SUCCESS = "Role revoked successfully: ";

  public static final String GRANT_PRIVILEGE = "grant privilege";
  public static final String GRANT_PRIVILEGE__HELP = "Grant a privilege to a role";
  public static final String GRANT_PRIVILEGE__ERROR = "Failed to grant privilege ";
  public static final String GRANT_PRIVILEGE__ROLE_NAME = "role-name";
  public static final String GRANT_PRIVILEGE__ROLE_NAME__HELP = "Name of the role";
  public static final String GRANT_PRIVILEGE__PRIVILEGE = "privilege";
  public static final String GRANT_PRIVILEGE__PRIVILEGE__HELP =
      "Privilege to be granted, it should be of the form <CLUSTER|DATA>:<READ|WRITE|MANAGE>:<REGION>:<KEY>";
  public static final String GRANT_PRIVILEGE__SUCCESS = "Privilege granted successfully: ";

  public static final String REVOKE_PRIVILEGE = "revoke privilege";
  public static final String REVOKE_PRIVILEGE__HELP = "Revoke a privilege from a role";
  public static final String REVOKE_PRIVILEGE__ERROR = "Failed to revoke privilege ";
  public static final String REVOKE_PRIVILEGE__ROLE_NAME = "role-name";
  public static final String REVOKE_PRIVILEGE__ROLE_NAME__HELP = "Name of the role";
  public static final String REVOKE_PRIVILEGE__PRIVILEGE = "privilege";
  public static final String REVOKE_PRIVILEGE__PRIVILEGE__HELP =
      "Privilege to be revoke, it should be of the form <CLUSTER|DATA>:<READ|WRITE|MANAGE>:<REGION>:<KEY>";
  public static final String REVOKE_PRIVILEGE__SUCCESS = "Privilege revoked successfully: ";

  public static final String LIST_ROLES = "list roles";
  public static final String LIST_ROLES__HELP = "List the roles assigned to a group";
  public static final String LIST_ROLES__ERROR = "Failed to list role ";
  public static final String LIST_ROLES__GROUP = "group";
  public static final String LIST_ROLES__GROUP__HELP =
      "Group for which the roles have to be listed";
  public static final String LIST_ROLES__SUCCESS = "";

  public static final String LIST_PRIVILEGES = "list privileges";
  public static final String LIST_PRIVILEGES__HELP = "List the privileges assigned to a role";
  public static final String LIST_PRIVILEGES__ERROR = "Failed to list privileges ";
  public static final String LIST_PRIVILEGES__ROLE_NAME = "role-name";
  public static final String LIST_PRIVILEGES__ROLE_NAME__HELP = "Name of the role";
  public static final String LIST_PRIVILEGES__SUCCESS = "";

  public static final String INVALIDATE_PRIVILEGE_CACHE = "invalidate privilege cache";
  public static final String INVALIDATE_PRIVILEGE_CACHE__HELP =
      "Invalidate the cached privileges in Ampool processes";
  public static final String INVALIDATE_PRIVILEGE_CACHE__ERROR = "Failed to invalidate privileges ";
  public static final String INVALIDATE_PRIVILEGE_CACHE__SUCCESS = "Invalidated privileges cache";

  /* start server */
  public static final String AMPOOL_0_PROPERTIES_1_NOT_FOUND_MESSAGE =
      "Warning: The Ampool {0}properties file {1} could not be found.";



  /**
   * Creates a MessageFormat with the given pattern and uses it to format the given argument.
   *
   * @param pattern the pattern to be substituted with given argument
   * @param argument an object to be formatted and substituted
   * @return formatted string
   * @throws IllegalArgumentException if the pattern is invalid, or the argument is not of the type
   *         expected by the format element(s) that use it.
   */
  public static String format(String pattern, Object argument) {
    return format(pattern, new Object[] {argument});
  }

  /**
   * Creates a MessageFormat with the given pattern and uses it to format the given arguments.
   *
   * @param pattern the pattern to be substituted with given arguments
   * @param arguments an array of objects to be formatted and substituted
   * @return formatted string
   * @throws IllegalArgumentException if the pattern is invalid, or if an argument in the
   *         <code>arguments</code> array is not of the type expected by the format element(s) that
   *         use it.
   */
  public static String format(String pattern, Object... arguments) {
    return MessageFormat.format(pattern, arguments);
  }


  public static final String IGNORE_INTERCEPTORS = "ignoreInterCeptors";

}
