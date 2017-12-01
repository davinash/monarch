#/*
#* Copyright (c) 2017 Ampool, Inc. All rights reserved.
#*
#* Licensed under the Apache License, Version 2.0 (the "License"); you
#* may not use this file except in compliance with the License. You
#* may obtain a copy of the License at
#*
#* http://www.apache.org/licenses/LICENSE-2.0
#*
#* Unless required by applicable law or agreed to in writing, software
#* distributed under the License is distributed on an "AS IS" BASIS,
#* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
#* implied. See the License for the specific language governing
#* permissions and limitations under the License. See accompanying
#* LICENSE file.
#*/

#!/usr/bin/env bash
# this is to exit shell script when fails
set -e
#set -o pipefail

##############################################################################
##
##  Ampool examples executor
##
##############################################################################

help ( ) {
    echo "Executes Ampool exmaples with default dependency set"
    echo "runExamples.sh -i <Ampool Installation directory> [-l <Running Locator Hostname>] [-p <Running Locator Port>]"

    echo ""
    echo "This script required MVN. And all default ports should be open"
}

sucess=false
LOG_FILE=/tmp/runExamples.log

# build dir based on build env
target_prefix="target"
target_jar_prefix="."

echo "Executing Ampool Examples " > $LOG_FILE 2>&1

killProcs(){
    ps -aef | grep $1 | grep -v grep | awk '{print $2}' | xargs kill -9
}

red () {
    if [ "$sucess" = "false" ]; then
        echo "Log File: $LOG_FILE"
        tput setaf 1;
        echo "$1"
        tput sgr0;
        # removing working dir
        rm -rf $DOWNLOAD_DIR
        exit 1;
    fi
        # removing working dir
        rm -rf $DOWNLOAD_DIR
}

green (){
    tput setaf 2;
    echo "$1"
    tput sgr0;
}

error_msg="Running MTable Examples failed"
trap 'red "${error_msg}"' EXIT

command_exists () {
    type "$1" &> /dev/null ;
}

start_ampool ()
{
    echo "Starting Ampool services"
    BASE_DIR=$1
    CP=$4
    $BASE_DIR/bin/start_ampool.sh -s locator -n Locator1
    $BASE_DIR/bin/start_ampool.sh -s server -n Server1 -l $2[$3] -c $CP
}

stop_ampool ()
{
    echo "Stopping Ampool services"
    $AMPOOL_DIR/bin/stop_ampool.sh -s server -n Server1 -l $2[$3]
    $AMPOOL_DIR/bin/stop_ampool.sh -s locator -n Locator1 -l $2[$3]
}

run_example ()
{
    AMPOOL_EXAMPLE_DIR=$1
    CLASS_NAME=$2
    LOCATOR_HOST=$3
    LOCATOR_PORT=$4
    SUC_MSG=$5
    EXTN_CLASSPATH=$6
    echo "------------------------------------------------------------------------------------------------"
    tput setaf 4;
    java -cp $AMPOOL_EXAMPLE_DIR/$target_prefix/lib/*:$AMPOOL_EXAMPLE_DIR/$target_prefix/$target_jar_prefix/ampool-examples-$AMPOOL_VERSION.jar:$EXTN_CLASSPATH $CLASS_NAME $LOCATOR_HOST $LOCATOR_PORT
    tput sgr0
    green "$SUC_MSG"
    echo "------------------------------------------------------------------------------------------------"
}

# parse arguments
# -i Ampool Installation dir
# -p Port. Locator service port to connect. Optional. DEFAULT : 10334
# -l Hostname. Locator hostname to connect. Optional. DEFAULT : localhost
# -s Should this script start Ampool service(s) DEFAULT : false

isAmpoolDir=false
isstart=true
isLocatorAdd=false
isServicePort=false

LOCATOR_ADD=localhost
SERVICE_PORT=10334

AMPOOL_VERSION=1.0.0-SNAPSHOT
EXAMPLE_SET=1

DOWNLOAD_DIR=/tmp/downloads

while getopts :i:p:l:h opt;
do
    case $opt in
        i) # Get Ampool dir
          AMPOOL_DIR="$OPTARG"
         isAmpoolDir=true
          ;;
        l) # Get locator address
          LOCATOR_ADD="$OPTARG"
          isLocatorAdd=true
          ;;
        p) # Get port
          SERVICE_PORT="$OPTARG"
          isServicePort=true
          ;;
        h) # Print help
          help
          exit 1
        ;;
    esac
done
shift $((OPTIND-1))

# check for ampool installation details
if [ "$isAmpoolDir" = "false" ]; then
    echo "Missing ampool installation directory"
    help
    exit 1;
fi
if [ ! -d "$AMPOOL_DIR" ]; then
    echo "Invalid installation directory $AMPOOL_DIR"
    help
    exit 1
fi

# build examples
EXAMPLE_DIR="$( cd "$( dirname "${BASH_SOURCE-$0}" )" && pwd )"
cd $EXAMPLE_DIR

echo "Building examples.."
if command_exists mvn ; then
    mvn clean package >>$LOG_FILE 2>&1
elif command_exists gradle ; then
    gradle clean build >>$LOG_FILE 2>&1
    target_prefix="build"
    target_jar_prefix="libs"
fi

#start locator and server with example jar in classpath for co-processor
if [ "true" = "$isstart" ];
then
    echo "Starting Ampool.."
    start_ampool $AMPOOL_DIR $LOCATOR_ADD $SERVICE_PORT "$EXAMPLE_DIR/$target_prefix/$target_jar_prefix/ampool-examples-$AMPOOL_VERSION.jar:$AMPOOL_DIR/lib-tier/*:$AMPOOL_DIR/lib-tier-dependencies/*" >>$LOG_FILE 2>&1
fi

echo "Running Ampool MTable Examples.."
# Running MTable client set
# Run Basic mtable client example
run_example $EXAMPLE_DIR "io.ampool.examples.mtable.MTableClient" $LOCATOR_ADD $SERVICE_PORT "Basic MTable client example completed successfully"

# Run Multiversion MTable client example
run_example $EXAMPLE_DIR "io.ampool.examples.mtable.MTableMultiVersionTableExample" $LOCATOR_ADD $SERVICE_PORT "MTable multi-versioned scan example completed successfully"

# Run Multiversion MTable client example for Unordered Table
run_example $EXAMPLE_DIR "io.ampool.examples.mtable.MTableMultiVersionUnorderedTableExample" $LOCATOR_ADD $SERVICE_PORT "MTable Unordered multi-versioned scan example completed successfully"

# Run co-processor example
run_example $EXAMPLE_DIR "io.ampool.examples.mtable.MTableCoprocessorExample" $LOCATOR_ADD $SERVICE_PORT "MTable with co-processor example completed successfully"

# Run MTable with Columns example
run_example $EXAMPLE_DIR "io.ampool.examples.mtable.MTableClientWithColumnTypes" $LOCATOR_ADD $SERVICE_PORT "MTable with various datatypes example completed successfully"

# Run MTable with Columns example
run_example $EXAMPLE_DIR "io.ampool.examples.mtable.MTableClientOrderedTable" $LOCATOR_ADD $SERVICE_PORT "MTable ordered example completed successfully"

# Run MTable with Columns example
run_example $EXAMPLE_DIR "io.ampool.examples.mtable.MTableClientUnorderedTable" $LOCATOR_ADD $SERVICE_PORT "MTable unordered example completed successfully"

# Run MTable with Columns example
run_example $EXAMPLE_DIR "io.ampool.examples.mtable.MTablePersistenceExample" $LOCATOR_ADD $SERVICE_PORT "MTable persistence example completed successfully"

# Run MTable Aggregation client example
run_example $EXAMPLE_DIR "io.ampool.examples.mtable.MTableAggregationClient" $LOCATOR_ADD $SERVICE_PORT "MTable aggregation client example completed successfully"

# Run MTable Observer Coprocessor example
run_example $EXAMPLE_DIR "io.ampool.examples.mtable.MTableObserverCoprocessor" $LOCATOR_ADD $SERVICE_PORT "MTable Observer Coprocessor example completed successfully"

# Run MTable CDC Listener example
run_example $EXAMPLE_DIR "io.ampool.examples.mtable.MTableCDCExample" $LOCATOR_ADD $SERVICE_PORT "MTable CDC example completed successfully"

# Run MTable CDC Listener example
run_example $EXAMPLE_DIR "io.ampool.examples.mtable.MTableCDCAsParquetFileExample" $LOCATOR_ADD $SERVICE_PORT "MTable CDC events as Parquet file example completed successfully"

# Run MTable CDC Listener example
run_example $EXAMPLE_DIR "io.ampool.examples.mtable.cdc.MTableCDCEndToEndExample" $LOCATOR_ADD $SERVICE_PORT "MTable CDC example completed successfully"


# Run MTable DeleteWithFiltersExample
run_example $EXAMPLE_DIR "io.ampool.examples.mtable.DeleteWithFiltersExample" $LOCATOR_ADD $SERVICE_PORT "MTable DeleteWithFiltersExample completed successfully"


echo "Running Ampool FTable Examples.."
# Running FTable client set

## Run Basic ftable client example
run_example $EXAMPLE_DIR "io.ampool.examples.ftable.FTableExample" $LOCATOR_ADD $SERVICE_PORT "Basic FTable client example completed successfully"

# Run Basic ftable client example
run_example $EXAMPLE_DIR "io.ampool.examples.ftable.FTableTypesExample" $LOCATOR_ADD $SERVICE_PORT "FTable client example with types completed successfully"

# Run Basic ftable client example
run_example $EXAMPLE_DIR "io.ampool.examples.ftable.FTableScanWithPartialColumnsExample" $LOCATOR_ADD $SERVICE_PORT "FTable client example with partial columns scan completed successfully"

# Run Basic ftable client example
run_example $EXAMPLE_DIR "io.ampool.examples.ftable.FTableScanWithFilters" $LOCATOR_ADD $SERVICE_PORT "FTable client example with filtered scan completed successfully"



if [ "true" = "$isstart" ];
then
    echo "Stopping Ampool.."
    stop_ampool $AMPOOL_DIR $LOCATOR_ADD $SERVICE_PORT >>$LOG_FILE 2>&1
fi

green "All examples are running"
sucess=true
