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

#!/bin/bash

# check for java in path and version should be 1.8
# exeutable permission for start script

##Util functions
help(){
    echo "---------------------------------------------------------------------------------"
    echo "1. To stop locator:"
    echo "   stop_ampool.sh -s locator -n <locator_name> -l <locator_host[port]>"
    echo "2. To stop server:"
    echo "   stop_ampool.sh -s server -n <server_name> -l <locator_host[port]>"
    echo "3. Help:"
    echo "   stop_ampool.sh -h"
    echo "---------------------------------------------------------------------------------"
    exit 1
}

# java version check
if type -p java > /dev/null; then
    #echo "found java executable in PATH"
    _java=java
elif [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
    #echo "found java executable in JAVA_HOME"
    _java="$JAVA_HOME/bin/java"
else
    echo "No java found in PATH."
    exit 1
fi

if [[ "$_java" ]]; then
    version=$("$_java" -version 2>&1 | awk -F '"' '/version/ {print $2}')
    #echo "version "$version""
    if [[ "$version" < "1.8" ]]; then
        echo "Required java 1.8 or more"
        exit 1
    fi
fi
# java version check

if [ "$#" -lt 2 ]; then
        help
fi

SERVICE="locator"
SERVICE_NAME="service1"
SERVICE_PORT=10335
LOCATOR_PORT=10334
LOCATOR_ADD="localhost[$LOCATOR_PORT]"
OPERATION="stop"

isService=false
isName=false
isLocatorAdd=false
isServicePort=false

#parse arguments
# -s Service type. Whether to start <locator|server>
# -n Service name. Any name to locator/server
# -p Port. Port service should bind to. Optional. Default locator port : 10334 Default server port : 10335
# -l Locator address. Applicable in case of starting server. Valid locator address with port format : "hostname[port]"
# -o Operation. <start|stop>

while getopts :s:n:p:l:h opt; do
    case $opt in
        s) # Get service
        SERVICE=$OPTARG
        isService=true
        ;;
        n) # Get service name
        SERVICE_NAME=$OPTARG
        isName=true
        ;;
        l) # Get locator address
        LOCATOR_ADD=$OPTARG
        isLocatorAdd=true
        ;;
        p) # Get port
        SERVICE_PORT=$OPTARG
        isServicePort=true
        ;;
        h) # Print help
        help
        ;;
    esac
done
shift $((OPTIND-1))

# End of getopts

JMX_MANAGER_SETTINGS='--J=-Dgemfire.jmx-manager=true --J=-Dgemfire.jmx-manager-start=true'
GEODE_HOME=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

if [ "$SERVICE" = "locator" ]; then
    #Locator specific code
    if [ "$OPERATION" = "start" ]; then
        if [ "$isName" = "false" ]; then
            echo "Insufficient arguments."
            help
        fi
        echo "Starting locator"
        $GEODE_HOME/mash -e "start locator --name=$SERVICE_NAME --port=$SERVICE_PORT"
        # End of start locator
    elif [ "$OPERATION" = "stop" ]; then
        if [ "$isName" = "false" ] || [ "$isLocatorAdd" = "false" ]; then
            echo "Insufficient arguments."
            help
        fi
        echo "Stopping locator"
        $GEODE_HOME/mash -e "connect --locator=$LOCATOR_ADD" -e "stop locator --name=$SERVICE_NAME"
    else
        echo "Unsupported operation $OPERATION"
        exit 1
    fi
elif [ "$SERVICE" = "server" ]; then
    # server specific code
    if [ "$isName" = "false" ] || [ "$isLocatorAdd" = "false" ]; then
            echo "Insufficient arguments."
            help
    fi
    if [ "$OPERATION" = "start" ]; then
        echo "Starting server"
        $GEODE_HOME/mash -e "start server --name=$SERVICE_NAME  --server-port=$SERVICE_PORT --locators=$LOCATOR_ADD"
        # End of start locator
    elif [ "$OPERATION" = "stop" ]; then
        echo "Stopping server"
        $GEODE_HOME/mash -e "connect --locator=$LOCATOR_ADD" -e "stop server --name=$SERVICE_NAME"
    else
        echo "Unsupported operation $OPERATION"
        exit 1
    fi
else
    echo "Unexpected service type $SERVICE"
    exit 1
fi
