#!/usr/bin/env bash

########## environment variables..
# AMPOOL home is must..
if [ x"$AMPOOL_HOME" = x ]
then
  echo "AMPOOL_HOME must be set to correct location."
  exit 1
fi

# Environment variables..
MASH=$AMPOOL_HOME/bin/mash


# required jar files: monarch-dependencies
AMPOOL_JAR_FILE=${AMPOOL_JAR:-$AMPOOL_HOME/lib/monarch-dependencies.jar}


# execute the main class and pass the arguments..
function __main__() {
  MAIN_CLASS="io.ampool.utils.MTableExportImport"
  CP="$AMPOOL_JAR_FILE"

  echo "Using class-path: $CP"

  # execute the command.
  java -cp $CP $MAIN_CLASS $*
}

# execute main
__main__ $*
