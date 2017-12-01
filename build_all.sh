#! /bin/bash
set -e

echo "Building all componenets. Skipping RAT"

echo "1. Building Ampool OSS (Monarch)"
cd ADS
./gradlew clean build -Dskip.tests=true
./gradlew install
cd ../

echo "2. Building connectors"
cd Connectors
mvn clean install -DskipTests

echo "Done."
