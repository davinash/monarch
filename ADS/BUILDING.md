# Building this Release from Source

All platforms require a Java installation, with JDK 1.8 or more recent version.

## Build from Source on Unix

1. Set the JAVA\_HOME environment variable.  For example:

    ```     
    JAVA_HOME=/usr/java/jdk1.8.0_60
    export JAVA_HOME
    ```
2. Download the project source from the Releases page at [Apache Geode] (http://geode.apache.org), and unpack the source code.
3. Within the directory containing the unpacked source code, build without the tests:
    
    ```
    $ ./gradlew build -Dskip.tests=true
    ```
Or, build with the tests:
   
    ```
    $ ./gradlew build
    ```
The built binaries will be in `geode-assembly/build/distributions/`,
or the `mash` script can be found in
`geode-assembly/build/install/monarch/bin/`.
4. Verify the installation by invoking `mash` to print version information and exit:
   
    ```
    $ mash version
    1.0.0-SNAPSHOT
    ```