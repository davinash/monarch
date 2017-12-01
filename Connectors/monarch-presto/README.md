# Monarch Presto Connector
A Distributed SQL parser to access, describe, and query monarch tables using PrestoDB.

## How to Use

Untar the package **monarch-presto-<connector_version>.tar.gz** and you should have the following:

```
monarch-presto-<connector_version>
├── dependencies
│   ├── monarch-<version>.jar
│   ├── guava-<version>.jar
│   ├── fastutil-<version>.jar
│   ├── javax.transaction-api-<version>.jar
│   ├── log4j-api-<version>.jar
│   ├── log4j-core-<version>.jar 
│   └── shiro-core-<version>.jar
├── lib
│   └── monarch-presto-<connector_version>.jar
├── etc
│   └── catalog
│   	     └── ampool.properties
├── README.md
└── AmpoolEULA.pdf
```
The *lib* directory contains the jar file for this connector and *dependencies* directory contains all dependent libraries.

## Pre-requisites
* Presto server [_version 0.189 or latest_]
* Presto CLI [_version 0.189 or latest_]
* Monarch 1.0.0-SNAPSHOT or latest
* JDK 1.8

## Monarch presto connector properties

| Name | Description | Default |
|------------|-------------| -------------|
| monarch.client.log    | Monarch client log file path| /tmp/presto-monarch-client.log |
| locator.host       | Host name or IP address of monarch locator. | localhost |
| locator.port      | Port of monarch locator | 10334 |

## Configuring presto connector
 1. Configure [presto cluster](https://prestodb.io/docs/current/installation/deployment.html).
 2. Setup [presto CLI](https://prestodb.io/docs/current/installation/cli.html).
 3. Setup and start [monarch cluster](http://docs.ampool-inc.com/w_MASH/).
 4. Create `monarch` directory in the `plugin` folder contained in the presto server package.
 5. Copy all jars from `dependencies` and `lib` directory from extracted monarch-presto package into `monarch` directory. 
 6. To add monarch connector in presto catalog, copy `etc/catalog/monarch.properties` to `etc/catalog/` directory in presto server package. 
 7. Value of `connector.name` from `etc/catalog/monarch.properties` must be `ampool`. Other can be changed according to configured setup.
 8. [Start presto server](https://prestodb.io/docs/current/installation/deployment.html#running-presto).
 9. [Start presto CLI](https://prestodb.io/docs/current/installation/cli.html) passing `--catalog` as `ampool` and `--schema` as `ampool`.
   
## Using presto connector
 1. After configuring presto connector, we can execute queries from presto CLI.
 2. Create and populate table in monarch cluster.
 3. Launch CLI 
  ```
    presto --catalog monarch --schema ampool
  ```
 4. List tables
  ```
    presto:ampool> show tables;
  ```
 5. List columns
  ```
      presto:ampool> show columns from <table name>;
  ```
 6. Describe table
  ```
      presto:ampool> describe <table name>;
  ```
 7. Select rows from table
   ```
       presto:ampool> select * from <table name>;
   ```
 8. Similarly we can run presto supported queries over Monarch.
 
## Limitation
 1. Presto connector works only with small-case table name.
