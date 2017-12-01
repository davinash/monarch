**[Overview](#overview)** 
**[Documentation](#Documentation)** 
**[Contributing](#Contributing)** 


## <a name="overview"></a>Overview

Monarch is a robust, in-memory computing platform that provides operational intelligence to both user (external) apps and data science driven analytics (internal) apps. 
It is based on [Apache Geode] (http://geode.apache.org/), adding several capabilities for analytical data workloads.

## <a name="Documentation"></a>Documentation
Please visit the [documentation section](http://docs.ampool-inc.com/).

## <a name="Contributing"></a>Contributing

This project welcomes new contributors. If you would like to help by adding new features, enhancements or fixing bugs, check out the contributing guidelines.

You acknowledge that your submissions to this repository are made pursuant the terms of the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0.html) and constitute "Contributions," as defined therein, and you represent and warrant that you have the right and authority to do so.
You can reach us on the [group](https://groups.google.com/forum/#!forum/ampool-users).


## <a name="Downloading and Installing Monarch"></a>Downloading and Installing Monarch
You can download and install the latest version of Monarch from the Monarch Release page. 
Refer to the documentation for installation steps.

If you would like to build Monarch from source, refer to the documentation on building from source.


## <a name="Monarch in 5 Minutes!"></a>Monarch in 5 Minutes!

NOTE: In the document <ampool-home> refers to the full directory path in which Monarch s/w is installed.

#### 1. Run Example script
Go to <ampool-home>/examples directory and run the runExamples.sh script
```
$ ./examples/runExamples.sh -i <ampool_home>
```

At the end of the running you should see the following message:

```
Stopping Ampool
All examples are running
$
```
This implies the installation is correctly done, else please contact the support@ampool.io

#### 2. An Overview of Ampool System
Ampool System/Cluster for all occurrences is based on a locator-server configuration of Apache Geode, referred here as Ampool cluster. Thus an Ampool cluster consist of one or more locators and one or more servers connected in certain topology.
##### 2.1. Locator:
Locator is an important component of Ampool cluster. Locator maintains and provides up to date topology information to clients and servers.
Locator guides clients to servers for data. There can be more than one locator in a cluster. Multiple locators in a cluster are recommended to insure continued availability in case a locator machine failure.
##### 2.2. Server:
Servers hold and maintain the data for a table, which internally is split into a number of buckets based on partition configuration. Servers communicate to maintain bucket balance and consistency.


#### 3. Ampool Cluster Management and Monitoring Tool
Ampool provides an interactive command line interface for the Ampool cluster management and monitoring called MASH (Memory Analytics Shell).
It provides multiple commands to manage the cluster.

To start MASH, type the following on command prompt

```
$ <ampool-home>/bin/mash
mash>
```
The mash prompt above implies you are in MASH shell.
Command help can be used to obtain the list all the supported commands
 ```
mash>help
```

To get details of a specific command type help followed by the specific command
 e.g. start sever command, start locator command.
```
mash>help start server
mash>help start locator
```
##### 3.1 Start Ampool Services
There are two services (the Ampool Locator and Ampool Server) that need to be started before you start using Ampool.
The recommended order of starting the two services is Locator first followed by Servers. Similarly recommended way to stop the two services is in the reverse order.

##### 3.2 Starting Ampool Locator (on local host machine)
The locator can be started using MASH start locator command as shown below.
```
mash>start locator  --name=locator1  --dir=<full path to pre-created locator data directory>
--properties-file=<ampool-home>/config/ampool_locator.properties
--hostname-for-clients=localhost
```
Below are more commonly used options when starting the locator. For the complete list of supported options you can refer help start locator command.
- dir: Directory in which the Locator will be started and run. This directory needs to exist a priori. The default is the current directory from which MASH is launched.
- hostname-for-clients : Hostname or IP address that will be sent to clients so they can connect to this Locator. The default is the bind-address of the Locator.
- Port: The port to be used for communication by the locator. If not specified, the default port 10334 is used.
- properties-file:The additional set of properties can be passed when starting the locator. For more details see <ampool-home>/config/ampool_locator.properties file.

Please use help command for more detailed description and list of parameters i.e.
```
mash>help start locator
```

##### 3.3 Starting Ampool Server (on local host)
The Server can be started using MASH start server command as shown below.
```
mash>start server  --name=server1  --server-port=40404  --locators=localhost[10334]
--dir= <full path to pre-created locator data directory>
 --properties-file=<ampool-home>/config/ampool_server.properties
```

Below are more commonly used options when starting the servers. For the complete list of supported options you can refer help start server command.
- dir : Directory in which the Cache Server will be started and run. This directory needs to
- locators : Sets the list of Locators used by the Ampool Server to join the appropriate Ampool cluster.
- properties-file : The additional set of properties can be passed when starting the server. For more details see <ampool-home>/config/ampool_server.properties file.
- server-port: The port that the distributed system’s server sockets in a client-server topology will listen on. The default server-port is 40404.
Please use help command for more detailed description and list of parameters i.e.
```
mash>help start server
```

##### 3.4 Stopping the Ampool Services
The Ampool services (both locator and server) can be stopped using MASH commands. It is recommended that the servers are stopped before the locator.
Assuming the above listed Locator and Sever are running, you can use below command to stop a single server. 
```
mash>stop server --name=server1
```
And the below command can be used to stop the locator service. 
```
mash>stop locator --name=locator
```
f you want to stop all the members (except the locator) in the Ampool cluster, you can use the shutdown command. But, stopping all members may cause data-loss if the disk-persistence is not enabled for the created tables. 
```
mash>shutdown
```
To include the locator in the shutdown use 
```
mash>shutdown --include-locators=true
```
#### 4. Setting up Ampool Cluster on a set of distributed machines
Let us say you have  5 machines (Machine_0 to Machine_4) and a Client machine on which you want to create an Ampool cluster and run the Ampool services (i.e. one locator and 4 servers) using MASH commands.

Assuming the above machine names and Ampool package is loaded on each of the machines including client machine, follow the process as below:
1. Log into Machine_0 and start Locator_0 through MASH shell using start locator  command.
```
mash>start locator  --name=Locator_0
```
2. Log into Machine_1 and start Server_1 through MASH shell using start server  command as foloows:
```
mash>start server  --name=Server_1  --server-port=40404  --locators=Machine_0[10334]
```
3. Log into Machine_2 and start Server_2 through MASH shell using start server  command as follows:
```
mash>start server  --name=Server_2  --server-port=40404  --locators=Machine_0[10334]
```
4. Log into Machine_3 and start Server_3 through MASH shell using start server  command as follows:
```
mash>start server  --name=Server_3  --server-port=40404  --locators=Machine_0[10334]
```
5. Log into Machine_4 and start Server_4 through MASH shell using start server  command as follows:
```
mash>start server  --name=Server_4  --server-port=40404  --locators=Machine_0[10334]
```

Your Ampool Cluster is all set. Now you can connect to the cluster through Client Machine using MASH as follows:
```
mash>connect --locator=Machine_0[10334]
```
Now you are ready to play around with other MASH commands that will help you create tables, put and update data, get data, delete tables etc. as described in next session.

#### 5. Other Mash Commands
To list a few commonly used MASH commands let us attempt to do the following:
1. Start an Ampool Cluster with one Locator and two Servers on local host.
2. Connect to the Ampool Cluster.
3. List members of the cluster.
4. Create an Ordered table and call it ```TableOrdered```.
5. Create an Unordered table and call it ```TableUnordered```.
6. Create an Immutable table and call it ```TableImmutable```.
7. List the tables.
8. Describe tables.
9. Insert a few rows into Ordered table i.e. TableOrdered.
10. Insert a few rows into Unordered table i.e. TableUnordered.
11. Append a few rows into Immutable table i.e. TableImmutable.
12. Get a complete row entry with specific Key from a table to verify the entries inserted above.
13. Update a specific column of a row in the table.
14. Delete a complete row entries with specific Key from the table.
15. Full table scan on tables.
16. Scan all entries within a specified range (applicable only for Ordered table).
17. Delete a table.
18. Stop Servers.
19. Stop Locator.
20. Exit MASH.

NOTE: Only the usage of the commands is shown. For details on individual commands and complete list of parameters, use the help command in MASH.

###### 5.1. Start an Ampool Cluster with one Locator and two Servers on local host.
```
mash>start locator --name=locator_1
mash>start server --name=server_1 –-server-port=40404
mash>start server --name=server_2 –-server-port=40405
```

###### 5.2. Connect to the Ampool Cluster.
```
mash>connect --name=locator_1
```

###### 5.3. List members of the cluster.
```
mash>list members
```
This will display list containing the locator_1, server_1 and server_2
###### 5.4. Create an Ordered Table call it TableOrdered.
Creating tables with ```--columns``` option with create a table with all column types as BINARY. In order to create a table with supported column types please use ```--schema-json``` option. It is applicable to all types of tables.
```
create table --name=TableOrdered --type=ORDERED_VERSIONED --columns=ID,NAME,AGE,SEX,DESIGNATION
```

###### 5.5. Create an Unordered Table call it TableUnordered.
```
create table --name=TableUnordered --type=UNORDERED --columns=ID,NAME,AGE,SEX,DESIGNATION
```

###### 5.6. Create an Immutable Table, with valid schema, and call it TableImmutable.
```
create table --name=TableImmutable --type=IMMUTABLE --schema-json='{"ID":"INT", "NAME":"STRING", "AGE":"INT", "GENDER":"STRING","DESIGNATION":"STRING"}'
```

###### 5.7. List the Tables.
```
mash>list tables
```
This will display list containing TableOrdered and TableUnordered
###### 5.8. Describe Tables.
```
mash> describe table –name=TableOrdered
mash> describe table –name=TableUnordered
mash> describe table –name=TableImmutable
```
This above two will give a complete description of TableOrdered and TableUnordered
###### 5.9. Insert a few rows into Ordered Table.
To insert into tables with columns having BINARY type, ```--value``` option can be used. It converts the provided string data to BINARY. If the table was defined with column types other than BINARY, then please ```--value-json``` option that will convert column values to the respective data types. This options is applicable to all types of tables including ```tput``` and ```tappend``` commands.
```
mash> tput --key=1 --value=ID=1,NAME=Andy,AGE=34,SEX=M,DESIGNATION=PM
--table= TableOrdered
```
```
mash> tput --key=2 --value=ID=2,NAME=Avinash,AGE=38,SEX=M,DESIGNATION=ARCH
--table= TableOrdered
```
```
mash> tput --key=3 --value=ID=3,NAME=Deepa,AGE=26,SEX=F,DESIGNATION=SE
--table= TableOrdered
```
```
mash> tput --key=4 --value=ID=4,NAME=Sri,AGE=32,SEX=M,DESIGNATION=SSE
--table= TableOrdered
```
```
mash> tput --key=5 --value=ID=5,NAME=Avani,AGE=36,SEX=F,DESIGNATION=PL
--table= TableOrdered
```
```
mash> tput --key=6 --value=ID=6,NAME=Satya,AGE=28,SEX=F,DESIGNATION=SE
--table= TableOrdered
```
```
mash> tput --key=7 --value=ID=7,NAME=Manish,AGE=34,SEX=M,DESIGNATION=ARCH
--table= TableOrdered
```
```
mash> tput --key=8 --value=ID=8,NAME=Shankar,AGE=32,SEX=M,DESIGNATION=TL
--table= TableOrdered
```

###### 5.10. Insert a few rows into Unordered Table.
```
mash> tput --key=1 --value=ID=1,NAME=Raghu,AGE=34,SEX=M,DESIGNATION=DIR
--table= TableUnordered
```
```
mash> tput --key=2 --value=ID=2,NAME=Bali,AGE=27,SEX=M,DESIGNATION=SE
--table= TableUnordered
```
```
mash> tput --key=3 --value=ID=3,NAME=Suman,AGE=26,SEX=F,DESIGNATION=SE
--table= TableUnordered
```
```
mash> tput --key=4 --value=ID=4,NAME=Danny,AGE=32,SEX=M,DESIGNATION=SSE
--table= TableUnordered
```
```
mash> tput --key=5 --value=ID=5,NAME=Kiran,AGE=31,SEX=F,DESIGNATION=SSE
--table= TableUnordered
```
```
mash> tput --key=6 --value=ID=6,NAME=Fransis,AGE=33,SEX=F,DESIGNATION=MNG
--table= TableUnordered
```
```
mash> tput --key=7 --value=ID=7,NAME=Arvind,AGE=34,SEX=M,DESIGNATION=ADM
--table= TableUnordered
```
```
mash> tput --key=8 --value=ID=8,NAME=Hari,AGE=32,SEX=M,DESIGNATION=FIN
--table= TableUnordered
```

###### 5.11. Append a few rows into an Immutable table.
```
mash>tappend --table=TableImmutable --value-json='{"ID":"1", "NAME":"Joey", "AGE":"33", "GENDER":"M","DESIGNATION":"ARCH"}'
mash>tappend --table=TableImmutable --value-json='{"ID":"2", "NAME":"Monica", "AGE":"38", "GENDER":"F","DESIGNATION":"ARCH"}'
mash>tappend --table=TableImmutable --value-json='{"ID":"3", "NAME":"Janice", "AGE":"29", "GENDER":"M","DESIGNATION":"HR"}'
```

###### 5.12. Get a row entry with specific Key from the Ordered and Unordered Tables.
```
mash>tget --key=2 --table=TableOrdered
```
This display all the column and their values for row whose key = 2 from TableOrdered.
```
mash>tget --key=7 --table=TableOrdered
```
This display all the column and their values for row whose key = 7 from TableOrdered.
```
mash>tget --key=4 --table=TableUnordered
```
This display all the column and their values for row whose key = 4 from TableUnordered.
```
mash>tget --key=6 --table= TableUnordered
```
This display all the column and their values for row whose key = 6 from TableUnordered.

###### 5.13. Update a specific column of a row in Ordered and Unordered tables.
Here the same tput command is used. If the specified key already exists, then tput acts like an update else it creates a new entry in the table.  For example.
```
mash> tput --key=6 --value=NAME=Raj,AGE=35 --table=TableOrdered
```
Here row with key=6 exist in TableOrdered and therefore the above command only updates the specified column values i.e. NAME=Raj and AGE=35. One can verify the change made using tget –key=6 –table=TableOrdered command.
```
mash> tput --key=11 --value=ID=11,NAME=Zhaid,AGE=25 --table=TableUnordered
```
Here row with key=11 does not exist in TableUnordered and therefore the above command will create a new row entry in TableUnordered table with specified key and column values. One can verify the change made using tget --key=11 --table=TableUnordered command.
If the table was created by specifying the column types using JSON format (see help on create table command), one can specify the values in JSON format too. For example:
```
mash> tput --key=11 –value-json=”{‘ID’:’11’,’NAME’:’Zhaid’,’AGE’:’25’}” --table=TableUnordered
```

###### 5.14. Delete a row few entries.
This is applicable to only Ordered and Unordered tables.
```
mash> tdelete --key=8 --table=TableOrdered
```
Here the complete row with key=8 will be permanently deleted from TableOrdered
```
mash> tdelete --key=3 --table=TableUnordered
```
Here the complete row with key=3 will be permanently deleted from TableUnordered.

###### 5.15. Full table scan on tables.
```
mash>tscan --table=TableOrdered
mash>tscan --table=TableUnordered
mash>tscan --table=TableImmutable
```

###### 5.16. Scan all entries within a specified range in an Ordered Table, with Start and Stop Key range.
```
mash>tscan --table=TableOrdered --startkey=2 --stopkey=7
```
This display all the rows starting with key falls in the range [startkey, stopkey) range. Note the stopkey is excluded from the range.
NOTE: Key being a STRING object, string comparison is used for determining if a key falls in the range or outside the range.

###### 5.17. Delete Table.
```
mash>delete table --name=TableOrdered
mash>delete table --name=TableUnordered
mash>delete table --name=TableImmutable
```
The deletion can be verified by using ```list tables``` command. You’ll find the deleted tables now missing from the list.

###### 5.18. Stop Servers.
```
mash>stop server --name=server_1
mash>stop server --name=server_2
```

###### 5.19. Stop Locator.
```
mash>stop locator --name=locator_1
```

###### 5.20. Exit MASH.
```
mash>exit
```

#### 6. Javadocs
You can find documentation for Monarch client API under 
```
<ampool-home>/javadoc directory.
```
#### 7. Quickstart Examples
Few examples demonstrating the use of Ampool Java client APIs can be found at location: <ampool-home>/examples directory.


