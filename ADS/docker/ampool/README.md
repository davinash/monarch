# Ampool Docker (v 1.3.1)
This document describes how to build and run docker images for Ampool services. These instructions are tested with the following docker version:

> ```docker version```

> Version:      1.10.3

> API version:  1.22

> Go version:   go1.5.3

> Git commit:   20f81dd

> Built:        Thu Mar 10 21:49:11 2016

> OS/Arch:      linux/amd64


> ```docker-compose version```

> docker-compose version 1.6.2, build 4d72027

> docker-py version: 1.7.2

> CPython version: 2.7.9

> OpenSSL version: OpenSSL 1.0.1j 15 Oct 2014

## Building the container image
The current docker image (1.3.1) is based on alpine (linux) and pulls JDK 1.8.0_92 as a dependency. It also exposes the following ports for different Ampool services:

* __1099__: JMX RMI port, used by the Locator
* __9090__: HTTP REST, on Server (available in 1.0 release)
* __7070__: HTTP web server, hosting Pulse on Locator (available in 1.0 release)
* __10001-3__: Ephemeral TCP/UDP ports for inter-member communication
* __10334__: Locator port
* __40404__: Server port

Use the following command to build the image:
```
$ docker build -t ampool/runtime:1.3.1 .
$ docker tag ampool/runtime:1.3.1 ampool/runtime:latest
```
This would pull ampool binaries from S3 and unpack the distribution.

Once the base runtime image is built, locator and server images need to be built:

```
$ docker build -t ampool/locator:latest -f Dockerfile.Locator .
$ docker build -t ampool/server:latest -f Dockerfile.Server .
```

After building the image, create a bridge network for Ampool Locator and Servers to connect with each other:

```
$ docker network create ampool-net
```
## Starting Ampool Services in docker containers

### Starting Locator

Once the network is created, you can start a container using the following command:
```
$ docker run -d -p 10334:10334 -p 7070:7070 -p 1099:1099 --net=ampool-net \
-e AMPOOL_SERVICE_HOST=192.168.99.100 \
-e AMPOOL_PORTS_LOW=10001 -e AMPOOL_PORTS_HIGH=10003 \
--name locator ampool/locator:latest
```

This essentially starts a locator specifying hosts for JMX manager and client connections.

### Starting Server
You can start another container using the following command:
```
$ docker run -d -v /pi/src/develop/ampool/ampool/server:/data -p 40404:40404 -p 9090:9090 --net=ampool-net \
-e AMPOOL_SERVICE_HOST=192.168.99.100 -e AMPOOL_LOCATOR_HOST=locator \
-e AMPOOL_INITIAL_HEAP=512M -e AMPOOL_MAX_HEAP=512M \
-e AMPOOL_PORTS_LOW=10001 -e AMPOOL_PORTS_HIGH=10003 \
-e SERVER_HEAP_PERCENTAGE=91 \
-e SERVER_EVICTION_PERCENTAGE=65 \
-e AMPOOL_SERVER_NAME=Server1 \
-e AMPOOL_START_REST=true \
--name Server1 ampool/server:latest

```
Once the server is started, you can check whether the locator is correctly discovered. On the locator's mash prompt, running `list members ` should show (example below):
```
mash>list members
 Name   | Id
------- | ---------------------------------------------
locator | 4cb0977b79f7(locator:44:locator)<ec><v0>:1024
Server1 | 09d012decde1(Server1:91)<ec><v3>:1024
```

If you need more than one Server, repeat the steps above to start another server container with different hostnames.

## Creating an Ampool (container) cluster
Though Ampool Locator and Servers can be created manually with the steps above, it can get complicated for test and production environments. For such cases, an automated deployment of ampool cluster is highly desirable, which can scale up/down and restart automatically, amongst other things. For this purpose, we can use any of the container runtime environments such as docker-compose, docker swarm, Cloud Foundry or Kubernetes. Let's start with docker-compose, which is already available in your docker deployment.

### Using docker-compose (experimental)
Besides the image definition (Dockerfile), this package also includes `docker-compose.yaml` file that defines Ampool services, namely locator and server. To setup an ampool cluster, simply run the following command from this directory:
```
docker-compose up -d
# OR
docker-compose up &
```
This creates a network bridge (`ampool_default`), starts a locator and a server. You can check the state of the running containers by using `docker-compose ps`. Once ampool services are started, you can check whether the locator has correctly discovered all the servers. Connect to the locator container, start mash prompt, and check:
```
$ docker exec -it <container_ID> /bin/bash
$ mash
mash> connect
mash> list members
```
#### Scaling the cluster
The number of servers can be scaled up or down by using standard docker-compose commands:
```
$ docker-compose scale server=3
```
You can check that list members now shows 3 servers instead of 1. Alternatively, you can look at the server status through the Pulse UI:
```
http://<docker_host>:7070/pulse
```

To shutdown the system, simply call `docker-compose down`.

#### Accessing Geode REST APIs
The service definition of the server in docker-compose also starts Geode/Ampool Developer REST APIs that can be used to access the endpoints. As the REST APIs cannot be hosted on the locator, it is difficult to expose these services cleanly from docker on a single external IP address. Therefore, to use these APIs, separate server end-points have to be accessed.

For example, on Mac OSX running Docker ToolBox, an explicit route to host should be added:
```
sudo route add -net 172.18.0.0/16 192.168.99.100
```
where `172.18.0.0` is the network bridge IP (`ampool_default`) and `192.168.99.100` is the IP for Docker Host VM.

Once the server is started, the REST APIs can be accessed via:
```
http://<server_ip>:9090/geode-api/v1
```

----
For questions or feedback, please contact support@ampool.io
