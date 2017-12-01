# Ampool Docker runtime.
/*
* Copyright (c) 2017 Ampool, Inc. All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License"); you
* may not use this file except in compliance with the License. You
* may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
* implied. See the License for the specific language governing
* permissions and limitations under the License. See accompanying
* LICENSE file.
*/

FROM alpine:latest
MAINTAINER Nitin Lamba <nitin@ampool.io>

LABEL name="Ampool Base Image"
LABEL vendor="Ampool Inc."
LABEL version=1.3.1
LABEL build-date="2017-MAR-11"

# download Ampool bits
ENV	JAVA_HOME /opt/jdk1.8.0_121
ENV	AMPOOL_VERSION 1.3.1

# download packages, JRE, Ampool bits, untar and cleanup
RUN apk add --update bash curl wget ca-certificates && mkdir /opt \
  && cd /tmp && wget "https://github.com/andyshinn/alpine-pkg-glibc/releases/download/2.23-r1/glibc-2.23-r1.apk" "https://github.com/andyshinn/alpine-pkg-glibc/releases/download/2.23-r1/glibc-bin-2.23-r1.apk" \
  && apk add --allow-untrusted glibc-2.23-r1.apk glibc-bin-2.23-r1.apk \
  && /usr/glibc-compat/sbin/ldconfig /lib /usr/glibc/usr/lib \
  && echo 'hosts: files mdns4_minimal [NOTFOUND=return] dns mdns4' >> /etc/nsswitch.conf \
  && cd /tmp && wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u121-b13/e9e7ea248e2c4826b92b3f075a80e441/jdk-8u121-linux-x64.tar.gz" \
  && tar -xzf jdk-8u121-linux-x64.tar.gz -C /opt \
  && rm -rf $JAVA_HOME/*src.zip \
            $JAVA_HOME/lib/missioncontrol \
            $JAVA_HOME/lib/visualvm \
            $JAVA_HOME/lib/*javafx* \
            $JAVA_HOME/jre/lib/plugin.jar \
            $JAVA_HOME/jre/bin/javaws \
            $JAVA_HOME/jre/bin/jjs \
            $JAVA_HOME/jre/bin/keytool \
            $JAVA_HOME/jre/bin/orbd \
            $JAVA_HOME/jre/bin/pack200 \
            $JAVA_HOME/jre/bin/policytool \
            $JAVA_HOME/jre/bin/rmid \
            $JAVA_HOME/jre/bin/rmiregistry \
            $JAVA_HOME/jre/bin/servertool \
            $JAVA_HOME/jre/bin/tnameserv \
            $JAVA_HOME/jre/bin/unpack200 \
            $JAVA_HOME/jre/lib/javaws.jar \
            $JAVA_HOME/jre/lib/desktop \
            $JAVA_HOME/jre/lib/deploy* \
            $JAVA_HOME/jre/lib/*javafx* \
            $JAVA_HOME/jre/lib/*jfx* \
            $JAVA_HOME/jre/lib/oblique-fonts \
            $JAVA_HOME/jre/lib/amd64/libdecora_sse.so \
            $JAVA_HOME/jre/lib/amd64/libprism_*.so \
            $JAVA_HOME/jre/lib/amd64/libfxplugins.so \
            $JAVA_HOME/jre/lib/amd64/libglass.so \
            $JAVA_HOME/jre/lib/amd64/libgstreamer-lite.so \
            $JAVA_HOME/jre/lib/amd64/libjavafx*.so \
            $JAVA_HOME/jre/lib/amd64/libjfx*.so \
            $JAVA_HOME/jre/lib/ext/nashorn.jar \
            $JAVA_HOME/jre/lib/ext/jfxrt.jar \
            $JAVA_HOME/jre/plugin \
            /usr/share/locale/* \
  && cd /opt \
  && wget "https://s3-us-west-2.amazonaws.com/ampool-releases/${AMPOOL_VERSION}/ampool-${AMPOOL_VERSION}.tar.gz" \
  && tar -xzf ampool-${AMPOOL_VERSION}.tar.gz \
  && rm -rf ampool-${AMPOOL_VERSION}.tar.gz \
  && apk del curl wget ca-certificates && rm /tmp/* /var/cache/apk/*

ENV AMPOOL_HOME /opt/ampool-$AMPOOL_VERSION
ENV PATH $PATH:$JAVA_HOME/bin:$AMPOOL_HOME/bin

VOLUME ["/data/"]
