FROM ubuntu:18.04
MAINTAINER Di Zhang

WORKDIR /src/seqspark

RUN apt-get update \
 && apt-get install -y \
    curl \
    unzip \
    openjdk-8-jdk \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

ENV SBT_VERSION 1.3.2
RUN \
  curl -L -o sbt-$SBT_VERSION.deb http://dl.bintray.com/sbt/debian/sbt-$SBT_VERSION.deb && \
  dpkg -i sbt-$SBT_VERSION.deb && \
  rm sbt-$SBT_VERSION.deb && \
  apt-get update && \
  apt-get install sbt && \
  sbt sbtVersion

# spark
ENV SPARK_VERSION 2.3.4
ENV SPARK_PACKAGE spark-${SPARK_VERSION}-bin-hadoop2.7
ENV SPARK_HOME /opt/spark-${SPARK_VERSION}
RUN curl -sL --retry 3 \
  "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz" \
  | gunzip \
  | tar x -C /opt/ \
 && mv /opt/$SPARK_PACKAGE $SPARK_HOME \
 && chown -R root:root $SPARK_HOME

# add current dir
ADD . .

# build the jar
RUN ./install --prefix /opt/seqspark --db-dir /opt/seqspark/ref --no-download

# use multiple stage build to reduce the final image size
FROM openjdk:8-jre-alpine

RUN apk add --no-cache bash

WORKDIR /root/

ENV SPARK_VERSION 2.3.4
ENV SPARK_HOME /opt/spark-${SPARK_VERSION}
ENV SEQSPARK_HOME /opt/seqspark

COPY --from=0 ${SPARK_HOME} ${SPARK_HOME}
COPY --from=0 ${SEQSPARK_HOME} ${SEQSPARK_HOME}

ENV PATH $PATH:${SPARK_HOME}/bin
ENV PATH $PATH:${SEQSPARK_HOME}/bin

RUN adduser -Ds /bin/bash seqspark
USER seqspark
WORKDIR /home/seqspark

ENTRYPOINT ["seqspark"]