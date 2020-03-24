#!/bin/bash -ex
set -x
set -e
SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $SCRIPT_DIR/..
mvn -P neutrino clean package -pl presto-event-listener,presto-main,presto-pinot,presto-aresdb,presto-server-neutrino,presto-spi,presto-geospatial,presto-cassandra,presto-rta -am -TC1 -DskipTests $*
