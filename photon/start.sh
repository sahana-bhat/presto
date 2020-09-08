#!/bin/bash

LOG_FILE="udeploy-start.log"
SERVICE_NAME=presto
HOST=`hostname`
FQDN=`hostname -f`
GC_LOG_FILE=$(ls -ltrh /shared/var/log/presto-gc* | awk '{print $NF}' | tail -1)
PIPELINE_FILE=/etc/uber/pipeline
ENVIRONMENT_FILE=/etc/uber/environment
ENVIRONMENT=`cat $ENVIRONMENT_FILE`

#Setup base directories and common env
SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
BASE_DIR="$SCRIPT_DIR/.."
source ${BASE_DIR}/photon/common

mkdir -p /shared/var/log

# Setup libjvmkill for killing the OOM worker
if [ ! -f "/var/log/udocker/presto/libjvmkill.so"  ] ; then
    sudo cp ${BASE_DIR}/presto-docker/bin/libjvmkill.so /var/log/udocker/presto/libjvmkill.so
fi

log "Starting Presto for $UBER_DATACENTER under $BASE_DIR"

config_nodeid() {
    log "Setting node ID: $HOST"

    sed -i "s/%HOSTNAME%/$HOST/g" ${BASE_DIR}/presto-docker/etc/node.properties
}

stop_presto() {
    log "Stopping Presto ..."
    pkill presto-server
}

start_presto() {
    LAUNCHER_LOG_FILESIZE=$(stat -c%s /var/log/udocker/presto/var/log/launcher.log)
    # Delete launcher logs if it is over 500m
    if (( LAUNCHER_LOG_FILESIZE > 500000000 )); then
       echo "deleting launcher logs because it is too big ..."
       sudo -u presto mv /var/log/udocker/presto/var/log/launcher.log /var/log/udocker/presto/var/log/launcher.log.old
    fi
    log "Starting Presto ..."
    set -e
    export PATH=$JAVA_HOME/bin:$PATH;
    sudo sh -c "$BASE_DIR/presto-docker/bin/launcher.py run --enable-central-logging"
    background_pid=$!
    wait "${background_pid}"
    echo "logoff"
}

rm -rf $BASE_DIR/presto-docker/etc
cp -R $BASE_DIR/config/dsc/dsc@presto-$UBER_DATACENTER\_$UDEPLOY_DEPLOYMENT_NAME $BASE_DIR/presto-docker/etc
mkdir -p $BASE_DIR/presto-docker/etc/hadoop/conf
mkdir -p $BASE_DIR/presto-docker/etc/catalog
mv $BASE_DIR/presto-docker/etc/*.xml $BASE_DIR/presto-docker/etc/hadoop/conf/
mv $BASE_DIR/presto-docker/etc/jmx.properties $BASE_DIR/presto-docker/etc/catalog/
mv $BASE_DIR/presto-docker/etc/hive.properties $BASE_DIR/presto-docker/etc/catalog/
mv $BASE_DIR/presto-docker/etc/rta_staging.properties $BASE_DIR/presto-docker/etc/catalog/
mv $BASE_DIR/presto-docker/etc/rta.properties $BASE_DIR/presto-docker/etc/catalog/
mv $BASE_DIR/presto-docker/etc/pinotstg.properties $BASE_DIR/presto-docker/etc/catalog/
mv $BASE_DIR/presto-docker/etc/pinotadhoc.properties $BASE_DIR/presto-docker/etc/catalog/
mv ${BASE_DIR}/photon/launcher.py ${BASE_DIR}/presto-docker/bin/

# Update truststore path and password in config
CONFIG_PROPERTIES="$BASE_DIR/presto-docker/etc/config.properties"
SECURE_KEYSTORE_PWD_FILE="/langley/current/photon/secure_presto/keystore/odin_password_2022-11-11"

SECURE_KEYSTORE_FILE="/langley/current/photon/secure_presto/keystore/odin_secure_presto_2022-11-11.keystore"
if [ -f $SECURE_KEYSTORE_FILE ]; then
    sed -i -e "/^http-server.https.keystore.path/ s#REPLACE_SSL_PRIVATE_TRUSTSTORE_FILEPATH#$SECURE_KEYSTORE_FILE#" $CONFIG_PROPERTIES
fi

if [ -f $SECURE_KEYSTORE_PWD_FILE ]; then
    SECURE_KEYSTORE_PWD=$(cat $SECURE_KEYSTORE_PWD_FILE)
    sed -i -e "/^http-server.https.keystore.key/ s/REPLACE_SSL_PRIVATE_TRUSTSTORE_PASSWORD/$SECURE_KEYSTORE_PWD/" $CONFIG_PROPERTIES
fi

# To get the actual co-ordinator hostname
if [ $ODIN_ROLE == 'coordinator' ]; then
  HOST_NAME=$HOSTNAME
else
  HOST_NAME=`jq -r '.all[] | .hostName' <<< $ODIN_NAMED_NODE_PLACEMENT`
fi

REPLACE_HOST_NAME="http://"$HOST_NAME".prod.uber.internal:8080"
sed -i -e "/^discovery.uri/ s%REPLACE_DISCOVERY_URI%$REPLACE_HOST_NAME%" $CONFIG_PROPERTIES

sed -i -e "s/host_name/$HOST_NAME/g" ${BASE_DIR}/photon/metrics.conf

echo $HOST_NAME |tr -d '\n' > /shared/coordinator

# Setup password in etc/resource-groups.properties
# Find the actual password from langley password file based on data center
# Append the password to the db url in resource-groups.properties
# e.g, resource-groups.config-db-url=jdbc:mysql://ip:port/db?&user=presto will become
# resource-groups.config-db-url=jdbc:mysql://ip:port/db?&user=presto&password=(pwd from langley)
# Do nothing if db url already has a password
RESOURCE_GROUP_DB_CONFIG="$BASE_DIR/presto-docker/etc/resource-groups.properties"
RESOURCE_GROUP_DB_PWD_FILE="/langley/current/photon/presto/odin_rg_password/PASSWORD_FILE"
RESOURCE_GROUP_DB_PWD_KEY="nemo.password_of_user.s_presto_presto"
if [ -f $RESOURCE_GROUP_DB_CONFIG ] \
     && grep "resource-groups.configuration-manager=db" $RESOURCE_GROUP_DB_CONFIG \
     && ! grep "password" $RESOURCE_GROUP_DB_CONFIG; then
	RESOURCE_GROUP_PWD=$(grep "$RESOURCE_GROUP_DB_PWD_KEY" $RESOURCE_GROUP_DB_PWD_FILE | awk -F'=' '{print $2}')
	sed -i -e "/^resource-groups.config-db-url/ s/$/\&password=$RESOURCE_GROUP_PWD/" $RESOURCE_GROUP_DB_CONFIG
fi

#Create krb5.conf with respective region
DATACENTER=`expr $UBER_DATACENTER | cut -c1-3`
if [ $DATACENTER == 'phx' ]; then
    sed -i -e "s/region/phx/g" /home/udocker/photon/photon/krb5.conf
else
    sed -i -e "s/region/dca1/g" /home/udocker/photon/photon/krb5.conf
fi

sudo cp /home/udocker/photon/photon/krb5.conf /etc/krb5.conf

# setup all mysql connector from langley
CONNECTOR_DIR="$BASE_DIR/presto-docker/etc/catalog"
if [ ! -z ${UBER_DATACENTER+x} ] && [ ! -z ${UDEPLOY_DEPLOYMENT_NAME+x} ]; then
	LANGLEY_CONNECTORS_DIR="/langley/current/photon/$UBER_DATACENTER/$UDEPLOY_DEPLOYMENT_NAME"
	if [ -d $LANGLEY_CONNECTORS_DIR ]; then
		ln -sf $LANGLEY_CONNECTORS_DIR/* $CONNECTOR_DIR
	else
		echo "$LANGLEY_CONNECTORS_DIR doesn't exist."
	fi
else
	echo "\$UBER_DATACENTER or \$UDEPLOY_DEPLOYMENT_NAME is not been set. UBER_DATACENTER=$UBER_DATACENTER, UDEPLOY_DEPLOYMENT_NAME=$UDEPLOY_DEPLOYMENT_NAME";
fi

config_nodeid
start_presto

METRICS_CONFIG="$BASE_DIR/presto-docker/etc/node.properties"
METRICS_CLUSTER_NAME=$(grep "node.environment=" $METRICS_CONFIG | awk -F'=' '{print $2}')

mkdir -p $SCRIPT_DIR/log
checker_pid=0
${BASE_DIR}/photon/metrics.py --port 8080 --interval 60 --service "presto.$METRICS_CLUSTER_NAME" --debug &
checker_pid=$!

trap 'kill $checker_pid; exit 1' SIGINT SIGTERM
sleep 5

while [ 1 ]; do
    if [[ $checker_pid -ne 0 ]]; then
        kill -0 $checker_pid
    fi
    set +x
    sleep 10
done

exit 0
