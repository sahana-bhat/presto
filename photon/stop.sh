#!/bin/bash

LOG_FILE="udeploy-start.log"
SERVICE_NAME=presto
HOST=`hostname`

#Setup base directories and common env
SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
BASE_DIR="$SCRIPT_DIR/.."
source ${BASE_DIR}/photon/common

log "Stopping Presto for $DATACENTER under $BASE_DIR"

stop_presto() {
    log "Stopping Presto ..."
    if isContainer; then
      pkill presto-server
    else
      sudo -u presto pkill presto-server
    fi
}

stop_presto
exit 0
