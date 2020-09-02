#!/bin/bash

set -ex

exec /usr/lib/nagios/plugins/check_http -I 127.0.0.1 -p ${UBER_PORT_HTTP} -u /v1/info -w 2 -c 10 -e '200 OK'