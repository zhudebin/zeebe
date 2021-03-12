#!/bin/bash -xeu

defaultHostName=$(hostname)
export ZEEBE_BROKER_GATEWAY_CLUSTER_HOST=${ZEEBE_BROKER_GATEWAY_CLUSTER_HOST:-${defaultHostName}}

ZEEBE_STANDALONE_GATEWAY=${ZEEBE_STANDALONE_GATEWAY-:"false"}
if [ "$ZEEBE_STANDALONE_GATEWAY" = "true" ]; then
    exec /usr/local/zeebe/bin/gateway
else
    export ZEEBE_BROKER_NETWORK_ADVERTISEDHOST=${ZEEBE_BROKER_NETWORK_ADVERTISEDHOST:-${defaultHostName}}
    exec /usr/local/zeebe/bin/broker
fi
