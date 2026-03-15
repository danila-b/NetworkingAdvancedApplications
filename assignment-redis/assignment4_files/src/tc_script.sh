#!/bin/bash

# Get MongoDB IP address
MONGO_IP=$(getent hosts mongodb | awk '{ print $1 }')
if [ -z "$MONGO_IP" ]; then
    echo "Error: Could not resolve MongoDB IP address"
    exit 1
fi
echo "MongoDB IP address: ${MONGO_IP}"

# Add priority queueing discipline
echo "Adding priority queueing discipline..."
if ! tc qdisc add dev eth0 root handle 1: prio; then
    echo "Error: Failed to add priority queueing discipline"
    exit 1
fi

# Add network emulation with delay
echo "Adding network delay of 10ms..."
if ! tc qdisc add dev eth0 parent 1:3 handle 30: netem delay 10ms; then
    echo "Error: Failed to add network delay"
    exit 1
fi

# Add traffic filter
echo "Adding traffic filter for MongoDB IP..."
if ! tc filter add dev eth0 protocol ip parent 1:0 prio 3 u32 match ip dst $MONGO_IP/32 flowid 1:3; then
    echo "Error: Failed to add traffic filter"
    exit 1
fi


echo "Network traffic shaping successfully configured"
