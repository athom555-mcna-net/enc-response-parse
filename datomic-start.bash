#!/bin/bash 

echo ""
echo "Starting Datomic transactor in in background..."

/opt/datomic/bin/transactor ${PWD}/dev-transactor-local.properties  & 

sleep 3
echo ""

