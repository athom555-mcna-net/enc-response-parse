#!/bin/bash 

datomic-ps.bash

pkill -9 -lf 'java.*datomic-transactor-pro-.*.jar.*clojure.main.*datomic.launcher'
echo ""

