#!/bin/bash

echo ""
ps -al | head -1
ps -al | grep 'java.*datomic-transactor-pro-.*.jar.*clojure.main.*datomic.launcher' | grep -v "grep"
echo ""

