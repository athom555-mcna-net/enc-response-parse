#!/bin/bash 

cat > /tmp/ctx.edn <<ENDER
{
 :db-uri               "datomic:dev://localhost:4334/enc-response"
 :tx-size-limit        3

 :missing-icn-fname    "/Users/athom555/work/missing-icns-prod-small.edn"
;:missing-icn-fname    "/Users/athom555/work/missing-icns-prod-orig.edn"

 :icn-maps-aug-fname   "icn-maps-aug.edn"
 :tx-data-fname        "tx-data.edn"

 :invoke-fn             enc-response.proc/icn-maps-aug->tx-data
}
ENDER

java -jar /Users/athom555/work/enc-response-parse/target/enc-response-parse-1.0.0-SNAPSHOT-standalone.jar  /tmp/ctx.edn 

