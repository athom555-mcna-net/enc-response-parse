#!/bin/bash 

cat > /tmp/ctx.edn <<ENDEND
{   :invoke-fn             enc-response.proc/icn-maps-aug->tx-data

    :icn-maps-aug-fname    "icn-maps-aug-prod-small.edn"
    :tx-data-fname         "tx-data-prod-small.edn"

    :db-uri               "datomic:dev://localhost:4334/enc-response"
}
ENDEND

java -jar /Users/athom555/work/enc-response-parse/target/enc-response-parse-1.0.0-SNAPSHOT-standalone.jar  /tmp/ctx.edn 

