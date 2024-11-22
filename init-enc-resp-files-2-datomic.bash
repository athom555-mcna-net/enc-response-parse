#!/bin/bash 

cat > /tmp/ctx.edn <<ENDEND
{   :db-uri               "datomic:dev://localhost:4334/enc-response-test"
    :tx-size-limit        500

   :encounter-response-root-dir "/Users/athom555/work/iowa-response-5"
 ; :missing-icn-fname           "/Users/athom555/work/missing-icns-prod-small.edn"
 ; :icn-maps-aug-fname          "icn-maps-aug.edn"
 ; :tx-data-fname               "tx-data.edn"

   :invoke-fn             enc-response.proc/init-enc-response-files->datomic
}
ENDEND

java -jar /Users/athom555/work/enc-response-parse/target/enc-response-parse-1.0.0-SNAPSHOT-standalone.jar  /tmp/ctx.edn 

