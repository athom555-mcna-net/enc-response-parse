#!/bin/bash 

cat > /tmp/ctx.edn <<ENDTAG
{:db-uri               "datomic:dev://localhost:4334/enc-response"
;:tx-size-limit        500
 :tx-size-limit        3

 :missing-icn-fname    "/Users/athom555/work/missing-icns-prod-small.edn"
;:missing-icn-fname    "/Users/athom555/work/missing-icns-prod-orig.edn"

 :icn-maps-aug-fname    "icn-maps-aug.edn"
 :invoke-fn             enc-response.proc/create-tx-data-chunked
}
ENDTAG

java -jar /Users/athom555/work/enc-response-parse/target/enc-response-parse-1.0.0-SNAPSHOT-standalone.jar  /tmp/ctx.edn 

