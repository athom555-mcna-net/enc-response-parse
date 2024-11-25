#!/bin/bash 

cat > /tmp/ctx.edn <<MYDATA
{:db-uri               "datomic:dev://localhost:4334/enc-response"
 :max-tx-size        500

;:missing-icn-fname    "/Users/athom555/work/missing-icns-prod-small.edn"
 :missing-icn-fname    "/Users/athom555/work/missing-icns-prod-orig.edn"

;:invoke-fn  enc-response.proc/enc-resp-disp-diff
 :invoke-fn  enc-response.proc/enc-resp-mult-count
}
MYDATA

java -jar /Users/athom555/work/enc-response-parse/target/enc-response-parse-1.0.0-SNAPSHOT-standalone.jar  /tmp/ctx.edn 

