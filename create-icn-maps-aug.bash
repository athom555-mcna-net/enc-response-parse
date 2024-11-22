#!/bin/bash 

cat > /tmp/ctx.edn <<ENDEND
{ :db-uri               "datomic:dev://localhost:4334/enc-response"
  :tx-size-limit        1000

  :missing-icn-fname           "./missing-icns-test.edn"
; :missing-icn-fname           "/Users/athom555/work/missing-icns-prod-small.edn"
  :icn-maps-aug-fname          "icn-maps-aug-test.edn"

  :invoke-fn             enc-response.proc/create-icn-maps-aug-datomic
}
ENDEND

java -jar /Users/athom555/work/enc-response-parse/target/enc-response-parse-1.0.0-SNAPSHOT-standalone.jar  /tmp/ctx.edn 

