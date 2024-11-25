#!/bin/bash 

cat > /tmp/ctx.edn <<ENDEND
{ :invoke-fn            enc-response.prod/save-icn-recs-datomic->missing-file

  :missing-icn-fname    "./missing-icns-test.edn"
  :db-uri               "datomic:dev://localhost:4334/missing-icns-fake"

; :icn-maps-aug-fname   "icn-maps-aug.edn"
; :tx-data-fname        "tx-data.edn"
; :max-tx-size          1000 ; The maxinum number of entity maps to include in a single Datomic transaction.

; :encounter-response-root-dir "/shared/tmp/iowa/iowa_response_files"
}
ENDEND

java -jar /Users/athom555/work/enc-response-parse/target/enc-response-parse-1.0.0-SNAPSHOT-standalone.jar  /tmp/ctx.edn 

