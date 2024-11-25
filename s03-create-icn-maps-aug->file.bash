#!/bin/bash 

cat > /tmp/ctx.edn <<ENDEND
{  :db-uri                "datomic:dev://localhost:4334/missing-icns-test"
   :tx-size-limit         100

   :missing-icn-fname     "./missing-icns-test.edn"
   :icn-maps-aug-fname    "icn-maps-aug.edn"
   :tx-data-fname         "tx-data.edn"

   :invoke-fn             enc-response.prod/save-icn-recs-datomic->missing-file
}
ENDEND

java -jar /Users/athom555/work/enc-response-parse/target/enc-response-parse-1.0.0-SNAPSHOT-standalone.jar  /tmp/ctx.edn 

