#!/bin/bash 

cat > /tmp/ctx.edn <<ENDEND
{ :invoke-fn            enc-response.prod/save-icn-recs-datomic->missing-file
  :db-uri               "datomic:dev://localhost:4334/missing-icns-fake"
  :missing-icn-fname    "./missing-icns-final.edn"
}
ENDEND

time java -jar /Users/athom555/work/enc-response-parse/target/enc-response-parse-1.0.0-SNAPSHOT-standalone.jar  /tmp/ctx.edn 

