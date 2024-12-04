#!/bin/bash 

cat > /tmp/ctx.edn <<ENDEND
{ :invoke-fn            enc-response.proc/create-icn-maps-aug->file

  :db-uri               "datomic:dev://localhost:4334/enc-response"

  :missing-icn-fname    "/Users/athom555/work/missing-icns-prod-small.edn"
  :icn-maps-aug-fname   "icn-maps-aug-prod-small.edn"
}
ENDEND

time java -jar /Users/athom555/work/enc-response-parse/target/enc-response-parse-1.0.0-SNAPSHOT-standalone.jar  /tmp/ctx.edn 
