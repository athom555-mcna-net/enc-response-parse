#!/bin/bash 

# Copy partial recs from missing ICN list to init fake datomic db (standin for prod).

cat > /tmp/ctx.edn <<ENDEND
{ :invoke-fn           enc-response.prod/init-missing-icns->datomic
  :db-uri              "datomic:dev://localhost:4334/enc-response-small"

  :missing-icn-fname   "/Users/athom555/work/missing-icns-prod-small.edn"
  :max-tx-size         3
}
ENDEND

time java -jar /Users/athom555/work/enc-response-parse/target/enc-response-parse-1.0.0-SNAPSHOT-standalone.jar  /tmp/ctx.edn 

