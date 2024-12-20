#!/bin/bash 

# Copy partial recs from missing ICN list to init fake datomic db (standin for prod).

cat > /tmp/ctx.edn <<ENDEND
{ :invoke-fn                      enc-response.proc/init-enc-response->datomic
  :db-uri                         "datomic:dev://localhost:4334/enc-response-full"

  :encounter-response-root-dir    "/Users/athom555/work/iowa-response"
  :max-tx-size                    10000
}
ENDEND

time clojure -X:run :opts-file '"/tmp/ctx.edn"'

