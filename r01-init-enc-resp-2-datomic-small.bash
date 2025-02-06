#!/bin/bash 

# Copy partial recs from missing ICN list to init fake datomic db (standin for prod).

cat > /tmp/ctx.edn <<ENDEND
{ :invoke-fn                      enc-response.proc/init-enc-response->datomic
  :db-uri                         "datomic:dev://localhost:4334/enc-response-test"

  :encounter-response-root-dir    "./enc-response-files-test-small"
  :max-tx-size                    3
}
ENDEND

clojure -X:run :opts-file '"/tmp/ctx.edn"'

