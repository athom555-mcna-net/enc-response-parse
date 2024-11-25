#!/bin/bash 

cat > /tmp/ctx.edn <<ENDEND
{  
    :invoke-fn             enc-response.proc/tx-data-file->datomic
    :db-uri                <prod uri>
    :max-tx-size           1000

    :tx-data-fname         "tx-data.edn"
}
ENDEND

java -jar /Users/athom555/work/enc-response-parse/target/enc-response-parse-1.0.0-SNAPSHOT-standalone.jar  /tmp/ctx.edn 

