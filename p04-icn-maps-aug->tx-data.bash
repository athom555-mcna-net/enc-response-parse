#!/bin/bash 

cat > /tmp/ctx.edn <<ENDEND
{   :invoke-fn             enc-response.proc/icn-maps-aug->tx-data

    :missing-icn-fname     "./missing-icns.edn"
    :icn-maps-aug-fname    "icn-maps-aug.edn"
    :tx-data-fname         "tx-data.edn"

}
ENDEND

java -jar /Users/athom555/work/enc-response-parse/target/enc-response-parse-1.0.0-SNAPSHOT-standalone.jar  /tmp/ctx.edn 

