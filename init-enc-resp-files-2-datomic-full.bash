#!/bin/bash 

cat > /tmp/ctx.edn <<ENDEND
{   :db-uri               "datomic:dev://localhost:4334/enc-response"
    :max-tx-size        500

   :encounter-response-root-dir "/Users/athom555/work/iowa-response"

 ; :missing-icn-fname           "/Users/athom555/work/missing-icns-prod-small.edn"
 ; :icn-maps-aug-fname          "icn-maps-aug.edn"
 ; :tx-data-fname               "tx-data.edn"

   :invoke-fn             enc-response.proc/init-enc-response-files->datomic
}
ENDEND

java -jar /Users/athom555/work/enc-response-parse/target/enc-response-parse-1.0.0-SNAPSHOT-standalone.jar  /tmp/ctx.edn 

#
# Stats 2024-11-21 #awt
#
# :init-enc-response-files->datomic--num-recs 517590
#
# ---------------------------------------------------------------------------------------------------
# Profile Stats:
#    Samples       TOTAL        MEAN      SIGMA           ID
#       279       41.619     0.149171   0.176493   :enc-response-fname->parsed
#       279       41.889     0.150139   0.184451   :enc-response-recs->datomic
#         1       84.378    84.377685   0.000000   :init-enc-response-files->datomic
# ---------------------------------------------------------------------------------------------------
#
