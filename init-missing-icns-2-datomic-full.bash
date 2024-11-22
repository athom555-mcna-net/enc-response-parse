#!/bin/bash 

cat > /tmp/ctx.edn <<ENDEND
{  :db-uri               "datomic:dev://localhost:4334/missing-icns"
   :tx-size-limit        1000

   :missing-icn-fname           "/Users/athom555/work/missing-icns-prod-orig.edn"

 ; :encounter-response-root-dir "/shared/tmp/iowa/iowa_response_files"
 ; :icn-maps-aug-fname          "icn-maps-aug.edn"
 ; :tx-data-fname               "tx-data.edn"

   :invoke-fn             enc-response.prod/init-missing-icns->datomic
}
ENDEND

java -jar /Users/athom555/work/enc-response-parse/target/enc-response-parse-1.0.0-SNAPSHOT-standalone.jar  /tmp/ctx.edn

#
# Stats 2024-11-21 #awt
#
# :load-missing-icns "/Users/athom555/work/missing-icns-prod-orig.edn"
# :with-timer-print :load-missing-icns     2.522087
# :init-missing-icns->datomic--num-missing 414153
# :peer-transact-entities-impl-chunked--enter
# :peer-transact-entities-impl-chunked--tx 0
# :peer-transact-entities-impl-chunked--tx 1
# :peer-transact-entities-impl-chunked--tx 2
# :peer-transact-entities-impl-chunked--tx 3
# <snip>
# :peer-transact-entities-impl-chunked--tx 412
# :peer-transact-entities-impl-chunked--tx 413
# :peer-transact-entities-impl-chunked--tx 414
# :peer-transact-entities-impl-chunked--leave
# :with-timer-print :peer-transact-entities-impl-chunked    10.746659
# :with-timer-print :init-missing-icns->datomic--insert    10.747287
# :init-missing-icns->datomic--leave
# :with-timer-print :init-missing-icns->datomic    14.318086
