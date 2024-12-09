#!/bin/bash 

cat > /tmp/ctx.edn <<ENDEND
{ :invoke-fn                    enc-response.proc/init-enc-response-files->updates-tsv

  :encounter-response-root-dir  "/Users/athom555/work/iowa-response" ; full data
  :plan-icn-update-tsv-fname    "./plan-icn-update-full.tsv"

; :encounter-response-root-dir  "./enc-response-files-test-small" ; full data:  "/Users/athom555/work/iowa-response"
; :plan-icn-update-tsv-fname    "./plan-icn-update-small.tsv"

  :db-uri  "dummy"   ; if nil or missing, derived from other above

}
ENDEND

java -jar /Users/athom555/work/enc-response-parse/target/enc-response-parse-1.0.0-SNAPSHOT-standalone.jar  /tmp/ctx.edn 

