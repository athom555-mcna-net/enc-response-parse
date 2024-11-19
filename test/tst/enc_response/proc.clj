(ns tst.enc-response.proc
  (:use enc-response.proc
        tupelo.core
        tupelo.test)
  (:require
    [clojure.data :as data]
    [clojure.java.io :as io]
    [clojure.pprint :as pp]
    [datomic.api :as d.peer]
    [enc-response.datomic :as datomic]
    [enc-response.parse :as parse]
    [tupelo.string :as str]
    [tupelo.test.jvm :as ttj]
    ))

(comment
  (verify
    (let [ctx {:db-uri             "datomic:dev://localhost:4334/enc-response"
               :tx-size-limit      500
               :missing-icn-fname  "resources/missing-icns-prod-small.edn"
               :icn-maps-aug-fname "icn-maps-aug.edn"}]
      (with-map-vals ctx [db-uri]
        (spyx (count-enc-response-recs ctx))
        (let [conn (d.peer/connect db-uri)
              db   (d.peer/db conn)
              rec  (enc-response-query-icn->plan-icn db "30000019034534") ; missing file => :encounter-transmission/icn
              ]
          (spyx-pretty rec)
          )))))

; search for ICNs with multiple encounter response records
(comment
  (verify
    (let [ctx {:db-uri            "datomic:dev://localhost:4334/enc-response"
               :tx-size-limit     500
               :missing-icn-fname "resources/missing-icns-prod-small.edn"}]
      (spyx (datomic/count-enc-response-recs ctx))
      (enc-resp-disp-diff ctx))))

(verify
  (let [response-rec-1     {:billing-provider-npi            "1952711780"
                             :claim-frequency-code            "1"
                             :claim-type                      "D"
                             :error-code                      "A00"
                             :error-field-value               ""
                             :field                           "PAID"
                             :first-date-of-service           "00000000"
                             :iowa-processing-date            "04132017"
                             :iowa-transaction-control-number "61710200783000021"
                             :line-number                     "00"
                             :mco-claim-number                "30000019034555"
                             :mco-paid-date                   "00000000"
                             :member-id                       "1626808D"
                             :total-paid-amount               "000000017750"
                             :db/id                           17592186045438}

        response-rec-2     {:billing-provider-npi            "1952711780"
                             :claim-frequency-code            "7"
                             :claim-type                      "D"
                             :error-code                      "A00"
                             :error-field-value               ""
                             :field                           "PAID"
                             :first-date-of-service           "00000000"
                             :iowa-processing-date            "10012019"
                             :iowa-transaction-control-number "61927400780000019"
                             :line-number                     "00"
                             :mco-claim-number                "30000019034555"
                             :mco-paid-date                   "00000000"
                             :member-id                       "1626808D"
                             :total-paid-amount               "000000017750"
                             :db/id                           17592186233847}
        response-recs-multi [response-rec-1
                             response-rec-2]
        result      (resp-recs->newest response-recs-multi)]
    (is= result response-rec-2)))

; #working
(verify
    (let [ctx {:db-uri            "datomic:dev://localhost:4334/enc-response"
               :tx-size-limit     500
               :missing-icn-fname "/Users/athom555/work/missing-icns-prod-small.edn"
             ; :missing-icn-fname  "/Users/athom555/work/missing-icns-prod-orig.edn"
               :icn-maps-aug-fname "icn-maps-aug.edn"
               }]
      ; (spyx-pretty (enc-resp-disp-diff ctx))

      (let [icn-maps-aug (create-icn-maps-aug-datomic ctx)]
        (is (->> (xlast icn-maps-aug)
              (submatch? {:encounter-transmission/icn      "30000019034555",
                          :encounter-transmission/plan     "ia-medicaid",
                          :encounter-transmission/plan-icn "61927400780000019",
                          :encounter-transmission/status
                          #:db{:ident :encounter-transmission.status/accepted}}))))

      ))
