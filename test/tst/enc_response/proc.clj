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

(def dashes "-----------------------------------------------------------------------------")

; search for ICNs with multiple encounter response records
(verify-focus
    (let [ctx {:db-uri             "datomic:dev://localhost:4334/enc-response"
               :tx-size-limit      500
               :missing-icn-fname  "resources/missing-icns-prod-small.edn"
               :icn-maps-aug-fname "icn-maps-aug.edn"}]
      (with-map-vals ctx [db-uri]
        (spyx (datomic/count-enc-response-recs ctx))
        (let [missing-recs (load-missing-icns ctx)
                conn (d.peer/connect db-uri)
                db   (d.peer/db conn)
                ;rec  (enc-response-query-icn->plan-icn db "30000019034534") ; missing file => :encounter-transmission/icn
                ]
          (doseq [missing-rec missing-recs]
            (nl)
            (println dashes)
            (let [icn (grab :encounter-transmission/icn missing-rec)
                  enc-resp-recs (datomic/icn->enc-response-recs db icn)
                  ]
              (spyx-pretty enc-resp-recs)
              (when (pos? (count enc-resp-recs))
                (doseq [alt (xrest enc-resp-recs)]
                  (nl)
                  (spyx-pretty (data/diff (xfirst enc-resp-recs) alt) ))))
            )
            )
        )))
