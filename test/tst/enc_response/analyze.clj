(ns tst.enc-response.analyze
  (:use tupelo.core
        tupelo.test)
  (:require
    [clojure.data :as data]
    [clojure.java.io :as io]
    [clojure.pprint :as pp]
    [datomic.api :as d.peer]
    [enc-response.datomic :as datomic]
    [schema.core :as s]
    [tupelo.csv :as csv]
    [tupelo.io :as tio]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    [tupelo.test.jvm :as ttj]
    )
  (:import
    [java.io File]
    ))


(verify-focus
  (let [ctx {:db-uri "datomic:dev://localhost:4334/enc-response-full"
             }
        ]
    (spyx (datomic/count-enc-response-recs ctx))

    (comment
      ; Query datomic to verify can retrieve records
      (with-map-vals ctx [db-uri]
        (let [conn (d.peer/connect db-uri)
              db   (d.peer/db conn)]
          (let [raw-result (only2 (d.peer/q '[:find (pull ?e [*])
                                              :where [?e :mco-claim-number "30000062649905"]]
                                    db))]
            (is (submatch? rec-1 raw-result)))
          (let [raw-result (only2 (d.peer/q '[:find (pull ?e [*])
                                              :where [?e :mco-claim-number "30000062649906"]]
                                    db))]
            (is (submatch? rec-2 raw-result))))))

    ))
