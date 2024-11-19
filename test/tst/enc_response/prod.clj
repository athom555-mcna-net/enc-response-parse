; Test functions & data before commit to production server

(ns tst.enc-response.prod
  (:use tupelo.core
        tupelo.test)
  (:require
    [clojure.data :as data]
    [clojure.edn :as edn]
    [clojure.java.io :as io]
    [clojure.pprint :as pp]
    [datomic.api :as d.peer]
    [enc-response.datomic :as datomic]
    [enc-response.parse :as parse]
    [enc-response.proc :as proc]
    [tupelo.string :as str]
    [tupelo.test.jvm :as ttj]
    ))

; Enable to see progress printouts
(def ^:dynamic verbose-tests?
  true)

; Defines URI for local transactor in `dev` mode. Uses `data-dir` in transactor *.properties file.
; Default entry `data-dir=data` => /opt/datomic/data/...
; Absolute path entry like `data-dir=/Users/myuser/datomic-data` => that directory.
(def db-uri-disk-test "datomic:dev://localhost:4334/enc-response-test")

(ttj/define-fixture :each
  {:enter (fn [ctx]
            (cond-it-> (validate boolean? (d.peer/delete-database db-uri-disk-test)) ; returns true/false
              verbose-tests? (println "  Deleted prior db: " it))
            (cond-it-> (validate boolean? (d.peer/create-database db-uri-disk-test))
              verbose-tests? (println "  Creating db:      " it)))
   :leave (fn [ctx]
            (cond-it-> (validate boolean? (d.peer/delete-database db-uri-disk-test))
              verbose-tests? (println "  Deleting db:      " it)))
   })

(def ctx-local
  {:encounter-response-root-dir "./enc-response-files-test-small" ; full data:  "/Users/athom555/work/iowa-response"
   :missing-icn-fname           "resources/missing-icns-prod-small.edn"
   :icn-maps-aug-fname          "icn-maps-aug.edn"
   :tx-data-chunked-fname       "tx-data-chuncked.edn"
   :tx-size-limit               2
   :db-uri                      db-uri-disk-test})

(def missing-icns-schema [
                          {:db/ident :encounter-transmission.status/accepted
                           ; :db/unique :db.unique/value
                           ; :db/unique :db.unique/identity
                           }
                          {:db/ident :encounter-transmission.status/rejected
                           ; :db/unique :db.unique/value
                           ; :db/unique :db.unique/identity
                           }

                          {:db/ident       :encounter-transmission/icn :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}

                          {:db/ident       :encounter-transmission/plan :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}

                          {:db/ident       :encounter-transmission/status :db/valueType :db.type/ref
                           :db/cardinality :db.cardinality/one}])

(verify
  (datomic/datomic-peer-transact-entities db-uri-disk-test missing-icns-schema)
  (let [rec1  {:encounter-transmission/icn    "30000019034534"
               :encounter-transmission/plan   "ia-medicaid"
               :encounter-transmission/status :encounter-transmission.status/accepted}
        rec2  {:encounter-transmission/icn    "30000019034535"
               :encounter-transmission/plan   "ia-medicaid"
               :encounter-transmission/status :encounter-transmission.status/rejected}
        resp1 (datomic/datomic-peer-transact-entities db-uri-disk-test [rec1 rec2])
        ]

    (let [conn   (d.peer/connect db-uri-disk-test)
          db     (d.peer/db conn)
          result (onlies (d.peer/q '[:find (pull ?eid [:db/id
                                                       :encounter-transmission/icn
                                                       :encounter-transmission/plan
                                                       {:encounter-transmission/status [*]}])
                                     :where [?eid :encounter-transmission/icn]]
                           db))]
      (is (submatch? [{:encounter-transmission/icn  "30000019034534"
                       :encounter-transmission/plan "ia-medicaid"
                       :encounter-transmission/status
                       #:db{:ident :encounter-transmission.status/accepted}}
                      {:encounter-transmission/icn  "30000019034535"
                       :encounter-transmission/plan "ia-medicaid"
                       :encounter-transmission/status
                       #:db{:ident :encounter-transmission.status/rejected}}]
            result)))))

(verify-focus
  (datomic/datomic-peer-transact-entities db-uri-disk-test missing-icns-schema)
  (let [txdata (edn/read-string (slurp "/Users/athom555/work/missing-icns-prod-small-txdata.edn"))
        resp1  (datomic/datomic-peer-transact-entities db-uri-disk-test txdata)

        db     (datomic/curr-db db-uri-disk-test)
        found  (onlies (d.peer/q '[:find (pull ?eid [:db/id
                                                     :encounter-transmission/icn
                                                     :encounter-transmission/plan
                                                     {:encounter-transmission/status [*]}])
                                   :where [?eid :encounter-transmission/icn]]
                         db))]
    (spyx-pretty found)
    ))
