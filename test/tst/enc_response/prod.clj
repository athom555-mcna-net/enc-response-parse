; Test functions & data before commit to production server
(ns       ; ^:test-refresh/focus
  tst.enc-response.prod
  (:use enc-response.prod
        tupelo.core
        tupelo.test)
  (:require
    [clojure.data :as data]
    [clojure.edn :as edn]
    [clojure.java.io :as io]
    [clojure.pprint :as pp]
    [datomic.api :as d.peer]
    [enc-response.datomic :as datomic]
    [enc-response.schemas :as schemas]
    [schema.core :as s]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    [tupelo.test.jvm :as ttj]
    ))

; Enable to see progress printouts
(def ^:dynamic verbose-tests?
  false)

; Defines URI for local transactor in `dev` mode. Uses `data-dir` in transactor *.properties file.
; Default entry `data-dir=data` => /opt/datomic/data/...
; Absolute path entry like `data-dir=/Users/myuser/datomic-data` => that directory.
(def db-uri "datomic:dev://localhost:4334/enc-response-test")

(ttj/define-fixture :each
  {:enter (fn [ctx]
            (cond-it-> (d.peer/delete-database db-uri) ; returns true/false
              verbose-tests? (println "  Deleted prior db: " it))
            (cond-it-> (d.peer/create-database db-uri)
              verbose-tests? (println "  Creating db:      " it)))
   :leave (fn [ctx]
            (cond-it-> (d.peer/delete-database db-uri)
              verbose-tests? (println "  Deleting db:      " it)))})

; Add 2 records to datomic using 2 different syntaxes. Verify query results.
(verify
  (let [ctx {:db-uri                      db-uri
             :tx-size-limit               3

             :encounter-response-root-dir "./enc-response-files-test-small" ; full data:  "/Users/athom555/work/iowa-response"
             :missing-icn-fname           "resources/missing-icns-prod-small.edn"
             :icn-maps-aug-fname          "icn-maps-aug.edn"
             :tx-data-fname               "tx-data.edn"}]
    (with-map-vals ctx [db-uri]
      ; create empty db
      (d.peer/delete-database db-uri)
      (d.peer/create-database db-uri)

      ; add schema
      (datomic/peer-transact-entities db-uri schemas/prod-missing-icns)

      ; add sample records.  Note 2 diffenent ways to specify sub-entity ":encounter-transmission/status"
      (let [test-entities [; specify :encounter-transmission.status/accepted as `ident` value
                           {:encounter-transmission/icn    "30000019034534"
                            :encounter-transmission/plan   "ia-medicaid"
                            :encounter-transmission/status :encounter-transmission.status/accepted}

                           ; specify :encounter-transmission.status/rejected as sub-entity
                           {:encounter-transmission/icn    "30000019034535"
                            :encounter-transmission/plan   "ia-medicaid"
                            :encounter-transmission/status {:db/ident :encounter-transmission.status/rejected}}]

            resp1         (datomic/peer-transact-entities db-uri test-entities) ; add entities to datomic

            ; query datomic for all entities & verify present in DB
            conn          (d.peer/connect db-uri)
            db            (d.peer/db conn)
            result        (onlies (d.peer/q '[:find (pull ?eid [:db/id
                                                                :encounter-transmission/icn
                                                                :encounter-transmission/plan
                                                                {:encounter-transmission/status [*]}])
                                              :where [?eid :encounter-transmission/icn]]
                                    db))]
        ; Use `submatch?` to ignore :db/id values
        (is (submatch? [{:encounter-transmission/icn  "30000019034534"
                         :encounter-transmission/plan "ia-medicaid"
                         :encounter-transmission/status
                         #:db{:ident :encounter-transmission.status/accepted}}
                        {:encounter-transmission/icn  "30000019034535"
                         :encounter-transmission/plan "ia-medicaid"
                         :encounter-transmission/status
                         #:db{:ident :encounter-transmission.status/rejected}}]
              result))))))

; Add 20 missing ICN entities to Datomic, extract, and elide the :db/id values
(verify
  (let [ctx {:db-uri                      db-uri
             :tx-size-limit               3

             :encounter-response-root-dir "./enc-response-files-test-small" ; full data:  "/Users/athom555/work/iowa-response"
             :missing-icn-fname           "resources/missing-icns-prod-small.edn"
             :icn-maps-aug-fname          "icn-maps-aug.edn"
             :tx-data-fname               "tx-data.edn"}]

    (init-missing-icns->datomic ctx) ; transact sample records into test DB

    (let [result       (datomic/elide-db-id
                         (onlies (d.peer/q '[:find (pull ?eid [:db/id
                                                               :encounter-transmission/icn
                                                               :encounter-transmission/plan
                                                               {:encounter-transmission/status [*]}])
                                             :where [?eid :encounter-transmission/icn]]
                                   (datomic/curr-db db-uri))))
          first-5-recs (it-> result
                         (sort-by :encounter-transmission/icn it)
                         (xtake 5 it))]
      (is= 20 (count result))
      (is= first-5-recs
        [#:encounter-transmission{:icn    "30000019034534"
                                  :plan   "ia-medicaid"
                                  :status #:db{:ident :encounter-transmission.status/accepted}}
         #:encounter-transmission{:icn    "30000019034535"
                                  :plan   "ia-medicaid"
                                  :status #:db{:ident :encounter-transmission.status/accepted}}
         #:encounter-transmission{:icn    "30000019034536"
                                  :plan   "ia-medicaid"
                                  :status #:db{:ident :encounter-transmission.status/accepted}}
         #:encounter-transmission{:icn    "30000019034537"
                                  :plan   "ia-medicaid"
                                  :status #:db{:ident :encounter-transmission.status/accepted}}
         #:encounter-transmission{:icn    "30000019034538"
                                  :plan   "ia-medicaid"
                                  :status #:db{:ident :encounter-transmission.status/accepted}}]))))

