; Test functions & data before commit to production server
(ns       ; ^:test-refresh/focus
  tst.enc-response.prod
  (:use tupelo.core
        tupelo.test)
  (:require
    [clojure.data :as data]
    [clojure.edn :as edn]
    [clojure.java.io :as io]
    [clojure.pprint :as pp]
    [datomic.api :as d.peer]
    [enc-response.datomic :as datomic]
    [enc-response.proc :as proc]
    [enc-response.schemas :as schemas]
    [schema.core :as s]
    [tupelo.schema :as tsk]
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

(def ctx-local
  {:db-uri                      db-uri-disk-test
   :tx-size-limit               3

   :encounter-response-root-dir "./enc-response-files-test-small" ; full data:  "/Users/athom555/work/iowa-response"
   :missing-icn-fname           "resources/missing-icns-prod-small.edn"
   :icn-maps-aug-fname          "icn-maps-aug.edn"
   :tx-data-chunked-fname       "tx-data-chuncked.edn"
   })

(ttj/define-fixture :each
  {:enter (fn [ctx]
            (spyx db-uri-disk-test)
            (cond-it-> (validate boolean? (d.peer/delete-database db-uri-disk-test)) ; returns true/false
              verbose-tests? (println "  Deleted prior db: " it))
            (cond-it-> (validate boolean? (d.peer/create-database db-uri-disk-test))
              verbose-tests? (println "  Creating db:      " it)))
   :leave (fn [ctx]
            (cond-it-> (validate boolean? (d.peer/delete-database db-uri-disk-test))
              verbose-tests? (println "  Deleting db:      " it)))
   })

(verify
  (with-map-vals ctx-local [db-uri]
    ; create empty db
    (nl)
    (spyx (datomic/peer-delete-db ctx-local))
    (spyx (d.peer/create-database db-uri))

    ; create schema
    (datomic/peer-transact-entities db-uri schemas/prod-missing-icns)

    ; add sample records
    (let [; specify :encounter-transmission.status/accepted as `ident`
          rec1   {:encounter-transmission/icn    "30000019034534"
                  :encounter-transmission/plan   "ia-medicaid"
                  :encounter-transmission/status :encounter-transmission.status/accepted}

          ; specify :encounter-transmission.status/rejectet as sub-entity
          rec2   {:encounter-transmission/icn    "30000019034535"
                  :encounter-transmission/plan   "ia-medicaid"
                  :encounter-transmission/status {:db/ident :encounter-transmission.status/rejected}
                  }
          resp1  (datomic/peer-transact-entities db-uri [rec1 rec2])

          conn   (d.peer/connect db-uri)
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

(verify
  (with-map-vals ctx-local [db-uri tx-size-limit]
    ; create empty db
    (datomic/peer-delete-db ctx-local)
    (d.peer/create-database db-uri)

    ; create schema
    (datomic/peer-transact-entities db-uri schemas/prod-missing-icns)

    ; add sample records
    (let [conn                 (d.peer/connect db-uri)

          missing-icns         (datomic/elide-db-id
                                 (proc/load-missing-icns ctx-local))
          missing-icns-chunked (partition-all tx-size-limit missing-icns)
          resp1                (datomic/peer-transact-entities-chunked conn missing-icns-chunked)


          result               (datomic/elide-db-id
                                 (onlies (d.peer/q '[:find (pull ?eid [:db/id
                                                                       :encounter-transmission/icn
                                                                       :encounter-transmission/plan
                                                                       {:encounter-transmission/status [*]}])
                                                     :where [?eid :encounter-transmission/icn]]
                                           (datomic/curr-db db-uri-disk-test))))]
      (is (->> (set result)
            (submatch?
              #{#:encounter-transmission{:icn "30000019034534"
                                         :plan "ia-medicaid"
                                         :status #:db{:ident :encounter-transmission.status/accepted}}
                #:encounter-transmission{:icn "30000019034535"
                                         :plan "ia-medicaid"
                                         :status #:db{:ident :encounter-transmission.status/accepted}}
                #:encounter-transmission{:icn "30000019034536"
                                         :plan "ia-medicaid"
                                         :status #:db{:ident :encounter-transmission.status/accepted}}
                #:encounter-transmission{:icn "30000019034537"
                                         :plan "ia-medicaid"
                                         :status #:db{:ident :encounter-transmission.status/accepted}}
                #:encounter-transmission{:icn "30000019034538"
                                         :plan "ia-medicaid"
                                         :status #:db{:ident :encounter-transmission.status/accepted}}} )))


      )))

(verify
  (datomic/peer-transact-entities db-uri-disk-test schemas/prod-missing-icns)
  (let [txdata (edn/read-string (slurp "/Users/athom555/work/missing-icns-prod-small-txdata.edn"))
        resp1  (datomic/peer-transact-entities db-uri-disk-test txdata)

        found  (onlies (d.peer/q '[:find (pull ?eid [:db/id
                                                     :encounter-transmission/icn
                                                     :encounter-transmission/plan
                                                     {:encounter-transmission/status [*]}])
                                   :where [?eid :encounter-transmission/icn]]
                         (datomic/curr-db db-uri-disk-test)))]
    (is (->> (xtake 5 found)
          (submatch?
            [{:encounter-transmission/icn  "30000019034534"
              :encounter-transmission/plan "ia-medicaid"
              :encounter-transmission/status
              #:db{:ident :encounter-transmission.status/accepted}}
             {:encounter-transmission/icn  "30000019034535"
              :encounter-transmission/plan "ia-medicaid"
              :encounter-transmission/status
              #:db{:ident :encounter-transmission.status/accepted}}
             {:encounter-transmission/icn  "30000019034536"
              :encounter-transmission/plan "ia-medicaid"
              :encounter-transmission/status
              #:db{:ident :encounter-transmission.status/accepted}}
             {:encounter-transmission/icn  "30000019034537"
              :encounter-transmission/plan "ia-medicaid"
              :encounter-transmission/status
              #:db{:ident :encounter-transmission.status/accepted}}
             {:encounter-transmission/icn  "30000019034538"
              :encounter-transmission/plan "ia-medicaid"
              :encounter-transmission/status
              #:db{:ident :encounter-transmission.status/accepted}}])))))
