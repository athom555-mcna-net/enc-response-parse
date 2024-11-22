(ns tst.enc-response.datomic
  (:use enc-response.datomic
        tupelo.core
        tupelo.test)
  (:require
    [clojure.data :as data]
    [clojure.java.io :as io]
    [clojure.pprint :as pp]
    [datomic.api :as d.peer]
    [enc-response.parse :as parse]
    [enc-response.proc :as proc]
    [tupelo.string :as str]
    [tupelo.test.jvm :as ttj]
    ))

; Enable to see progress printouts
(def ^:dynamic verbose-tests?
  false)

; Defines URI for local Peer transactor in `dev` mode. Uses `data-dir` in transactor *.properties file.
; Default entry `data-dir=data` => /opt/datomic/data/...
; Absolute path entry like `data-dir=/Users/myuser/datomic-data` => that directory.
(def db-uri "datomic:dev://localhost:4334/enc-response-test")

(ttj/define-fixture :each
  {:enter (fn [ctx]
            (cond-it-> (validate boolean? (d.peer/delete-database db-uri)) ; returns true/false
              verbose-tests? (println "  Deleted prior db: " it))
            (cond-it-> (validate boolean? (d.peer/create-database db-uri))
              verbose-tests? (println "  Creating db:      " it)))
   :leave (fn [ctx]
            (cond-it-> (validate boolean? (d.peer/delete-database db-uri))
              verbose-tests? (println "  Deleting db:      " it)))})

(verify
  (let [data     [:a 2 {:c {:db/id 999 :d 4}}]
        expected [:a 2 {:c {:d 4}}]]
    (is= (elide-db-id data) expected) ; removes mapentry of {:db/id 999}
    (is= (elide-db-id expected) expected)) ; idempotent
  (let [data [:a 2 {:c [:db/id 999] :d 4}]]
    (is= (elide-db-id data) data))) ; 2-element vector is not a MapEntry, so unaffected

; parse data from first encounter response file => datomic
(verify
  (let [ctx-local       {:db-uri                      db-uri

                         ; full data: "/Users/athom555/work/iowa-response"
                         :encounter-response-root-dir "./enc-response-files-test-small" ; full data:  "/Users/athom555/work/iowa-response"
                         :missing-icn-fname           "resources/missing-icns-prod-small.edn"
                         :icn-maps-aug-fname          "icn-maps-aug.edn"
                         :tx-data-fname               "tx-data.edn"
                         :tx-size-limit               2}

        enc-resp-fnames (proc/get-enc-response-fnames ctx-local)
        fname-first     (xfirst enc-resp-fnames)]

    ; verify parsed all lines => records from first file
    (let [data-recs (parse/enc-response-fname->parsed fname-first)
          rec-1     (xfirst data-recs)
          rec-5     (xlast data-recs)]
      (is= 5 (count data-recs))

      (enc-response-datomic-init ctx-local)
      (proc/enc-response-recs->datomic ctx-local data-recs) ; commit records

      ; verify can retrieve first & last records from datomic
      (let [conn (d.peer/connect db-uri)
            db   (d.peer/db conn)]
        (let [result (only2 (d.peer/q '[:find (pull ?e [*])
                                        :where [?e :mco-claim-number "30000000100601"]] ; rec-1
                              db))]
          (is (submatch? rec-1 result)))
        (let [result (only2 (d.peer/q '[:find (pull ?e [*])
                                        :where [?e :mco-claim-number "30000062649897"]] ; rec-5
                              db))]
          (is (submatch? rec-5 result)))))))

