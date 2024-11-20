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
(def db-uri-disk-test "datomic:dev://localhost:4334/enc-response-test")

(ttj/define-fixture :each
  {:enter (fn [ctx]
            (cond-it-> (validate boolean? (d.peer/delete-database db-uri-disk-test)) ; returns true/false
              verbose-tests? (println "  Deleted prior db: " it))
            (cond-it-> (validate boolean? (d.peer/create-database db-uri-disk-test))
              verbose-tests? (println "  Creating db:      " it)))
   :leave (fn [ctx]
            (cond-it-> (validate boolean? (d.peer/delete-database db-uri-disk-test))
              verbose-tests? (println "  Deleting db:      " it)))})

(def ctx-local
  {:db-uri                      db-uri-disk-test

   :encounter-response-root-dir "./enc-response-files-test-small" ; full data:  "/Users/athom555/work/iowa-response"
   :missing-icn-fname           "resources/missing-icns-prod-small.edn"
   :icn-maps-aug-fname          "icn-maps-aug.edn"
   :tx-data-fname               "tx-data.edn"
   :tx-size-limit               2})

(verify
  (let [data     [:a 2 {:c {:db/id 999 :d 4}}]
        expected [:a 2 {:c {:d 4}}]]
    (is= (elide-db-id data) expected) ; removes mapentry of {:db/id 999}
    (is= (elide-db-id expected) expected)) ; idempotent
  (let [data [:a 2 {:c [:db/id 999] :d 4}]]
    (is= (elide-db-id data) data))) ; 2-element vector is not a MapEntry, so unaffected

; parse data from first encounter response file => datomic
(verify
  ; full data: "/Users/athom555/work/iowa-response"
  (let [enc-resp-fnames (proc/get-enc-response-fnames ctx-local)
        fname-first     (xfirst enc-resp-fnames)]

    ; verify parsed all lines => records from first file
    (let [data-recs (parse/enc-response-fname->parsed fname-first)
          rec-1     (xfirst data-recs)
          rec-5     (xlast data-recs)]
      (is= 5 (count data-recs))

      (enc-response-schema->datomic ctx-local) ; commit schema
      (proc/enc-response-recs->datomic ctx-local data-recs) ; commit records

      ; verify can retrieve first & last records from datomic
      (let [conn (d.peer/connect db-uri-disk-test)
            db   (d.peer/db conn)]
        (let [result (only2 (d.peer/q '[:find (pull ?e [*])
                                        :where [?e :mco-claim-number "30000000100601"]] ; rec-1
                              db))]
          (is (submatch? rec-1 result)))
        (let [result (only2 (d.peer/q '[:find (pull ?e [*])
                                        :where [?e :mco-claim-number "30000062649897"]] ; rec-5
                              db))]
          (is (submatch? rec-5 result)))))))

; parse data from all encounter response files => datomic
(verify
  (enc-response-schema->datomic ctx-local) ; commit schema
  (proc/enc-response-files->datomic ctx-local)

  ; verify can retrieve first & last records from datomic
  (let [conn (d.peer/connect db-uri-disk-test)
        db   (d.peer/db conn)]
    (let [enc-resp-recs (onlies (d.peer/q '[:find (pull ?e [*])
                                            :where [?e :mco-claim-number]]
                                  db))
          recs-sorted   (vec (sort-by :mco-claim-number enc-resp-recs))]
      (is= (count enc-resp-recs) 14)
      (is= (xfirst recs-sorted)
        {:billing-provider-npi            "1952711780"
         :claim-frequency-code            "1"
         :claim-type                      "D"
         :error-code                      "A00"
         :error-field-value               ""
         :field                           "PAID"
         :first-date-of-service           "06302021"
         :iowa-processing-date            "12022021"
         :iowa-transaction-control-number "62133600780000001"
         :line-number                     "00"
         :mco-claim-number                "30000000100601"
         :mco-paid-date                   "08202021"
         :member-id                       "2610850C"
         :total-paid-amount               "000000004763"
         :db/id                           17592186045418})
      (is= (xlast recs-sorted)
        {:billing-provider-npi            "1952711780"
         :claim-frequency-code            "7"
         :claim-type                      "D"
         :error-code                      "A00"
         :error-field-value               ""
         :field                           "DENIED"
         :first-date-of-service           "08062021"
         :iowa-processing-date            "01062022"
         :iowa-transaction-control-number "62200600780000002"
         :line-number                     "00"
         :mco-claim-number                "30000063295501"
         :mco-paid-date                   "12292021"
         :member-id                       "3382022I"
         :total-paid-amount               "000000000000"
         :db/id                           17592186045438}))))

