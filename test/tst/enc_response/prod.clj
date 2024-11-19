; Test functions & data before commit to production server

(ns tst.enc-response.prod
  (:use tupelo.core
        tupelo.test)
  (:require
    [clojure.data :as data]
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

(verify
  (let [resp1       (datomic/enc-response-schema->datomic ctx-local) ; commit schema into datomic
        ; >>          (pp/pprint resp1)

        rec-1       {:mco-claim-number                "30000062649905"
                     :iowa-transaction-control-number "62133600780000013"
                     :iowa-processing-date            "12022021"
                     :claim-type                      "D"
                     :claim-frequency-code            "7"
                     :member-id                       "1704114C"
                     :first-date-of-service           "03112021"
                     :billing-provider-npi            "1952711780"
                     :mco-paid-date                   "11242021"
                     :total-paid-amount               "000000022968"
                     :line-number                     "00"
                     :error-code                      "A00"
                     :field                           "PAID"
                     :error-field-value               ""}
        rec-2       {:mco-claim-number                "30000062649906"
                     :iowa-transaction-control-number "62133600780000014"
                     :iowa-processing-date            "12022021"
                     :claim-type                      "D"
                     :claim-frequency-code            "1"
                     :member-id                       "1704114C"
                     :first-date-of-service           "07012021"
                     :billing-provider-npi            "1952711780"
                     :mco-paid-date                   "11242021"
                     :total-paid-amount               "000000000000"
                     :line-number                     "00"
                     :error-code                      "A00"
                     :field                           "DENIED"
                     :error-field-value               ""}
        sample-recs [rec-1
                     rec-2]]
    (datomic/enc-response-recs->datomic ctx-local sample-recs)

    ; Query datomic to verify can retrieve records
    (with-map-vals ctx-local [db-uri]
      (let [conn (d.peer/connect db-uri)
            db   (d.peer/db conn)]
        (let [raw-result (only2 (d.peer/q '[:find (pull ?e [*])
                                            :where [?e :mco-claim-number "30000062649905"]]
                                  db))]
          (is (submatch? rec-1 raw-result)))
        (let [raw-result (only2 (d.peer/q '[:find (pull ?e [*])
                                            :where [?e :mco-claim-number "30000062649906"]]
                                  db))]
          (is (submatch? rec-2 raw-result)))))))

(verify-focus
  (datomic/enc-response-schema->datomic ctx-local)
  (let [
        rec-1       {:mco-claim-number                "30000062649905"
                     :iowa-transaction-control-number "62133600780000013"
                     :iowa-processing-date            "12022021"
                     :claim-type                      "D"
                     :claim-frequency-code            "7"
                     :member-id                       "1704114C"
                     :first-date-of-service           "03112021"
                     :billing-provider-npi            "1952711780"
                     :mco-paid-date                   "11242021"
                     :total-paid-amount               "000000022968"
                     :line-number                     "00"
                     :error-code                      "A00"
                     :field                           "PAID"
                     :error-field-value               ""}
        rec-2       {:mco-claim-number                "30000062649906"
                     :iowa-transaction-control-number "62133600780000014"
                     :iowa-processing-date            "12022021"
                     :claim-type                      "D"
                     :claim-frequency-code            "1"
                     :member-id                       "1704114C"
                     :first-date-of-service           "07012021"
                     :billing-provider-npi            "1952711780"
                     :mco-paid-date                   "11242021"
                     :total-paid-amount               "000000000000"
                     :line-number                     "00"
                     :error-code                      "A00"
                     :field                           "DENIED"
                     :error-field-value               ""}
        sample-recs [rec-1
                     rec-2]]
    (datomic/enc-response-recs->datomic ctx-local sample-recs)

    ; Query datomic to verify can retrieve records
    (with-map-vals ctx-local [db-uri]
      (let [conn (d.peer/connect db-uri)
            db   (d.peer/db conn)]
        (let [raw-result (only2 (d.peer/q '[:find (pull ?e [*])
                                            :where [?e :mco-claim-number "30000062649905"]]
                                  db))]
          (is (submatch? rec-1 raw-result)))
        (let [raw-result (only2 (d.peer/q '[:find (pull ?e [*])
                                            :where [?e :mco-claim-number "30000062649906"]]
                                  db))]
          (is (submatch? rec-2 raw-result)))))))


