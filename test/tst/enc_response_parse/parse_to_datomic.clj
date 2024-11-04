(ns tst.enc-response-parse.parse-to-datomic
  (:use enc-response-parse.parse-to-datomic
        tupelo.core
        tupelo.test)
  (:require
    [clojure.pprint :as pp]
    [datomic.api :as d]
    [schema.core :as s]
    [tupelo.core :as t]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    [tupelo.test.jvm :as ttj]
    ))

(def verbose? true)

; Defines URI for local transactor in `dev` mode. Uses `data-dir` in transactor *.properties file.
; Default entry `data-dir=data` => /opt/datomic/data/...
; Absolute path entry like `data-dir=/Users/myuser/datomic-data` => that directory.
(def db-uri-disk-test "datomic:dev://localhost:4334/enc-response-test")

(ttj/define-fixture :each
  {:enter (fn [ctx]
            (cond-it-> (validate boolean? (d/delete-database db-uri-disk-test)) ; returns true/false
              verbose? (println "  Deleted prior db: " it))
            (cond-it-> (validate boolean? (d/create-database db-uri-disk-test))
              verbose? (println "  Creating db:      " it)))
   :leave (fn [ctx]
            (cond-it-> (validate boolean? (d/delete-database db-uri-disk-test))
              verbose? (println "  Deleting db:      " it)))
   })

(def ctx-local
  {:encounter-response-root-dir "./enc-response-files-test-small" ; full data:  "/Users/athom555/work/iowa-response"
   :missing-icn-fname           "resources/missing-icns-5.edn"
   :icn-maps-aug-fname          "icn-maps-aug.edn"
   :tx-data-chunked-fname       "tx-data-chuncked.edn"
   :tx-size-limit               2
   })

(s/defn fn->vec-fn :- tsk/Fn
  "Vectorize a function, so that instead of operating on a scalar value,
  it operates on each value in a 1D array. Used twice, the resulting function operates
  on each value in a 2D array."
  [f :- tsk/Fn]
  (s/fn [block :- [s/Any]]
    (mapv f block)))

(s/defn array-1d->2d :- [[s/Any]]
  "Convert a 1D sequence to a 2D array (possibly ragged)."
  [row-size :- s/Int
   seq-1d :- [s/Any]]
  (partition-all row-size seq-1d))

(s/defn array-2d->1d :- [s/Any]
  "Concatenate rows of a 2D array (possibly ragged), returning a 1-D vector."
  [seq-2d :- [[s/Any]]]
  (apply glue seq-2d))

(verify
  (let [seq1       (range 5)
        arr1       (array-1d->2d 2 seq1)
        seq2       (array-2d->1d arr1)

        inc-1d     (fn->vec-fn inc)
        inc-2d     (fn->vec-fn inc-1d)

        seq1-inc-a (inc-1d seq1)
        arr1-inc   (inc-2d arr1)
        seq1-inc-b (array-2d->1d arr1-inc)
        ]
    (is= seq1 [0 1 2 3 4])
    (is= arr1 [[0 1]
               [2 3]
               [4]])
    (is= seq1 seq2)

    (is= seq1-inc-a [1 2 3 4 5])

    (is= arr1-inc [[1 2]
                   [3 4]
                   [5]])
    (is= seq1-inc-b [1 2 3 4 5])))

(verify
  (let [
        conn        (d/connect db-uri-disk-test)

        resp2       @(d/transact conn enc-response-schema)
        ; >>          (pp/pprint resp2)

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
                     rec-2]
        resp3       @(d/transact conn sample-recs)
        ; >>          (pp/pprint resp3)
        ]
    (let [db         (d/db conn)
          raw-result (only2 (d/q '[:find (pull ?e [*])
                                   :where [?e :mco-claim-number "30000062649905"]]
                              db))]
      (is (submatch? rec-1 raw-result)))
    (let [db         (d/db conn)
          raw-result (only2 (d/q '[:find (pull ?e [*])
                                   :where [?e :mco-claim-number "30000062649906"]]
                              db))]
      (is (submatch? rec-2 raw-result)))
    ))


