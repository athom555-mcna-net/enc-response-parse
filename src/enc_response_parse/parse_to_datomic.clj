(ns enc-response-parse.parse-to-datomic
  (:use tupelo.core
        tupelo.test)
  (:require
    [clojure.pprint :as pp]
    [datomic.api :as d.peer]
    [enc-response-parse.core :as core]
    [schema.core :as s]
    [tupelo.core :as t]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    [tupelo.test.jvm :as ttj]
    ))

(def ^:dynamic verbose?
  "Enable to see progress printouts"
  false)

;---------------------------------------------------------------------------------------------------
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

;---------------------------------------------------------------------------------------------------
(def enc-response-schema [{:db/ident     :mco-claim-number
                           :db/valueType :db.type/string :db/cardinality :db.cardinality/one}

                          {:db/ident     :iowa-transaction-control-number
                           :db/valueType :db.type/string :db/cardinality :db.cardinality/one}

                          {:db/ident     :iowa-processing-date
                           :db/valueType :db.type/string :db/cardinality :db.cardinality/one}

                          {:db/ident     :claim-type
                           :db/valueType :db.type/string :db/cardinality :db.cardinality/one}

                          {:db/ident     :claim-frequency-code
                           :db/valueType :db.type/string :db/cardinality :db.cardinality/one}

                          {:db/ident     :member-id
                           :db/valueType :db.type/string :db/cardinality :db.cardinality/one}

                          {:db/ident     :first-date-of-service
                           :db/valueType :db.type/string :db/cardinality :db.cardinality/one}

                          {:db/ident     :billing-provider-npi
                           :db/valueType :db.type/string :db/cardinality :db.cardinality/one}

                          {:db/ident     :mco-paid-date
                           :db/valueType :db.type/string :db/cardinality :db.cardinality/one}

                          {:db/ident     :total-paid-amount
                           :db/valueType :db.type/string :db/cardinality :db.cardinality/one}

                          {:db/ident     :line-number
                           :db/valueType :db.type/string :db/cardinality :db.cardinality/one}

                          {:db/ident     :error-code
                           :db/valueType :db.type/string :db/cardinality :db.cardinality/one}

                          {:db/ident     :field
                           :db/valueType :db.type/string :db/cardinality :db.cardinality/one}

                          {:db/ident     :error-field-value
                           :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])

(s/defn enc-response-schema->datomic :- s/Any
  "Transact the schema for encounter response records into Datomic"
  [ctx :- tsk/KeyMap]
  (with-map-vals ctx [db-uri]
    (let [conn (d.peer/connect db-uri)
          resp @(d.peer/transact conn enc-response-schema)]
      resp)))

(s/defn enc-response-fname->lines :- [s/Str]
  [fname :- s/Str]
  (let [lines (it-> fname
                (slurp it)
                (str/split-lines it)
                (drop-if #(str/whitespace? %) it))]
    lines))

(s/defn enc-response-fname->parsed :- [tsk/KeyMap]
  [fname :- s/Str]
  (let [data-recs (forv [line (enc-response-fname->lines fname)]
                    (core/parse-string-fields core/iowa-encounter-response-specs line))]
    data-recs))

(s/defn enc-resp-recs->datomic :- s/Any
  [ctx :- tsk/KeyMap
   enc-resp-recs :- [tsk/KeyMap]]
  (with-map-vals ctx [db-uri tx-size-limit]
    (let [enc-resp-rec-chunked (partition-all tx-size-limit enc-resp-recs)
          conn (d.peer/connect db-uri)
          resp @(d.peer/transact conn enc-resp-recs)]
      resp)))
