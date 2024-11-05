(ns enc-response-parse.parse-to-datomic
  (:use tupelo.core
        tupelo.test)
  (:require
    [clojure.pprint :as pp]
    [datomic.api :as d]
    [enc-response-parse.core :as core]
    [schema.core :as s]
    [tupelo.core :as t]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    [tupelo.test.jvm :as ttj]
    ))

(def verbose? false)

; Defines URI for local transactor in `dev` mode. Uses `data-dir` in transactor *.properties file.
; Default entry `data-dir=data` => /opt/datomic/data/...
; Absolute path entry like `data-dir=/Users/myuser/datomic-data` => that directory.
(def ^:dynamic db-uri-disk "datomic:dev://localhost:4334/enc-response")

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

(s/defn enc-response-fname->lines :- [s/Str]
  [fname :- s/Str]
  (let [lines (it-> fname
                (slurp it)
                (str/split-lines it)
                (drop-if #(str/whitespace? % ) it))]
    lines))

(s/defn enc-response-fname->parsed :- [tsk/KeyMap]
  [fname :- s/Str]
  (let [data-recs (forv [line (enc-response-fname->lines fname)]
                    (core/parse-string-fields core/iowa-encounter-response-specs line))]
    data-recs))

(s/defn enc-resp-recs->datomic  :- s/Any
  [enc-resp-recs :- [tsk/KeyMap]]

  )
