(ns enc-response-parse.parse-to-datomic
  (:use tupelo.core
        tupelo.test)
  (:require
    [clojure.pprint :as pp]
    [datomic.api :as d]
    [demo.util :as u]
    [schema.core :as s]
    [tst.conf :as conf]
    [tupelo.core :as t]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    [tupelo.test.jvm :as ttj]
    ))

(def verbose? true)

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

