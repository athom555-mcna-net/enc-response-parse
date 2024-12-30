(ns enc-response.schemas
  (:use tupelo.core
        tupelo.test)
  (:require
    [schema.core :as s]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    ))


(s/def prod-missing-icns :- [tsk/KeyMap]
  "Describes data extracted from prod records with missing value for `plan-icn`"
  [{:db/ident :encounter-transmission.status/accepted}
   {:db/ident :encounter-transmission.status/rejected}

   {:db/ident       :encounter-transmission/icn :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident       :encounter-transmission/previous-icn :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident       :encounter-transmission/plan :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident       :encounter-transmission/plan-icn :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   ; Points to :encounter-transmission.status/*
   {:db/ident       :encounter-transmission/status :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}])

(s/def encounter-response :- [tsk/KeyMap]
  "Describes fields in Encounter Response file (see NS enc-response.parse)"
  [{:db/ident     :mco-claim-number
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
    :db/valueType :db.type/string :db/cardinality :db.cardinality/one}

   {:db/ident     :fname-str
    :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   ])

