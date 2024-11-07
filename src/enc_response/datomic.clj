(ns enc-response.datomic
  (:use tupelo.core
        tupelo.test)
  (:require
    [clojure.pprint :as pp]
    [datomic.api :as d.peer]
    [enc-response.util :as util]
    [schema.core :as s]
    [tupelo.core :as t]
    [tupelo.math :as math]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    [tupelo.test.jvm :as ttj]
    ))

(def ^:dynamic verbose?
  "Enable to see progress printouts"
  false)

;-----------------------------------------------------------------------------
; Define an EID-like value from Datomic (aka a :db/id value) as any positive integer with at
; least 9 digits.

(s/def eid-min-digits :- s/Int
  "The minimum length positive int to be 'EID-like' (Datomic Entity ID)"
  9)      ; 32 bit Long is about +/- 2e9

(s/def eid-min-value :- BigInteger
  "The minimum value positive int to be 'EID-like' (Datomic Entity ID)"
  (math/pow->BigInteger 10 eid-min-digits))

(s/defn eid? :- s/Bool
  "Is an int 'EID-like' (Datomic Entity ID), i.e. large positive integer?"
  [v :- s/Int] (<= eid-min-value v))

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

;-----------------------------------------------------------------------------
(s/defn transact-seq-peer :- tsk/Vec
  "Accepts a sequence of transactions into Datomic, which are committed in order.
  Each transaction is a vector of entity maps."
  [conn :- s/Any ; Datomic connection
   txs :- [[tsk/KeyMap]]]
  (reduce
    (fn [cum tx]
      (conj cum
        @(d.peer/transact conn tx))) ; uses Datomic Peer API
    []
    txs))

(s/defn transact-seq-peer-with :- datomic.db.Db
  "Accepts a sequence of transactions into Datomic, which are committed in order.
  Each transaction is a vector of entity maps."
  [conn :- s/Any ; Datomic connection
   txs :- [[tsk/KeyMap]]]
  (loop [db-curr  (d.peer/db conn)
         txs-curr txs]
    (if (empty? txs-curr)
      db-curr
      (let
        [tx-curr  (t/xfirst txs-curr)
         txs-next (t/xrest txs-curr)
         result   (d.peer/with db-curr tx-curr) ; transact data into db-curr
         db-next  (t/grab :db-after result)]
        (recur db-next txs-next)))))

;-----------------------------------------------------------------------------
(s/defn enc-response-schema->datomic :- s/Any
  "Transact the schema for encounter response records into Datomic"
  [ctx :- tsk/KeyMap]
  (with-map-vals ctx [db-uri]
    (let [conn (d.peer/connect db-uri)
          resp @(d.peer/transact conn enc-response-schema)]
      resp)))

(s/defn enc-response-recs->datomic :- s/Any
  [ctx :- tsk/KeyMap
   enc-resp-recs :- [tsk/KeyMap]]
  (with-map-vals ctx [db-uri tx-size-limit]
    (let [enc-resp-rec-chunked (partition-all tx-size-limit enc-resp-recs)
          conn (d.peer/connect db-uri)
          resp  (transact-seq-peer  conn enc-resp-rec-chunked)]
      ; (pp/pprint resp )
      resp)))
