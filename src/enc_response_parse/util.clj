(ns enc-response-parse.util
  (:require
    [clojure.java.io :as io]
    [clojure.pprint :as pp]
    [clojure.tools.reader.edn :as edn]
    [datomic.api :as d.peer]
   ;[datomic.client.api :as d.client]
    [schema.core :as s]
    [tupelo.math :as math]
    [tupelo.schema :as tsk]
    ))

; Define an EID-like value from Datomic (aka a :db/id value) as any positive integer with at
; least 9 digits.

(s/def eid-min-digits :- s/Int
  "The minimum length positive int to be 'EID-like' (Datomic Entity ID)"
  9)                ; 32 bit Long is about +/- 2e9

(s/def eid-min-value :- BigInteger
  "The minimum value positive int to be 'EID-like' (Datomic Entity ID)"
  (math/pow->BigInteger 10 eid-min-digits))

(s/defn eid? :- s/Bool
  "Is an int 'EID-like' (Datomic Entity ID), i.e. large positive integer?"
  [v :- s/Int] (<= eid-min-value v))

;-----------------------------------------------------------------------------
(s/defn transact-seq-peer :- tsk/Vec
  "Accepts a sequence of transactions into Datomic, which are committed in order.
  Each transaction is a vector of entity maps."
  [conn :- s/Any ; Datomic connection
   txs :- [[tsk/KeyMap]]]
  (reduce
    (fn [cum tx]
      (conj cum
        (d.peer/transact conn {:tx-data tx}))) ; uses Datomic Client API
    []
    txs))

(comment
  (s/defn transact-seq-client :- tsk/Vec
    "Accepts a sequence of transactions into Datomic, which are committed in order.
    Each transaction is a vector of entity maps."
    [conn :- s/Any ; Datomic connection
     txs :- [[tsk/KeyMap]]]
    (reduce
      (fn [cum tx]
        (conj cum
          (d.client/transact conn {:tx-data tx}))) ; uses Datomic Client API
      []
      txs)))

