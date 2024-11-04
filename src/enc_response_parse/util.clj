(ns enc-response-parse.util
  (:require
    [datomic.api :as d.peer] ; Datomic Peer API
    [schema.core :as s]
    [tupelo.core :as t]
    [tupelo.math :as math]
    [tupelo.schema :as tsk]
    ))

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

;-----------------------------------------------------------------------------
(s/defn iowa-prefix? :- s/Bool
  [s :- s/Str]
  (t/with-exception-default false ; in case less than 3 chars
    (t/truthy? (= "ia-" (subs s 0 3)))))

;-----------------------------------------------------------------------------
(s/defn transact-seq-peer :- tsk/Vec
  "Accepts a sequence of transactions into Datomic, which are committed in order.
  Each transaction is a vector of entity maps."
  [conn :- s/Any ; Datomic connection
   txs :- [[tsk/KeyMap]]]
  (reduce
    (fn [cum tx]
      (conj cum
        (d.peer/transact conn tx))) ; uses Datomic Peer API
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

