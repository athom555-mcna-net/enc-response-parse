(ns enc-response-parse.util
  (:require
    [datomic.api :as d.peer]
    [schema.core :as s]
    [tupelo.core :as t]
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
        (d.peer/transact conn tx))) ; uses Datomic Peer API
    []
    txs))

(s/defn transact-seq-peer-with :- datomic.db.Db
  "Accepts a sequence of transactions into Datomic, which are committed in order.
  Each transaction is a vector of entity maps."
  [conn :- s/Any ; Datomic connection
   txs :- [[tsk/KeyMap]]]
  (loop [db  (d.peer/db conn)
         txs txs]
    (if (empty? txs)
      db
      (let [tx-curr  (t/xfirst txs)
            txs-next (t/xrest txs)
            result   (d.peer/with db tx-curr) ; use Datomic Peer API
            db-next  (t/grab :db-after result)]
        (recur db-next txs-next)))))

