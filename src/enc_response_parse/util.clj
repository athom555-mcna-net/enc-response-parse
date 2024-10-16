(ns enc-response-parse.util
  (:require
    [schema.core :as s]
    [tupelo.math :as math]
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
