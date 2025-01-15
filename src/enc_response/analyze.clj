(ns enc-response.analyze
  (:use tupelo.core
        tupelo.test)
  (:require
    [datomic.api :as d.peer]
    [enc-response.datomic :as datomic]
    [schema.core :as s]
    [tupelo.profile :as prof]
    [tupelo.schema :as tsk]
    ))

(def db-uri "datomic:dev://localhost:4334/enc-response-full")
(def db (datomic/curr-db db-uri))

(s/defn keyfn-enc-resp :- tsk/Vec
  [a :- tsk/KeyMap]
  [(grab :fname-str a)
   (grab :mco-claim-number a)
   (grab :iowa-transaction-control-number a)])

(def all-recs
  (prof/with-timer-print :enc-response.analyze--all-recs
    (spyx :enc-response.analyze--all-recs-enter)
    (with-result
      (onlies (d.peer/q '[:find (pull ?e [*])
                          :where [?e :mco-claim-number]]
                db))
      (spyx :enc-response.analyze--all-recs-leave))))

(def all-recs-sorted
  (prof/with-timer-print :enc-response.analyze--all-recs-sort
    (vec (sort-by keyfn-enc-resp all-recs))))

(def mco-icn->recs
  (prof/with-timer-print :enc-response.analyze--group-by
    (group-by :mco-claim-number all-recs-sorted)))

(def mco-icn->count
  (prof/with-timer-print :enc-response.analyze--map-vals
    (map-vals mco-icn->recs count)))

(def mco-icn->count-1
  (prof/with-timer-print :enc-response.analyze--count-1
    (into {} (for [[mco-num recs] mco-icn->recs
                   :let [recs-cnt (count recs)]
                   :when (= 1 recs-cnt)]
               [mco-num recs-cnt]))))

(def mco-icn->count-2
  (prof/with-timer-print :enc-response.analyze--count-2
    (into {} (for [[mco-num recs] mco-icn->recs
                   :let [recs-cnt (count recs)]
                   :when (= 2 recs-cnt)]
               [mco-num recs-cnt]))))

(def mco-icn->count-3
  (prof/with-timer-print :enc-response.analyze--count-3
    (into {} (for [[mco-num recs] mco-icn->recs
                   :let [recs-cnt (count recs)]
                   :when (= 3 recs-cnt)]
               [mco-num recs-cnt]))))

(def mco-icn->count-4
  (prof/with-timer-print :enc-response.analyze--count-4
    (into {} (for [[mco-num recs] mco-icn->recs
                   :let [recs-cnt (count recs)]
                   :when (= 4 recs-cnt)]
               [mco-num recs-cnt]))))

(def mco-icn->count-1-4
  (prof/with-timer-print :enc-response.analyze--count-1-4
    (into {} (for [[mco-num recs] mco-icn->recs
                   :let [recs-cnt (count recs)]
                   :when (<= 1 recs-cnt 4)]
               [mco-num recs-cnt]))))

(def mco-icn->count-5
  (prof/with-timer-print :enc-response.analyze--count-5
    (into {} (for [[mco-num recs] mco-icn->recs
                   :let [recs-cnt (count recs)]
                   :when (= 5 recs-cnt)]
               [mco-num recs-cnt]))))

(def mco-icn->count-6+
  (prof/with-timer-print :enc-response.analyze--count-6+
    (into {} (for [[mco-num recs] mco-icn->recs
                   :let [recs-cnt (count recs)]
                   :when (<= 6 recs-cnt)]
               [mco-num recs-cnt]))))
