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
    (vec (sort-by :mco-claim-number all-recs))))

(def grp-by-mco-number (group-by :mco-claim-number all-recs-sorted))
(def mco-number->count (map-vals grp-by-mco-number count))
(def mco-number->count-1 (into {} (for [[mco-num recs] grp-by-mco-number
                                        :let [recs-cnt (count recs)]
                                        :when (= 1 recs-cnt)]
                                    [mco-num recs-cnt])))
(def mco-number->count-2 (into {} (for [[mco-num recs] grp-by-mco-number
                                        :let [recs-cnt (count recs)]
                                        :when (= 2 recs-cnt)]
                                    [mco-num recs-cnt])))
(def mco-number->count-3 (into {} (for [[mco-num recs] grp-by-mco-number
                                        :let [recs-cnt (count recs)]
                                        :when (= 3 recs-cnt)]
                                    [mco-num recs-cnt])))
(def mco-number->count-4 (into {} (for [[mco-num recs] grp-by-mco-number
                                        :let [recs-cnt (count recs)]
                                        :when (= 4 recs-cnt)]
                                    [mco-num recs-cnt])))
(def mco-number->count-5+ (into {} (for [[mco-num recs] grp-by-mco-number
                                         :let [recs-cnt (count recs)]
                                         :when (<= 5 recs-cnt)]
                                     [mco-num recs-cnt])))
