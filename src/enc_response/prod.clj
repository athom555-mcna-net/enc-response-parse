(ns enc-response.prod
  (:use tupelo.core)
  (:require
    [clojure.pprint :as pp]
    [datomic.api :as d.peer]
    [enc-response.datomic :as datomic]
    [enc-response.proc :as proc]
    [enc-response.schemas :as schemas]
    [schema.core :as s]
    [tupelo.profile :as prof]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    ))

(def ^:dynamic verbose?
  "Enable to see progress printouts"
  false)

(s/defn init-missing-icns->datomic
  [ctx]
  (prn :init-missing-icns->datomic--enter)
  (prof/with-timer-print :init-missing-icns->datomic
    (with-map-vals ctx [db-uri max-tx-size]
      ; create empty db
      (d.peer/delete-database db-uri)
      (d.peer/create-database db-uri)

      ; insert schema
      (datomic/peer-transact-entities db-uri schemas/prod-missing-icns)

      ; insert sample records
      (let [missing-icns (datomic/elide-db-id ; elide old value for :db/id => transaction values
                           (proc/load-missing-icns ctx))]
        (prn :init-missing-icns->datomic--num-missing (count missing-icns))
        (prof/with-timer-print :init-missing-icns->datomic--insert
          (datomic/peer-transact-entities db-uri max-tx-size missing-icns))
        (prn :init-missing-icns->datomic--leave)))))

(comment ; #todo #awt working
  (s/defn init-enc-response->datomic
    [ctx]
    (prn :init-enc-response->datomic--enter)
    (prof/with-timer-print :init-enc-response->datomic
      (with-map-vals ctx [db-uri max-tx-size]
        ; create empty db
        (d.peer/delete-database db-uri)
        (d.peer/create-database db-uri)

        ; insert schema
        (datomic/peer-transact-entities db-uri schemas/encounter-response)

        ; insert sample records
        (let [missing-icns
              (proc/load-enc-response ctx)]
          (prn :init-enc-response->datomic--num-missing (count enc-response))
          (prof/with-timer-print :init-enc-response->datomic--insert
            (datomic/peer-transact-entities db-uri max-tx-size enc-response))
          (prn :init-enc-response->datomic--leave))
        ))))


(s/defn pull-icn-recs-from-datomic :- [tsk/KeyMap]
  "Pulls all records from datomic that possess attr `:encounter-transmission/icn`. Unwraps inner
   vector to return a vector of entity-maps."
  [ctx :- tsk/KeyMap]
  (with-map-vals ctx [db-uri]
    (let [result (onlies (d.peer/q '[:find (pull ?eid [*])
                                     :where [?eid :encounter-transmission/icn]]
                           (datomic/curr-db db-uri)))]
      result)))

(s/defn save-icn-recs-datomic->missing-file :- [tsk/KeyMap]
  "Pulls all records from datomic that possess attr `:encounter-transmission/icn`. Unwraps inner
   vector to return a vector of entity-maps."
  [ctx :- tsk/KeyMap]
  (prn :save-icn-recs-datomic->missing--enter)
  (with-map-vals ctx [missing-icn-fname]
    (let [entities (pull-icn-recs-from-datomic ctx)]
      (prn :save-icn-recs-datomic->missing--writing missing-icn-fname)
      (prof/with-timer-print :save-icn-recs-datomic->missing--writing
        (spit missing-icn-fname
          (with-out-str
            (pp/pprint entities))))))
  (prn :save-icn-recs-datomic->missing--enter))

