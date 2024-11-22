(ns enc-response.prod
  (:use tupelo.core)
  (:require
    [datomic.api :as d.peer]
    [enc-response.datomic :as datomic]
    [enc-response.proc :as proc]
    [enc-response.schemas :as schemas]
    [schema.core :as s]
    [tupelo.profile :as prof]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    )
  (:gen-class))

(def ^:dynamic verbose?
  "Enable to see progress printouts"
  false)

(s/defn init-missing-icns->datomic
  [ctx]
  (prn :init-missing-icns->datomic--enter)
  (prof/with-timer-print :init-missing-icns->datomic
    (with-map-vals ctx [db-uri tx-size-limit]
      ; create empty db
      (d.peer/delete-database db-uri)
      (d.peer/create-database db-uri)

      ; insert schema
      (datomic/peer-transact-entities db-uri schemas/prod-missing-icns)

      ; insert sample records
      (let [missing-icns (datomic/elide-db-id ; elide old value for :db/id => transaction values
                           (proc/load-missing-icns ctx))
            resp1        (datomic/peer-transact-entities db-uri tx-size-limit missing-icns)])))
  (prn :init-missing-icns->datomic--leave))

