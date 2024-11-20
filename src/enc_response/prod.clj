(ns enc-response.prod
  (:use tupelo.core)
  (:require
    [datomic.api :as d.peer]
    [enc-response.datomic :as datomic]
    [enc-response.proc :as proc]
    [enc-response.schemas :as schemas]
    [schema.core :as s]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    )
  (:gen-class))

(def ^:dynamic verbose?
  "Enable to see progress printouts"
  false)

(s/defn init-missing-icns->datomic
  [ctx]
  (with-map-vals ctx [db-uri tx-size-limit]
    ; create empty db
    (datomic/peer-delete-db ctx)
    (d.peer/create-database db-uri)

    ; create schema
    (datomic/peer-transact-entities db-uri schemas/prod-missing-icns)

    ; add sample records
    (let [conn                 (d.peer/connect db-uri)

          missing-icns         (datomic/elide-db-id
                                 (proc/load-missing-icns ctx))
          missing-icns-chunked (partition-all tx-size-limit missing-icns)
          resp1                (datomic/peer-transact-entities-chunked conn missing-icns-chunked)])))

