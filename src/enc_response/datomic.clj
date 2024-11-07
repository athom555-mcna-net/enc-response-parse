(ns enc-response.datomic
  (:use tupelo.core
        tupelo.test)
  (:require
    [clojure.pprint :as pp]
    [clojure.tools.reader.edn :as edn]
    [datomic.api :as d.peer]
    [schema.core :as s]
    [tupelo.core :as t]
    [tupelo.math :as math]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
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

;---------------------------------------------------------------------------------------------------
(def enc-response-schema
  "Encounter Response file fields (see NS enc-response.parse)"
  [{:db/ident     :mco-claim-number
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
(s/defn enc-response-schema->datomic :- s/Any
  "Transact the schema for encounter response records into Datomic"
  [ctx :- tsk/KeyMap]
  (with-map-vals ctx [db-uri]
    (let [conn (d.peer/connect db-uri)
          resp @(d.peer/transact conn enc-response-schema)]
      resp)))

(s/defn enc-response-recs->datomic :- s/Any
  "Transact encounter response records into Datomic, using a block size from `ctx`
  as specified by :tx-size-limit. "
  [ctx :- tsk/KeyMap
   enc-resp-recs :- [tsk/KeyMap]]
  (with-map-vals ctx [db-uri tx-size-limit]
    (let [enc-resp-rec-chunked (partition-all tx-size-limit enc-resp-recs)
          conn                 (d.peer/connect db-uri)
          resp                 (transact-seq-peer conn enc-resp-rec-chunked)]
      ; (pp/pprint resp )
      resp)))

;-----------------------------------------------------------------------------
(s/defn query-missing-icns :- [[s/Any]]
  [db :- datomic.db.Db]
  (let [missing-icns (vec (d.peer/q '[:find ?eid ?icn ?previous-icn
                                      :where
                                      [(missing? $ ?eid :encounter-transmission/plan-icn)]
                                      [?eid :encounter-transmission/icn ?icn]
                                      [?eid :encounter-transmission/previous-icn ?previous-icn]]
                            db))]
    missing-icns))

(s/defn query-missing-icns-iowa-narrow :- [[s/Any]]
  [db :- datomic.db.Db]
  (let [missing-icns (mapv only
                       (d.peer/q '[:find (pull ?eid [:db/id
                                                     :encounter-transmission/icn
                                                     :encounter-transmission/previous-icn
                                                     :encounter-transmission/plan
                                                     {:encounter-transmission/status [*]}])
                                   :where
                                   [(missing? $ ?eid :encounter-transmission/plan-icn)]
                                   [?eid :encounter-transmission/icn ?icn]
                                   (or
                                     [?eid :encounter-transmission/status :encounter-transmission.status/accepted]
                                     [?eid :encounter-transmission/status :encounter-transmission.status/rejected])
                                   [?eid :encounter-transmission/plan ?plan]
                                   [(enc-response-parse.util/iowa-prefix? ?plan)]
                                   ]
                         db))]
    missing-icns))

;-----------------------------------------------------------------------------
(s/defn find-missing-icns :- [[s/Any]]
  [ctx]
  (with-map-vals ctx [db-uri]
    (let [conn         (d.peer/connect db-uri)
          db           (d.peer/db conn)
          missing-icns (query-missing-icns db)]
      (println "Number Missing ICNs:  " (count missing-icns))
      missing-icns)))

(s/defn find-missing-icns-iowa-narrow :- [[s/Any]]
  [ctx]
  (with-map-vals ctx [db-uri]
    (let [conn         (d.peer/connect db-uri)
          db           (d.peer/db conn)
          missing-icns (query-missing-icns-iowa-narrow db)]
      (println "Number Missing ICNs Iowa Narrow:  " (count missing-icns))
      missing-icns)))

(defn save-missing-icns
  [ctx]
  (prn :save-missing-icns--enter)
  (let [missing-icns (find-missing-icns ctx)]
    (with-map-vals ctx [missing-icn-fname]
      (spit missing-icn-fname
        (with-out-str
          (pp/pprint
            (vec missing-icns))))))
  (prn :save-missing-icns--leave))

(defn save-missing-icns-iowa-narrow
  [ctx]
  (prn :save-missing-icns-iowa-narrow--enter)
  (let [missing-icn-recs (find-missing-icns-iowa-narrow ctx)]
    (with-map-vals ctx [missing-icn-fname]
      (spit missing-icn-fname
        (with-out-str
          (pp/pprint
            (vec missing-icn-recs))))))
  (prn :save-missing-icns-iowa-narrow--leave))

(s/defn load-commit-transactions :- s/Any
  [ctx]
  (with-map-vals ctx [tx-data-chunked-fname]
    (let [conn (d.peer/connect (grab :db-uri ctx))
          txs  (edn/read-string (slurp tx-data-chunked-fname))]
      (transact-seq-peer conn txs))))

(s/defn load-commit-transactions-with :- datomic.db.Db
  [ctx]
  (prn :-----------------------------------------------------------------------------)
  (prn :load-commit-transactions-with--enter)
  (with-map-vals ctx [tx-data-chunked-fname]
    (let [txs                 (edn/read-string (slurp tx-data-chunked-fname))
          conn                (d.peer/connect (grab :db-uri ctx))
          db-before           (d.peer/db conn)
          missing-icns-before (query-missing-icns-iowa-narrow db-before)
          db-after            (transact-seq-peer-with conn txs)
          missing-icns-after  (query-missing-icns-iowa-narrow db-after)]
      (println "Missing ICNs before = " (count missing-icns-before))
      (println "Missing ICNs after  = " (count missing-icns-after))))
  (prn :load-commit-transactions-with--leave)
  (prn :-----------------------------------------------------------------------------))

(s/defn count-enc-response-recs :- s/Int
  [ctx]
  (with-map-vals ctx [db-uri]
    (let [conn     (d.peer/connect db-uri)
          db       (d.peer/db conn)
          num-recs (d.peer/q '[:find (count ?e)
                                       :where [?e :mco-claim-number]]
                             db)]
      (if (empty? num-recs)
        0
        (only2 num-recs)))))