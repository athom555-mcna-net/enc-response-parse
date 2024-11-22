(ns enc-response.datomic
  (:use tupelo.core
        tupelo.test)
  (:require
    [clojure.pprint :as pp]
    [clojure.tools.reader.edn :as edn]
    [clojure.walk :as walk]
    [datomic.api :as d.peer]
    [enc-response.schemas :as schemas]
    [schema.core :as s]
    [tupelo.core :as t]
    [tupelo.math :as math]
    [tupelo.profile :as prof]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    )
  (:gen-class))

(def ^:dynamic verbose?
  "Enable to see progress printouts"
  true)

;-----------------------------------------------------------------------------
; Define an EID-like value from Datomic (aka a :db/id value) as any positive integer with at
; least 9 digits.

(s/def eid-min-digits :- s/Int
  "The minimum length positive int to be 'EID-like' (Datomic Entity ID)"
  9)      ; 32 bit Integer is about +/- 2e9

(s/def eid-min-value :- BigInteger
  "The minimum value positive int to be 'EID-like' (Datomic Entity ID)"
  (math/pow->BigInteger 10 eid-min-digits))

(s/defn eid? :- s/Bool
  "Is an int 'EID-like' (Datomic Entity ID), i.e. large positive integer?"
  [v :- s/Int] (<= eid-min-value v))

;-----------------------------------------------------------------------------
(defn elide-db-id
  "Recursively walk a data structure and remove any map entries with the key `:db/id`"
  [data]
  (->> data
    (walk/postwalk (fn elide-db-id-walk-fn
                     [item]
                     (when-not (and (map-entry? item) ; `nil` is ignored when re-building maps
                                 (= :db/id (key item)))
                       item)))))

;-----------------------------------------------------------------------------
(s/defn curr-db :- datomic.db.Db
  [db-uri :- s/Str]
  (let [conn (d.peer/connect db-uri)
        db   (d.peer/db conn)]
    db))

(s/defn peer-delete-db :- s/Bool
  "Deletes all data and schema for Encounter Response files from Datomic. "
  [ctx :- tsk/KeyMap]
  (with-map-vals ctx [db-uri]
    (when verbose?
      (prn :datomic-peer-delete-db db-uri))
    (d.peer/delete-database db-uri)))

(s/defn ^:no-doc peer-transact-entities-impl-single :- s/Any
  "Accepts a 1D sequence of entity maps, and commits them into Datomic as a single transaction. "
  [db-uri :- s/Str
   entity-maps :- [tsk/KeyMap]]
  (when verbose?
    (prn :peer-transact-entities-impl-single--enter))
  (prof/with-timer-print :peer-transact-entities-impl-single
    (let [conn (d.peer/connect db-uri)
          resp @(d.peer/transact conn entity-maps)]
      (with-result resp
        (when verbose?
          (prn :peer-transact-entities-impl-single--leave))))))

(s/defn ^:no-doc peer-transact-entities-impl-chunked :- tsk/Vec
  "Accepts a 2D array of entity maps. Each row of data, in order, is committed into Datomic
  as a separate transaction.  Returns a vector of transaction results."
  [db-uri :- s/Str
   max-tx-size :- s/Int
   entity-maps :- [tsk/KeyMap]]
  (when verbose?
    (prn :peer-transact-entities-impl-chunked--enter))
  (prof/with-timer-print :peer-transact-entities-impl-chunked
    (let [conn             (d.peer/connect db-uri)
          entities-chunked (partition-all max-tx-size entity-maps)
          tx-results       (reduce
                             (fn [cum tx]
                               (when verbose?
                                 (prn :peer-transact-entities-impl-chunked--tx (count cum)))
                               (conj cum
                                 @(d.peer/transact conn tx)))
                             []
                             entities-chunked)]
      (with-result tx-results
        (when verbose?
          (prn :peer-transact-entities-impl-chunked--leave))))))

(s/defn peer-transact-entities :- s/Any
  "Accepts a sequence of entity maps, and commits them into Datomic as a transaction. Usage:

       (peer-transact-entities db-uri entity-maps)
         Commits the entity-maps in a single transaction.

       (peer-transact-entities db-uri max-tx-size entity-maps)
         Partitions the entity-maps into multiple transactions using max-tx-size, committing them in sequence.
  "
  ([db-uri :- s/Str
    data :- [tsk/KeyMap]]
   (peer-transact-entities-impl-single db-uri data))
  ([db-uri :- s/Str
    max-tx-size :- s/Int
    data :- [tsk/KeyMap]]
   (peer-transact-entities-impl-chunked db-uri max-tx-size data)))

(s/defn peer-transact-entities-with :- datomic.db.Db
  "Accepts a 2D array of entity maps.  First takes a snapshot of Datomic.
  Then, each row of data is committed onto the snapshot using `(d.peer/with ...)`,
  as a separate transaction.  Returns the snapshot as modified by the transactions,
  but without modifying the actual Datomic state."
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
  (with-map-vals ctx [tx-data-fname db-uri max-tx-size]
    (let [entity-maps (edn/read-string (slurp tx-data-fname))]
      (peer-transact-entities db-uri max-tx-size entity-maps))))

(s/defn load-commit-transactions-with :- datomic.db.Db
  [ctx]
  (prn :-----------------------------------------------------------------------------)
  (prn :load-commit-transactions-with--enter)
  (with-map-vals ctx [tx-data-fname]
    (let [txs                 (edn/read-string (slurp tx-data-fname))
          conn                (d.peer/connect (grab :db-uri ctx))
          db-before           (d.peer/db conn)
          missing-icns-before (query-missing-icns-iowa-narrow db-before)
          db-after            (peer-transact-entities-with conn txs)
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

;-----------------------------------------------------------------------------
(s/defn enc-response-schema->datomic :- s/Any
  "Transact the schema for encounter response records into Datomic"
  [ctx :- tsk/KeyMap]
  (with-map-vals ctx [db-uri]
    (prn :enc-response-schema->datomic db-uri)
    (peer-transact-entities db-uri schemas/encounter-response)))

(s/defn enc-response-datomic-init :- s/Any
  "Transact the schema for encounter response records into Datomic"
  [ctx :- tsk/KeyMap]
  (with-map-vals ctx [db-uri]
    (prn :enc-response-datomic-init db-uri)
    (peer-delete-db ctx)
    (d.peer/create-database db-uri)
    (enc-response-schema->datomic ctx)))

(s/defn icn->enc-response-recs :- [tsk/KeyMap]
  [db :- s/Any
   icn :- s/Str]
  ; query might return more than 1 result => vec of maps
  (let [recs (onlies (d.peer/q '[:find (pull ?e [*])
                                 :in $ ?icn
                                 :where [?e :mco-claim-number ?icn]]
                       db icn))]
    recs))

(s/defn enc-response-query-icn->plan-icn :- [s/Str]
  [db :- s/Any
   icn :- s/Str]
  ; query might return more than 1 result => vec of maps
  (let [recs      (icn->enc-response-recs db icn)
        plan-icns (vec (distinct
                         (for [rec recs]
                           (grab :iowa-transaction-control-number rec))))]
    plan-icns))
