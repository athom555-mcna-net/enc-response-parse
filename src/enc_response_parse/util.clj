(ns enc-response-parse.util
  (:use tupelo.core)
  (:require
    [clojure.java.io :as io]
    [clojure.pprint :as pp]
    [clojure.tools.reader.edn :as edn]
    [datomic.api :as d.peer]
    [flatland.ordered.map :as omap]
    [schema.core :as s]
    [tupelo.core :as t]
    [tupelo.math :as math]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    ))

(def ^:dynamic ctx-default
  {
   :encounter-response-root-dir "/shared/tmp/iowa/iowa_response_files"
   :missing-icn-fname           "missing-icns.edn"
   :icn-maps-aug-fname          "icn-maps-aug.edn"
   :tx-data-chunked-fname       "tx-data-chuncked.edn"

   :tx-size-limit               500 ; The maxinum number of entity maps to include in a single Datomic transaction.
   })

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

;-----------------------------------------------------------------------------
(s/defn config-load->ctx :- tsk/KeyMap
  [config-fname :- s/Str]
  (let [config (edn/read-string (slurp config-fname))
        ;  >>     (spyx-pretty :config-read config)
        ctx    (it-> ctx-default
                 (glue it config)
                 (glue it {:db-uri (str (grab :datomic-uri config) \? (grab :postgres-uri config))})
                 (dissoc it :datomic-uri :postgres-uri))]
    (spyx-pretty :loaded ctx)
    ctx))

(defn dummy-fn
  [& args]
  (prn :dummy-fn--enter)
  (with-result :dummy-fn--result
    (prn :dummy-fn--leave)))

(s/defn dispatch :- s/Any
  [ctx :- tsk/KeyMap]
  (spyx-pretty :dispatch-fn ctx)
  (let [invoke-fn (grab :invoke-fn ctx)
        ; >>>       (spyx invoke-fn)
        ctx       (dissoc ctx :invoke-fn)
        form      (list invoke-fn ctx)]
    ; (spyxx invoke-fn)
    ; (prn :92 (find-var invoke-fn))
    ; (prn :93 (requiring-resolve invoke-fn))
    ; (prn :94 (qualified-symbol? invoke-fn))
    ; (prn :95 (simple-symbol? invoke-fn))
    ; (prn :var (var invoke-fn))
    ; (prn :form)
    ; (pp/pprint form)
    (eval form)))

