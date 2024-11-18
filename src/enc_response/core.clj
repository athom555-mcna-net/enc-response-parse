(ns enc-response.core
  (:use tupelo.core)
  (:require
    [clojure.tools.reader.edn :as edn]
    [clojure.pprint :as pp]
    [enc-response.datomic :as datomic] ; required to make `eval` work
    [enc-response.parse :as parse] ; required to make `eval` work
    [enc-response.proc :as proc] ; required to make `eval` work
    [schema.core :as s]
    [tupelo.schema :as tsk]
    )
  (:gen-class))

(def ^:dynamic ctx-default
  {
   :encounter-response-root-dir "/shared/tmp/iowa/iowa_response_files"
   :missing-icn-fname           "missing-icns.edn"
   :icn-maps-aug-fname          "icn-maps-aug.edn"
   :tx-data-chunked-fname       "tx-data-chuncked.edn"

   :tx-size-limit               500 ; The maxinum number of entity maps to include in a single Datomic transaction.
   })

;-----------------------------------------------------------------------------
(defn dummy-fn
  [& args]
  ; (prn :dummy-fn--enter)
  (with-result :dummy-fn--result
    ; (prn :dummy-fn--leave)
    (+ 2 3)))

;-----------------------------------------------------------------------------
(s/defn config-load->ctx :- tsk/KeyMap
  [config-fname :- s/Str]
  (let [config (edn/read-string (slurp config-fname))
        ;  >>     (spyx-pretty :config-read config)
        c1     (glue ctx-default config)
        c2     (if (:db-uri c1)
                 c1 ; do not overwrite pre-existing value
                 (glue c1 {:db-uri (str (grab :datomic-uri config) \? (grab :postgres-uri config))}))
        ctx    (dissoc c2 :datomic-uri :postgres-uri)] ; always remove component URI values
    (spyx-pretty :loaded ctx)
    ctx))

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
  ; (prn :form)
  ; (pp/pprint form)
    (eval form)))

;---------------------------------------------------------------------------------------------------
(s/defn main-impl
  [config-fname :- s/Str]
  (spy :main-impl--enter)
  (spyx :main-impl config-fname)
  (assert (not-nil? config-fname))

  (let [ctx    (config-load->ctx config-fname)
        >>     (spyx-pretty ctx)
        result (dispatch ctx)]
    (spy :main-impl--dispatch-post)
    (spyx (type result))
    ; (spyx-pretty :main-result result)
    (with-result result
      (spy :main-impl--leave))))

(defn -main
  [config-fname]
  (spy :main--enter)
  (main-impl config-fname)
  (spy :main--leave)

  (spy :calling-system-exit)
  (System/exit 0))
