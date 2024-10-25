(ns enc-response-parse.core
  (:use tupelo.core)
  (:require
    [clojure.java.io :as io]
    [clojure.pprint :as pp]
    [clojure.tools.reader.edn :as edn]
    [datomic.api :as d.peer]
    [enc-response-parse.util :as util]
    [flatland.ordered.map :as omap]
    [schema.core :as s]
    [tupelo.misc :as misc]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    )
  (:import
    [java.io File])
  (:gen-class))

(def ^:dynamic verbose?
  "Enable to see progress printouts"
  false)

(def ^:dynamic ctx-default
  {:encounter-response-root-dir "/shared/tmp/iowa/iowa_response_files"
   :missing-icn-fname           "missing-icns.edn"
   :icn-maps-aug-fname          "icn-maps-aug.edn"
   :tx-data-chunked-fname       "tx-data-chuncked.edn"

   :tx-size-limit               500 ; The maxinum number of entity maps to include in a single Datomic transaction.
   })

(s/def encounter-response-filename-patt
  "Regex pattern for encounter response files (no parent dirs!)"
  #"^ENC_RESPONSE_D_.*.TXT$") ; eg `ENC_RESPONSE_D_20200312_062014.TXT`

(s/def spec-opts-default :- tsk/KeyMap
  "Default options for field specs"
  {:trim?          true ; trim leading/trailing blanks from returned string

   ; Don't crash if insufficient chars found in line.  Should only be used for last N fields
   :length-strict? true})

(s/def iowa-encounter-response-specs :- [tsk/KeyMap]
  "Field specs (in order) for the Iowa Encounter Response files. Named like `ENC_RESPONSE_D_20210722_071949.TXT`"
  [{:name :mco-claim-number :format :numeric :length 30} ; #todo it is a number like "30000062649905                "
   {:name :iowa-transaction-control-number :format :numeric :length 17}
   {:name :iowa-processing-date :format :numeric :length 8}
   {:name :claim-type :format :char :length 1}
   {:name :claim-frequency-code :format :numeric :length 1}
   {:name :member-id :format :alphanumeric :length 8}
   {:name :first-date-of-service :format :numeric :length 8}
   {:name :billing-provider-npi :format :numeric :length 10}
   {:name :mco-paid-date :format :numeric :length 8}
   {:name :total-paid-amount :format :numeric :length 12}
   {:name :line-number :format :numeric :length 2}
   {:name :error-code :format :alphanumeric :length 3} ; #todo always "A00"
   {:name :field :format :alphanumeric :length 24 :length-strict? false}
   {:name :error-field-value :format :alphanumeric :length 80 :length-strict? false} ; #todo seems to be missing (all blanks!)
   ])

;-----------------------------------------------------------------------------
(s/defn config-load->ctx :- tsk/KeyMap
  [config-fname :- s/Str]
  (let [config (edn/read-string (slurp config-fname))
        >>     (spyx-pretty :config-read config)
        ctx    (it-> ctx-default
                 (glue it config)
                 (glue it {:db-uri (str (grab :datomic-uri config) \? (grab :postgres-uri config))})
                 (dissoc it :datomic-uri :postgres-uri))]
    (spyx-pretty :loaded ctx)
    ctx))

;-----------------------------------------------------------------------------
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

;---------------------------------------------------------------------------------------------------
(s/def format->pattern :- tsk/KeyMap
  "Map from format kw to regex pattern."
  {:char         #"\p{Alpha}*" ; *** empty string allowed ***
   :numeric      #"\p{Digit}*" ; *** empty string allowed ***
   :alphanumeric #"\p{Alnum}*" ; *** empty string allowed ***
   })

(s/defn validate-format :- s/Str
  "Perform a regex check to validates a string format. Returns the string upon success, else
  throws."
  [format :- s/Keyword
   src :- s/Str]
  (let [regex-patt (grab format format->pattern)]
    (when (nil? (re-matches regex-patt src))
      (throw (ex-info "string does not match format" (vals->map src format)))))
  src)    ; return input if passes

(s/defn spec-slice
  [spec-in :- tsk/KeyMap
   char-seq-in :- [Character]]
  (let [spec (into spec-opts-default spec-in)]
    (with-map-vals spec [name format length length-strict? trim?]
      (assert-info (pos? length) "field length must be positive" (vals->map spec))
      ; NOTE: `split-at` works for both a string and a char seq
      (let [[field-chars rem-chars-seq] (split-at length char-seq-in)]
        (when length-strict?
          (assert-info (= length (count field-chars)) "field chars missing" (vals->map spec-in field-chars rem-chars-seq)))
        (let [field-str (cond-it-> (str/join field-chars)
                          trim? (str/trim it))
              result    {:state  {:chars-remaining rem-chars-seq}
                         :output {name field-str}}]
          (validate-format format field-str)
          result)))))

(s/defn parse-string-fields :- tsk/KeyMap
  "Parse the input string the supplied fields specification"
  [field-specs :- [tsk/KeyMap]
   input-str :- s/Str]
  (loop [specs  field-specs
         chars  (vec input-str)
         result (omap/ordered-map)]
    (if (empty? specs)
      result ; return
      (let [last-spec?  (= 1 (count specs))
            spec-curr   (xfirst specs)
            ; >> (spyx spec-curr)
            slice-out   (spec-slice spec-curr chars)
            ; >> (spyx slice-out)
            specs-next  (xrest specs)
            chars-next  (fetch-in slice-out [:state :chars-remaining])
            result-next (glue result (grab :output slice-out))]
        (recur specs-next chars-next result-next)))))

;---------------------------------------------------------------------------------------------------
(s/defn enc-resp-file-name? :- s/Bool
  [fname :- s/Str]
  (boolean (re-matches encounter-response-filename-patt fname)))

(s/defn enc-resp-file? :- s/Bool
  [file :- File]
  (enc-resp-file-name?
    (.getName file) ; returns string w/o parent dirs
    ))

(s/defn parse-grep-result :- tsk/KeyMap
  [out :- s/Str]
  (let [out         (str/trim out)
        matches-out (re-matches #"(^.*ENC_RESPONSE_.*.TXT):(.*)$" out)
        result      {:fname   (xsecond matches-out)
                     :content (xthird matches-out)}]
    result))

(s/defn extract-enc-resp-fields ; :- s/Str
  [shell-result :- tsk/KeyMap]
  (with-map-vals shell-result [exit out err]
    (assert (= 0 exit))
    (assert (= "" err))
    (let [grep-result         (parse-grep-result out)
          >>                  (when verbose?
                                (println "                      found file: " (grab :fname grep-result)))
          enc-response-line   (grab :content grep-result)
          enc-response-parsed (parse-string-fields iowa-encounter-response-specs enc-response-line)]
      enc-response-parsed)))

(s/defn orig-icn->response-parsed :- tsk/KeyMap
  [ctx :- tsk/KeyMap
   icn-str :- s/Str]
  (with-map-vals ctx [encounter-response-root-dir]
    (let [icn-str             (str/trim icn-str)
          shell-cmd-str       (format "grep '^%s' %s/ENC_*.TXT" icn-str encounter-response-root-dir)
          shell-result        (misc/shell-cmd shell-cmd-str)
          enc-response-parsed (extract-enc-resp-fields shell-result)]
      enc-response-parsed)))

(s/defn query-missing-icns :- [[s/Any]]
  [db :- datomic.db.Db]
  (let [missing-icns (vec (d.peer/q '[:find ?eid ?icn ?previous-icn
                                      :where
                                      [(missing? $ ?eid :encounter-transmission/plan-icn)]
                                      [?eid :encounter-transmission/icn ?icn]
                                      [?eid :encounter-transmission/previous-icn ?previous-icn]]
                            db))]
    missing-icns))

(s/defn find-missing-icns :- [[s/Any]]
  [ctx]
  (with-map-vals ctx [db-uri]
    (let [conn         (d.peer/connect db-uri)
          db           (d.peer/db conn)
          missing-icns (query-missing-icns db)]
      (println "Number Missing ICNs:  " (count missing-icns))
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

(s/defn load-missing-icns :- [tsk/KeyMap]
  [ctx :- tsk/KeyMap]
  (with-map-vals ctx [missing-icn-fname]
    (println "Reading: " missing-icn-fname)
    (let [; 2D file. Each record is [<eid> <icn> <previous-icn>]
          missing-data (edn/read-string (slurp missing-icn-fname))
          icn-strs     (forv [rec missing-data]
                         (zipmap [:eid :icn :previous-icn] rec))]
      icn-strs)))

(s/defn create-icn-maps-aug :- [tsk/KeyMap]
  [ctx :- tsk/KeyMap]
  (with-map-vals ctx [icn-maps-aug-fname]
    (let [missing-icn-maps (load-missing-icns ctx)
          icn-maps-aug     (forv [icn-map missing-icn-maps]
                             (when verbose?
                               (println "seaching ENC_RESPONSE_*.TXT for icn:" icn-map))
                             (with-map-vals icn-map [icn]
                               (let [enc-resp    (->sorted-map (orig-icn->response-parsed ctx icn))
                                     iowa-tcn    (grab :iowa-transaction-control-number enc-resp)
                                     icn-map-aug (glue icn-map {:plan-icn iowa-tcn})]
                                 icn-map-aug)))]
      (println "Writing: " icn-maps-aug-fname)
      (spit icn-maps-aug-fname (with-out-str (pp/pprint icn-maps-aug)))
      icn-maps-aug)))

(s/defn create-tx-data-chunked :- [[tsk/KeyMap]]
  [ctx]
  (prn :create-tx-data-chunked--enter)
  (with-map-vals ctx [icn-maps-aug-fname tx-data-chunked-fname tx-size-limit]
    (let [icn-maps-aug    (edn/read-string (slurp icn-maps-aug-fname))
          tx-data         (forv [icn-map-aug icn-maps-aug]
                            (with-map-vals icn-map-aug [eid plan-icn]
                              {:db/id                           eid
                               :encounter-transmission/plan-icn plan-icn}))
          tx-data-chunked (unlazy (partition-all tx-size-limit tx-data))]
      (println "Writing: " tx-data-chunked-fname)
      (spit tx-data-chunked-fname (with-out-str (pp/pprint tx-data-chunked)))
      (with-result tx-data-chunked
        (prn :create-tx-data-chunked--leave)))))

(s/defn load-commit-transactions :- s/Any
  [ctx]
  (with-map-vals ctx [tx-data-chunked-fname]
    (let [conn (d.peer/connect (grab :db-uri ctx))
          txs  (edn/read-string (slurp tx-data-chunked-fname))]
      (util/transact-seq-peer conn txs))))

(s/defn load-commit-transactions-with :- datomic.db.Db
  [ctx]
  (prn :-----------------------------------------------------------------------------)
  (prn :load-commit-transactions-with--enter)
  (with-map-vals ctx [tx-data-chunked-fname]
    (let [txs                 (edn/read-string (slurp tx-data-chunked-fname))
          conn                (d.peer/connect (grab :db-uri ctx))
          db-before           (d.peer/db conn)
          missing-icns-before (query-missing-icns db-before)
          db-after            (util/transact-seq-peer-with conn txs)
          missing-icns-after  (query-missing-icns db-after)]
      (println "Missing ICNs before = " (count missing-icns-before))
      (println "Missing ICNs after  = " (count missing-icns-after))))
  (prn :load-commit-transactions-with--leave)
  (prn :-----------------------------------------------------------------------------))

;---------------------------------------------------------------------------------------------------
(s/defn icn-status-sample :- [[s/Any]]
  [ctx]
  (prn :icn-status-sample--enter)
  (with-map-vals ctx [db-uri]
    (let [conn   (d.peer/connect db-uri)
          db     (d.peer/db conn)
          result (vec (d.peer/q '[:find (pull ?eid [:encounter-transmission/icn
                                                    {:encounter-transmission/status [*]}]
                                          )
                                  :in $ [?status ...]
                                  :where [?eid :encounter-transmission/status ?status]]

                        db
                        [:encounter-transmission.status/accepted
                         :encounter-transmission.status/rejected
                         :encounter-transmission.status/rejected-by-validation]))
          ]
      (prn :icn-status-sample--result)
      (pp/pprint result))))
(comment
  [[:encounter-transmission/icn "30000000165819"]
   [:encounter-transmission/previous-icn "30000000165694"]
   [:encounter-transmission/status
    {:db/id    17592186045428,
     :db/ident :encounter-transmission.status/accepted}]])

(comment
  ; find all the Stewart or Stuart first names
  (is (submatch? [{:name/first "Stewart", :name/last "Brand"}
                  {:name/first "Stuart", :name/last "Halloway"}
                  {:name/first "Stuart", :name/last "Smalley"}]
        (sort-by :name/last (onlies (d/q '[:find (pull ?e [*])
                                           :in $ [?name ...]
                                           :where [?e :name/first ?name]]
                                      db
                                      ["Stewart" "Stuart"])))))

  )
;---------------------------------------------------------------------------------------------------
(defn -main
  [config-fname]
  (spy :main--enter)
  (spyx :-main config-fname)
  (assert (not-nil? config-fname))

  (with-result
    (let [ctx    (config-load->ctx config-fname)
          >>     (spyx-pretty ctx)
          result (dispatch ctx)]
      (spy :main--dispatch-post)
      (spyx (type result))
      ; (spyx-pretty :main-result result)
      (spy :main--leave)
      result)))
