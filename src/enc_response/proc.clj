(ns enc-response.proc
  (:use tupelo.core)
  (:require
    [clojure.java.io :as io]
    [clojure.pprint :as pp]
    [clojure.tools.reader.edn :as edn]
    [datomic.api :as d.peer]
    [enc-response.datomic :as datomic]
    [enc-response.parse :as parse]
    [enc-response.schemas :as schemas]
    [enc-response.util :as util]
    [schema.core :as s]
    [tupelo.csv :as csv]
    [tupelo.profile :as prof]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    ))

(def ^:dynamic verbose?
  "Enable to see progress printouts"
  false)

(s/defn load-missing-icns :- [tsk/KeyMap]
  [ctx :- tsk/KeyMap]
  (with-map-vals ctx [missing-icn-fname]
    (prn :load-missing-icns missing-icn-fname)
    (prof/with-timer-print :load-missing-icns
      (edn/read-string (slurp missing-icn-fname)))))

(s/defn resp-recs->newest :- tsk/KeyMap
  "Accepts a vec of Encounter Response records from Datomic, returning the one with the
  latest value for :iowa-processing-date"
  [resp-recs :- [tsk/KeyMap]]
  (let [rec->iso-date-str (s/fn [resp-rec :- tsk/KeyMap]
                            (let [datestr-mmddyyyy (grab :iowa-processing-date resp-rec)
                                  result           (util/date-str-mmddyyyy->iso datestr-mmddyyyy)]
                              result))
        recs-sorted       (sort-by rec->iso-date-str resp-recs)
        result            (xlast recs-sorted)]
    result))

(s/defn enc-resp-disp-diff :- [tsk/KeyMap]
  [ctx :- tsk/KeyMap]
  (spyx (datomic/count-enc-response-recs ctx))
  (with-map-vals ctx [db-uri]
    (let [missing-recs      (load-missing-icns ctx)
          >>                (prn :num-missing-icns (count missing-recs))
          db                (datomic/curr-db db-uri)
          ; for each rec with missing ICN, find only the newest encounter response record
          enc-response-recs (forv [missing-rec missing-recs]
                              (let [icn                 (grab :encounter-transmission/icn missing-rec)
                                    enc-resp-recs       (datomic/icn->enc-response-recs db icn)
                                    enc-resp-rec-newest (resp-recs->newest enc-resp-recs)]
                                enc-resp-rec-newest))]
      enc-response-recs)))

(s/defn create-icn-maps-aug->file :- [tsk/KeyMap]
  [ctx :- tsk/KeyMap]
  (spyx :create-icn-maps-aug-datomic--enter)
  (spyx :create-icn-maps-aug-datomic (datomic/count-enc-response-recs ctx))
  (prof/with-timer-print :create-icn-maps-aug-datomic--processing
    (with-map-vals ctx [db-uri icn-maps-aug-fname]
      (let [missing-recs (load-missing-icns ctx)
            >>           (prn :num-missing-icns (count missing-recs))
            db           (datomic/curr-db db-uri)
            ; for each rec with missing ICN, find only the newest encounter response record
            icn-maps-aug (forv [missing-rec missing-recs]
                           (let [icn                 (grab :encounter-transmission/icn missing-rec)
                                 enc-resp-recs       (datomic/icn->enc-response-recs db icn)
                                 enc-resp-rec-newest (resp-recs->newest enc-resp-recs)

                                 iowa-tcn            (grab :iowa-transaction-control-number enc-resp-rec-newest)
                                 icn-map-aug         (glue missing-rec {:encounter-transmission/plan-icn iowa-tcn})]
                             icn-map-aug))]

        (println "Writing: " icn-maps-aug-fname)
        (prof/with-timer-print :create-icn-maps-aug-datomic--writing-file
          (spit icn-maps-aug-fname (with-out-str (pp/pprint icn-maps-aug))))
        (with-result icn-maps-aug
          (spyx :create-icn-maps-aug-datomic--enter))))))

(s/defn icn-maps-aug->tx-data :- [tsk/KeyMap]
  [ctx]
  (prn :icn-maps-aug->tx-data--enter)
  (with-map-vals ctx [icn-maps-aug-fname tx-data-fname]
    (let [icn-maps-aug (edn/read-string (slurp icn-maps-aug-fname))
          tx-data      (keep-if not-nil? ; skip if plan-icn not found #todo unnecessary?
                         (forv [icn-map-aug icn-maps-aug]
                           (let [eid      (grab :db/id icn-map-aug)
                                 icn      (grab :encounter-transmission/icn icn-map-aug)
                                 plan-icn (grab :encounter-transmission/plan-icn icn-map-aug)]
                             (if (truthy? plan-icn) ; skip if plan-icn not found #todo unnecessary?
                               {:db/id                           eid
                                :encounter-transmission/plan-icn plan-icn}
                               (prn :skipping-nil--plan-icn icn)))))]
      (println "Writing: " tx-data-fname)
      (prof/with-timer-print :icn-maps-aug->tx-data--writing-file
        (spit tx-data-fname (with-out-str (pp/pprint tx-data))))
      (with-result tx-data
        (prn :icn-maps-aug->tx-data--leave)))))

(comment
  (defn enc-resp-mult-count
    [ctx]
    (spyx (datomic/count-enc-response-recs ctx))
    (with-map-vals ctx [db-uri]
      (let [missing-recs       (load-missing-icns ctx)
            >>                 (prn :num-missing-icns (count missing-recs))
            db                 (curr-db db-uri)
            icns-duplicate     (keep-if not-nil?
                                 (forv [[idx missing-rec] (indexed missing-recs)]
                                   (let [icn               (grab :encounter-transmission/icn missing-rec)
                                         enc-resp-recs     (datomic/icn->enc-response-recs db icn)
                                         num-enc-resp-recs (count enc-resp-recs)]
                                     (println idx icn num-enc-resp-recs)
                                     (when (< 1 num-enc-resp-recs)
                                       icn))))
            num-icns-duplicate (count icns-duplicate)]
        (spyx num-icns-duplicate)))))

(s/defn tx-data-file->datomic :- s/Any
  [ctx]
  (prn :tx-data-file->datomic--enter)
  (with-map-vals ctx [tx-data-fname db-uri max-tx-size]
    (let [entity-maps (prof/with-timer-print :tx-data-file->datomic--read-file
                        (edn/read-string (slurp tx-data-fname)))]
      (with-result (datomic/peer-transact-entities db-uri max-tx-size entity-maps)
        (prn :tx-data-file->datomic--leave)))))

(s/defn error-code->status-str :- s/Str
  "Convert the 'error-code' field of an Encounter Response record into a text string
  'accepted' or 'rejected'."
  [error-code :- s/Str]
  (if (str/lowercase= "a00" error-code)
    "accepted"
    "rejected"))

(s/defn enc-resp-parsed->tsv
  "Appends a block of TSV data to output file as specified in ctx."
  [ctx :- tsk/KeyMap
   data-recs :- [tsk/KeyMap]
   init-output-file? :- s/Bool]
  (prof/with-timer-accum :enc-resp-parsed->tsv
    (let [header-flg (truthy? init-output-file?)
          append-flg (not header-flg)]
      (with-map-vals ctx [plan-icn-update-tsv-fname]
        (let [out-recs (forv [data-rec data-recs]
                         (with-map-vals data-rec [mco-claim-number error-code iowa-transaction-control-number]
                           {:icn      mco-claim-number
                            :status   (error-code->status-str error-code)
                            :plan-icn iowa-transaction-control-number}))
              csv-str  (csv/entities->csv out-recs {:separator \tab :header? header-flg})]
          (spit plan-icn-update-tsv-fname csv-str :append append-flg))))))

(s/defn init-enc-response-files->updates-tsv
  "Uses `:encounter-response-root-dir` from map `ctx` to specify a directory of
  Encounter Response files. For each file in turn, loads/parses the file and saves
  data fields => TSV file."
  [ctx :- tsk/KeyMap]
  (nl)
  (prn :init-enc-response-files->updates-tsv--enter)
  (prof/with-timer-accum :init-enc-response-files->updates-tsv
    (let [enc-resp-fnames (parse/iowa-enc-response-dir->fnames ctx)
          first-time?     (atom true)]
      (prn :init-enc-response-files->updates-tsv--num-files (count enc-resp-fnames))
      (nl)
      (doseq [fname enc-resp-fnames]
        (prn :init-enc-response-files->updates-tsv--processing fname)
        (let [data-recs (parse/iowa-enc-response-fname->parsed fname)]
          (enc-resp-parsed->tsv ctx data-recs @first-time?)
          (reset! first-time? false)))
      (nl)))
  (prof/print-profile-stats!)
  (prn :init-enc-response-files->updates-tsv--leave)
  (nl))

(s/defn enc-response-recs->datomic :- s/Any
  "Transact encounter response records into Datomic, using a block size from `ctx`
  as specified by :max-tx-size. "
  [ctx :- tsk/KeyMap
   entity-maps :- [tsk/KeyMap]]
  (prof/with-timer-accum :enc-response-recs->datomic
    (with-map-vals ctx [db-uri max-tx-size]
      (datomic/peer-transact-entities db-uri max-tx-size entity-maps))))

(s/defn init-enc-response-files->datomic :- [s/Str]
  "Uses `:encounter-response-root-dir` from map `ctx` to specify a directory tree of
  Encounter Response files. For each file in turn, loads/parses the file and commits the data
  into Datomic. Returns a vector of the filenames processed.

  Assumes schema has already been transacted into Datomic. "
  [ctx :- tsk/KeyMap]
  (nl)
  (prn :init-enc-response-files->datomic--enter)
  (prof/with-timer-accum :init-enc-response-files->datomic
    (datomic/enc-response-datomic-init ctx)
    (let [enc-resp-fnames (parse/iowa-enc-response-dir->fnames ctx)]
      (prn :enc-response-files->datomic--num-files (count enc-resp-fnames))
      (nl)
      (doseq [fname enc-resp-fnames]
        (prn :enc-response-files->datomic--processing fname)
        (let [data-recs (parse/iowa-enc-response-fname->parsed fname)]
          (enc-response-recs->datomic ctx data-recs)))
      (nl)
      (prn :init-enc-response-files->datomic--num-recs (datomic/count-enc-response-recs ctx))
      enc-resp-fnames))
  (prof/print-profile-stats!)
  (prn :init-enc-response-files->datomic--leave)
  (nl))

; #todo #awt working
(s/defn init-enc-response->datomic
  [ctx]
  (prn :init-enc-response->datomic--enter)
  (prof/with-timer-print :init-enc-response->datomic
    (with-map-vals ctx [db-uri max-tx-size]
      (datomic/enc-response-datomic-init ctx)

      (let [enc-resp-fnames (parse/iowa-enc-response-dir->fnames ctx)]
        (prn :init-enc-response->datomic--num-fnames (count enc-resp-fnames))
        (doseq [fname enc-resp-fnames]
          (prn :init-enc-response->datomic----processing fname)
          (let [data-recs (parse/iowa-enc-response-fname->parsed fname)]
            (prof/with-timer-print :init-enc-response->datomic--insert
              (datomic/peer-transact-entities db-uri max-tx-size data-recs)))))
      (prof/print-profile-stats!)
      (nl)
      (prn :init-enc-response->datomic--leave))))

