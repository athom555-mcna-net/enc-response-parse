(ns enc-response.proc
  (:use tupelo.core)
  (:require
    [clojure.data :as data]
    [clojure.java.io :as io]
    [clojure.pprint :as pp]
    [clojure.tools.reader.edn :as edn]
    [datomic.api :as d.peer]
    [enc-response.datomic :as datomic]
    [enc-response.parse :as parse]
    [enc-response.util :as util]
    [schema.core :as s]
    [tupelo.misc :as misc]
    [tupelo.profile :as prof]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    )
  (:gen-class))

(def ^:dynamic verbose?
  "Enable to see progress printouts"
  false)

(s/defn load-missing-icns :- [tsk/KeyMap]
  [ctx :- tsk/KeyMap]
  (with-map-vals ctx [missing-icn-fname]
    (println "Reading: " missing-icn-fname)
    (prof/with-timer-print :reading-file
      (let [missing-data (edn/read-string (slurp missing-icn-fname))]
        missing-data))))

(comment
  (s/defn create-icn-maps-aug-grep :- [tsk/KeyMap]
    [ctx :- tsk/KeyMap]
    (with-map-vals ctx [icn-maps-aug-fname]
      (let [missing-icn-maps (load-missing-icns ctx)
            icn-maps-aug     (forv [icn-map missing-icn-maps]
                               (when verbose?
                                 (nl)
                                 (println "seaching ENC_RESPONSE_*.TXT for icn:" icn-map))
                               (let [icn         (grab :encounter-transmission/icn icn-map)
                                     enc-resp    (->sorted-map (parse/grep-orig-icn->response-parsed ctx icn))
                                     iowa-tcn    (grab :iowa-transaction-control-number enc-resp)
                                     icn-map-aug (glue icn-map {:encounter-transmission/plan-icn iowa-tcn})]
                                 icn-map-aug))]
        (println "Writing: " icn-maps-aug-fname)
        (spit icn-maps-aug-fname (with-out-str (pp/pprint icn-maps-aug)))
        icn-maps-aug))))

(s/defn get-enc-response-fnames :- [s/Str]
  [ctx :- tsk/KeyMap]
  (let [enc-resp-root-dir-File (io/file (grab :encounter-response-root-dir ctx))
        all-files              (file-seq enc-resp-root-dir-File) ; returns a tree like `find`
        enc-resp-fnames        (sort (mapv str (keep-if parse/enc-resp-file? all-files)))]
    enc-resp-fnames))

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

(s/defn enc-response-files->datomic :- [s/Str]
  "Uses `:encounter-response-root-dir` from map `ctx` to specify a directory of
  Encounter Response files. For each file in turn, loads/parses the file and commits the data
  into Datomic. Returns a vector of the filenames processed.

  Assumes schema has already been transacted into Datomic. "
  [ctx :- tsk/KeyMap]
  (prof/with-timer-accum :enc-response-files->datomic
    (let [enc-resp-fnames (get-enc-response-fnames ctx)]
      (nl)
      (prn :enc-response-files->datomic--num-files (count enc-resp-fnames))
      (nl)
      (doseq [fname enc-resp-fnames]
        (prn :enc-response-files->datomic--processing fname)
        (let [data-recs (parse/enc-response-fname->parsed fname)]
          (datomic/enc-response-recs->datomic ctx data-recs)))
      (nl)
      (prn :enc-response-files->datomic--num-recs (datomic/count-enc-response-recs ctx))
      (nl)
      enc-resp-fnames))
  (prof/print-profile-stats!)
  (nl))

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

(s/defn create-icn-maps-aug-datomic :- [tsk/KeyMap]
  [ctx :- tsk/KeyMap]
  (spyx (datomic/count-enc-response-recs ctx))
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
      (prof/with-timer-print :writing-file
        (spit icn-maps-aug-fname (with-out-str (pp/pprint icn-maps-aug))))
      icn-maps-aug)))

(s/defn icn-maps-aug->tx-data-chunked :- [[tsk/KeyMap]]
  [ctx]
  (prn :create-tx-data-chunked--enter)
  (with-map-vals ctx [icn-maps-aug-fname tx-data-chunked-fname tx-size-limit]
    (let [icn-maps-aug    (edn/read-string (slurp icn-maps-aug-fname))
          tx-data         (keep-if not-nil? ; skip if plan-icn not found #todo unnecessary?
                            (forv [icn-map-aug icn-maps-aug]
                              (let [eid      (grab :db/id icn-map-aug)
                                    icn      (grab :encounter-transmission/icn icn-map-aug)
                                    plan-icn (grab :encounter-transmission/plan-icn icn-map-aug)]
                                (if (truthy? plan-icn) ; skip if plan-icn not found #todo unnecessary?
                                  {:db/id                           eid
                                   :encounter-transmission/plan-icn plan-icn}
                                  (prn :skipping-nil--plan-icn icn)))))
          tx-data-chunked (unlazy (partition-all tx-size-limit tx-data))]
      (println "Writing: " tx-data-chunked-fname)
      (spit tx-data-chunked-fname (with-out-str (pp/pprint tx-data-chunked)))
      (with-result tx-data-chunked
        (prn :create-tx-data-chunked--leave)))))

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
