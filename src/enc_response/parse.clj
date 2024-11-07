(ns enc-response.parse
  (:use tupelo.core)
  (:require
    [clojure.java.io :as io]
    [clojure.pprint :as pp]
    [clojure.tools.reader.edn :as edn]
    [enc-response.datomic :as datomic]
    [flatland.ordered.map :as omap]
    [schema.core :as s]
    [tupelo.misc :as misc]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    )
  (:gen-class)
  )

(def ^:dynamic verbose?
  "Enable to see progress printouts"
  false)

;---------------------------------------------------------------------------------------------------
(s/def encounter-response-filename-patt
  "Regex pattern for encounter response files (no parent dirs!)"
  #"^ENC_RESPONSE_D_.*.TXT$") ; eg `ENC_RESPONSE_D_20200312_062014.TXT`

(s/defn enc-resp-file-name? :- s/Bool
  [fname :- s/Str]
  (boolean (re-matches encounter-response-filename-patt fname)))

(s/defn enc-resp-file? :- s/Bool
  [file :- java.io.File]
  (it-> file
    (.getName it) ; returns string w/o parent dirs
    (enc-resp-file-name? it)))

;---------------------------------------------------------------------------------------------------
(s/def spec-opts-default :- tsk/KeyMap
  "Default options for field specs"
  {:trim?          true ; trim leading/trailing blanks from returned string

   ; Don't crash if insufficient chars found in line.  Should only be used for last N fields
   :length-strict? true})

(s/def iowa-encounter-response-specs :- [tsk/KeyMap]
  "Field specs (in order) for the Iowa Encounter Response files. Named like `ENC_RESPONSE_D_20210722_071949.TXT`.
  NOTE: all fields are strings.
     :char            - string of [a-zA-Z] chars
     :numeric         - string of [0-9] chars
     :alphanumeric    - string of [0-9a-zA-Z] chars
     :text            - string of ASCII characters  "
  [
   {:name :mco-claim-number :format :numeric :length 30} ; #todo it is a number like "30000062649905                "
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
   {:name :error-code :format :alphanumeric :length 3} ; #todo almost always "A00" (alt: "A45", "B01")
   {:name :field :format :text :length 24 :length-strict? false}
   {:name :error-field-value :format :text :length 80 :length-strict? false} ; #todo seems to be missing (all blanks!)
   ])

;---------------------------------------------------------------------------------------------------
(s/def format->pattern :- tsk/KeyMap
  "Map from format kw to regex pattern."
  {:char         #"\p{Alpha}*" ; *** empty string allowed ***
   :numeric      #"\p{Digit}*" ; *** empty string allowed ***
   :alphanumeric #"\p{Alnum}*" ; *** empty string allowed ***
   :text         #"\p{ASCII}*" ; *** empty string allowed ***
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

(s/defn load-missing-icns :- [tsk/KeyMap]
  [ctx :- tsk/KeyMap]
  (with-map-vals ctx [missing-icn-fname]
    (println "Reading: " missing-icn-fname)
    (let [missing-data (edn/read-string (slurp missing-icn-fname))]
      missing-data)))

(s/defn create-icn-maps-aug :- [tsk/KeyMap]
  [ctx :- tsk/KeyMap]
  (with-map-vals ctx [icn-maps-aug-fname]
    (let [missing-icn-maps (load-missing-icns ctx)
          icn-maps-aug     (forv [icn-map missing-icn-maps]
                             (when verbose?
                               (nl)
                               (println "seaching ENC_RESPONSE_*.TXT for icn:" icn-map))
                             (let [icn         (grab :encounter-transmission/icn icn-map)
                                   enc-resp    (->sorted-map (orig-icn->response-parsed ctx icn))
                                   iowa-tcn    (grab :iowa-transaction-control-number enc-resp)
                                   icn-map-aug (glue icn-map {:encounter-transmission/plan-icn iowa-tcn})]
                               icn-map-aug))]
      (println "Writing: " icn-maps-aug-fname)
      (spit icn-maps-aug-fname (with-out-str (pp/pprint icn-maps-aug)))
      icn-maps-aug)))

(s/defn create-tx-data-chunked :- [[tsk/KeyMap]]
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

(s/defn enc-response-fname->lines :- [s/Str]
  [fname :- s/Str]
  (let [lines (it-> fname
                (slurp it)
                (str/split-lines it)
                (drop-if #(str/whitespace? %) it))]
    lines))

(s/defn enc-response-fname->parsed :- [tsk/KeyMap]
  [fname :- s/Str]
  (let [data-recs (forv [line (enc-response-fname->lines fname)]
                    (parse-string-fields iowa-encounter-response-specs line))]
    data-recs))

(s/defn get-enc-response-fnames :- [s/Str]
  [ctx :- tsk/KeyMap]
  (let [enc-resp-root-dir-File (io/file (grab :encounter-response-root-dir ctx))
        all-files              (file-seq enc-resp-root-dir-File) ; returns a tree like `find`
        enc-resp-fnames        (sort (mapv str (keep-if enc-resp-file? all-files)))]
    enc-resp-fnames))

(s/defn enc-response-files-2-datomic :- [s/Str]
  "Uses `:encounter-response-root-dir` from map `ctx` to specify a directory of
  Encounter Response files. For each file in turn, loads/parses the file and commits the data
  into Datomic. Returns a vector of the filenames processed.

  Assumes schema has already been transacted into Datomic. "
  [ctx :- tsk/KeyMap]
  (let [enc-resp-fnames (get-enc-response-fnames ctx)]
    (nl)
    (prn :enc-response-files->datomic--num-files (count enc-resp-fnames))
    (nl)
    (doseq [fname enc-resp-fnames]
      (prn :enc-response-files->datomic--parsing fname)
      (let [data-recs (enc-response-fname->parsed fname)]
        (comment)
        (prn :enc-response-files->datomic--saving fname)
        (datomic/enc-response-recs->datomic ctx data-recs)))
    (nl)
    (prn :enc-response-files->datomic--num-recs (datomic/count-enc-response-recs ctx))
    (nl)
    enc-resp-fnames))

