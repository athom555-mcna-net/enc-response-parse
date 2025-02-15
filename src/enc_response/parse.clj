(ns enc-response.parse
  (:use tupelo.core)
  (:require
    [clojure.java.io :as io]
    [enc-response.parse.specs :as specs]
    [flatland.ordered.map :as omap]
    [schema.core :as s]
    [tupelo.profile :as prof]
    [tupelo.schema :as tsk]
    [tupelo.set :as set]
    [tupelo.string :as str]
    ))

(def ^:dynamic verbose?
  "Enable to see progress printouts"
  false)

;---------------------------------------------------------------------------------------------------
(s/def iowa-encounter-response-filename-patt
  "Regex pattern for encounter response files (no parent dirs!)"
  #"^ENC_RESPONSE_D_.*.TXT$") ; eg `ENC_RESPONSE_D_20200312_062014.TXT`

(s/defn iowa-enc-resp-file-name? :- s/Bool
  [fname :- s/Str]
  (boolean (re-matches iowa-encounter-response-filename-patt fname)))

(s/defn iowa-enc-resp-File? :- s/Bool
  [file :- java.io.File]
  (it-> file
    (.getName it) ; returns string w/o parent dirs
    (iowa-enc-resp-file-name? it)))

(s/defn iowa-enc-response-dir->fnames :- [s/Str]
  [ctx :- tsk/KeyMap]
  (let [enc-resp-root-dir-File (io/file (grab :encounter-response-root-dir ctx))
        all-files              (file-seq enc-resp-root-dir-File) ; returns a tree like `find`
        enc-resp-fnames        (sort (mapv str (keep-if iowa-enc-resp-File? all-files)))]
    enc-resp-fnames))

(s/defn iowa-enc-response-fname->base-str :- s/Str
  [fname :- s/Str]
  (let [r1   (re-matches #".*(ENC_RESPONSE_.*.TXT)" fname)
        base (xsecond r1)]
    base))

(s/defn iowa-enc-response-fname->utc-str :- s/Str
  [fname :- s/Str]
  (let [r1               (re-matches #".*ENC_RESPONSE_._(\d{8})_(\d{6}).TXT" fname)
        date-str         (nth r1 1)
        time-str         (nth r1 2)
        year             (subs date-str 0 4)
        month            (subs date-str 4 6)
        day              (subs date-str 6 8)
        hour             (subs time-str 0 2)
        min              (subs time-str 2 4)
        sec              (subs time-str 4 6)
        utc-date-str     (format "%4s-%2s-%2s" year month day)
        utc-time-str     (format "%2s:%2s:%2s" hour min sec)
        utc-datetime-str (str utc-date-str \T utc-time-str)]
    utc-datetime-str))

;---------------------------------------------------------------------------------------------------

(s/defn validate-format :- s/Str
  "Perform a regex check to validates a string format. Returns the string upon success, else
  throws."
  [format :- s/Keyword
   src :- s/Str]
  (let [regex-patt (grab format specs/format->pattern)]
    (when (nil? (re-matches regex-patt src))
      (throw (ex-info "string does not match format" (vals->map src format)))))
  src)    ; return input if passes

; #todo refactor to calculate start/stop idxs, then use (substr ...) with length check or yield "" if allowed
(s/defn spec-slice
  [spec-in :- tsk/KeyMap
   char-seq-in :- [Character]]
  (let [spec (glue specs/opts-default spec-in)]
    (with-map-vals spec [name format length trim? validate?]
      (assert-info (pos? length) "field length must be positive" (vals->map spec))
      ; NOTE: `split-at` works for both a string and a char seq
      (let [[field-chars rem-chars-seq] (split-at length char-seq-in)
            field-str (cond-it-> (str/join field-chars)
                        trim? (str/trim it))
            result    {:state  {:chars-remaining rem-chars-seq}
                       :output {name field-str}}]
        (when validate?
          (validate-format format field-str))
        result))))

(s/defn parse-string-fields :- tsk/KeyMap
  "Parse the input string the supplied fields specification"
  [field-specs :- [tsk/KeyMap]
   input-str :- s/Str]
  (let [total-chars (apply + (mapv :length field-specs))
        padding     (repeat total-chars \space)]
    (loop [specs  field-specs
           chars  (glue (seq input-str) padding)
           result (omap/ordered-map)]
      (if (empty? specs)
        result ; return
        (let [spec-curr   (xfirst specs)
              ; >> (spyx spec-curr)
              slice-out   (spec-slice spec-curr chars)
              ; >> (spyx slice-out)
              specs-next  (xrest specs)
              chars-next  (fetch-in slice-out [:state :chars-remaining])
              result-next (glue result (grab :output slice-out))]
          (recur specs-next chars-next result-next))))))

;---------------------------------------------------------------------------------------------------
(s/defn enc-response-fname->lines :- [s/Str]
  [fname :- s/Str]
  (let [lines (it-> fname
                (slurp it)
                (str/split-lines it)
                (drop-if str/whitespace? it))]
    lines))

(s/defn iowa-enc-response-fname->parsed :- [tsk/KeyMap]
  [fname :- s/Str]
  (prof/with-timer-accum :iowa-enc-response-fname->parsed
    (let [base-str  (iowa-enc-response-fname->base-str fname)
          data-recs (forv [line (enc-response-fname->lines fname)]
                      (let [rec1 (parse-string-fields specs/iowa-encounter-response line)
                            rec2 (glue rec1 {:fname-str base-str})]
                        rec2))]
      data-recs)))

;---------------------------------------------------------------------------------------------------
(s/defn error-severity->outstr :- s/Str
  "Convert the input field :error-severity to value wanted in TSV file for DB"
  [error-severity :- s/Str]
  (let [str-lc (str/lower-case error-severity)
        result (cond
                 (= str-lc "io") "warning"
                 (contains-key? #{"re" "rl" "rb"} str-lc) "error"
                 :else "unknown")]
    result))

(s/defn utah-99999-rec? :- s/Bool
  [line :- s/Str]
  (let [parsed-rec (parse-string-fields specs/utah-encounter-response-common line)]
    ; #todo maybe use only the "key field" values for test???
    (with-map-vals parsed-rec
      [encounter-line-number encounter-reference-number error-field error-number
       record-type related-plan-id submission-number tcn]
      (and
        ; (= encounter-line-number "")
        (= encounter-reference-number "0000000000") ; *** key field ***
        ; (= error-field "")
        (= error-number "99999") ; *** key field ***
        ; (= record-type "")
        (= related-plan-id "") ; *** key field ***
        ; (not= submission-number "")
        ; (= tcn "")
        ))))

(s/defn utah-rejected-fname->icns :- #{s/Str}
  "Parse the Utah rejected claims file and return a list of ICNs as strings."
  [fname :- s/Str]
  (let [rejected-src (slurp fname)
        lines-1      (str/split-lines rejected-src)
        lines-2      (mapv str/trim (xdrop 2 lines-1))
        lines-3      (forv [line lines-2]
                       (let [tokens (map str/trim (str/split line #"\|"))
                             icn    (xfirst tokens)]
                         icn))
        result       (into (sorted-set) lines-3)]
    result))

(def fname-utah-rejected-src "resources/ut_rejected_claims_to_icn_map.txt")
(s/def utah-rejected-icns :- #{s/Str}
  (utah-rejected-fname->icns fname-utah-rejected-src))

(s/defn utah-enc-response-fname->parsed :- [tsk/KeyMap]
  [fname :- s/Str]
  (let [lines-1   (enc-response-fname->lines fname)
        lines-2   (xrest lines-1) ; drop hdr line
        lines-3   (drop-if utah-99999-rec? lines-2)
        data-recs (forv [line lines-3]
                    (parse-string-fields specs/utah-encounter-response-rec00 line))]
    data-recs))

(s/defn utah-enc-response-fname->by-enc-ref :- {s/Str [tsk/KeyMap]}
  [fname :- s/Str]
  (let [data-recs  (utah-enc-response-fname->parsed fname)
        by-enc-ref (group-by :encounter-reference-number data-recs)]
    (glue (sorted-map) by-enc-ref)))

(s/defn utah-enc-response-fname->by-enc-ref-rej-only :- {s/Str [tsk/KeyMap]} ; #todo needs a test
  [fname :- s/Str]
  (let [by-enc-ref    (utah-enc-response-fname->by-enc-ref fname)
        all-icns      (set (keys by-enc-ref))
        common-icns   (set/intersection all-icns utah-rejected-icns)
        rejected-only (glue (sorted-map) (submap-by-keys by-enc-ref common-icns))]
    rejected-only))

(s/defn utah-enc-response-fname->tsv-recs :- [{s/Str [{s/Str s/Str}]}]
  [fname :- s/Str]
  (let [by-enc-ref (utah-enc-response-fname->by-enc-ref-rej-only fname)
        tsv-recs   (forv [[icn-num enc-recs] by-enc-ref]
                     (let [error-recs (forv [enc-rec enc-recs]
                                        (with-map-vals enc-rec [error-number error-severity]
                                          {"code"     error-number
                                           "severity" (error-severity->outstr error-severity)}))]
                       {icn-num error-recs}))]
    tsv-recs))

(s/defn utah-enc-response-fname->tsv-file
  [fname-enc-resp :- s/Str
   file-tsv] ;
  (let [tsv-recs  (utah-enc-response-fname->tsv-recs fname-enc-resp)
        out-lines (forv [tsv-rec tsv-recs]
                    (assert (= 1 (count tsv-rec))) ; only 1 entry per map
                    (let [entry           (first tsv-rec)
                          icn             (key entry)
                          error-info      (val entry)
                          error-info-json (edn->json error-info)
                          out-line        (str icn \tab error-info-json)]
                      out-line))
        out-str   (str/join \newline out-lines)]
    (spit file-tsv out-str)
    nil))

