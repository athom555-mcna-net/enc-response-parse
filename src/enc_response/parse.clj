(ns enc-response.parse
  (:use tupelo.core)
  (:require
    [clojure.java.io :as io]
    [enc-response.parse.specs :as specs]
    [flatland.ordered.map :as omap]
    [schema.core :as s]
    [tupelo.profile :as prof]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    ))

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

(s/defn enc-resp-File? :- s/Bool
  [file :- java.io.File]
  (it-> file
    (.getName it) ; returns string w/o parent dirs
    (enc-resp-file-name? it)))

(s/defn enc-response-dir->fnames :- [s/Str]
  [ctx :- tsk/KeyMap]
  (let [enc-resp-root-dir-File (io/file (grab :encounter-response-root-dir ctx))
        all-files              (file-seq enc-resp-root-dir-File) ; returns a tree like `find`
        enc-resp-fnames        (sort (mapv str (keep-if enc-resp-File? all-files)))]
    enc-resp-fnames))

(s/defn enc-response-fname->base-str :- s/Str
  [fname :- s/Str]
  (let [r1   (re-matches #".*(ENC_RESPONSE_.*.TXT)" fname)
        base (xsecond r1)]
    base))

(s/defn enc-response-fname->utc-str :- s/Str
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
    (with-map-vals spec [name format length trim? validate? length-strict? ]
      (assert-info (pos? length) "field length must be positive" (vals->map spec))
      ; NOTE: `split-at` works for both a string and a char seq
      (let [[field-chars rem-chars-seq] (split-at length char-seq-in)]
        (when length-strict?
          (assert-info (= length (count field-chars)) "field chars missing" (vals->map spec-in field-chars rem-chars-seq)))
        (let [field-str (cond-it-> (str/join field-chars)
                          trim? (str/trim it))
              result    {:state  {:chars-remaining rem-chars-seq}
                         :output {name field-str}}]
          (when validate?
            (validate-format format field-str))
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

(s/defn utah-9999-line? :- s/Bool
  [line :- s/Str]
  (let [rec (parse-string-fields specs/utah-encounter-response-common line)]
    (with-map-vals rec
      [encounter-line-number encounter-reference-number error-field error-number
       record-type related-plan-id submission-number tcn]
      (and
        (= encounter-line-number "")
        (= encounter-reference-number "0000000000")
        (= error-field "")
        (= error-number "99999")
        (= record-type "")
        (= related-plan-id "")
        (not= submission-number "")
        (= tcn "")))))

;---------------------------------------------------------------------------------------------------
(s/defn enc-response-fname->lines :- [s/Str]
  [fname :- s/Str]
  (let [lines (it-> fname
                (slurp it)
                (str/split-lines it)
                (drop-if str/whitespace? it))]
    lines))

(s/defn enc-response-fname->parsed :- [tsk/KeyMap]
  [fname :- s/Str]
  (prof/with-timer-accum :enc-response-fname->parsed
    (let [base-str  (enc-response-fname->base-str fname)
          data-recs (forv [line (enc-response-fname->lines fname)]
                      (let [rec1 (parse-string-fields specs/iowa-encounter-response line)
                            rec2 (glue rec1 {:fname-str base-str})]
                        rec2))]
      data-recs)))


