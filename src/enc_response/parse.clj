(ns enc-response.parse
  (:use tupelo.core)
  (:require
    [clojure.java.io :as io]
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
(s/def spec-opts-default :- tsk/KeyMap
  "Default options for field specs"
  {:trim?          true ; trim leading/trailing blanks from returned string
   :validate?      true ; #todo add usage

   ; Don't crash if insufficient chars found in line.  Should only be used for last N fields
   :length-strict? true})
; #todo add option for :failure-type [:exception or :default-result :- tsk/KeyMap]

(s/def iowa-encounter-response-specs :- [tsk/KeyMap]
  "Field specs (in order) for the Iowa Encounter Response files. Named like `ENC_RESPONSE_D_20210722_071949.TXT`.
  NOTE: all fields are strings.
     :alpha           - string of [a-zA-Z] chars
     :digit           - string of [0-9] chars
     :alphanumeric    - string of [0-9a-zA-Z] chars
     :text            - string of ASCII characters  "
  [
   {:name :mco-claim-number :format :digit :length 30} ; #todo it is a number like "30000062649905                "
   {:name :iowa-transaction-control-number :format :digit :length 17}
   {:name :iowa-processing-date :format :digit :length 8}
   {:name :claim-type :format :alpha :length 1}
   {:name :claim-frequency-code :format :digit :length 1}
   {:name :member-id :format :alphanumeric :length 8}
   {:name :first-date-of-service :format :digit :length 8}
   {:name :billing-provider-npi :format :digit :length 10}
   {:name :mco-paid-date :format :digit :length 8}
   {:name :total-paid-amount :format :digit :length 12}
   {:name :line-number :format :digit :length 2}
   {:name :error-code :format :alphanumeric :length 3} ; #todo almost always "A00" (alt: "A45", "B01")
   {:name :field :format :text :length 24 :length-strict? false}
   {:name :error-field-value :format :text :length 80 :length-strict? false} ; #todo seems to be missing (all blanks!)
   ])

(s/def utah-encounter-response-specs-rec99999 :- [tsk/KeyMap]
  "Field specs (in order) for the Utah Encounter Response files. Named like `4950_DOHHT007992-001_15007163_20231123.txt`.
  NOTE: all fields are strings.
     :alpha           - string of [a-zA-Z] chars
     :digit           - string of [0-9] chars
     :alphanumeric    - string of [0-9a-zA-Z] chars
     :text            - string of ASCII characters  "
  [
   {:name :submitter-id :format :text :length 4}
   {:name :capitated-plan-id :format :text :length 20}
   {:name :filler-01 :format :text :length 20}
   {:name :submission-number :format :text :length 30}
   {:name :rejected-record-count :format :alphanumeric :length 10}
   {:name :filler-02 :format :text :length 20}
   {:name :filler-03 :format :text :length 3}
   {:name :filler-04 :format :text :length 1}
   {:name :filler-05 :format :text :length 1}
   {:name :error-number :format :text :length 5}
   {:name :error-severity :format :text :length 2}
   {:name :filler-06 :format :text :length 20 :length-strict? false}
   {:name :filler-07 :format :text :length 18 :length-strict? false}
   ])

(s/def utah-encounter-response-specs-rec00 :- [tsk/KeyMap]
  "Field specs (in order) for the Utah Encounter Response files. Named like `4950_DOHHT007992-001_15007163_20231123.txt`.
  NOTE: all fields are strings.
     :alpha           - string of [a-zA-Z] chars
     :digit           - string of [0-9] chars
     :alphanumeric    - string of [0-9a-zA-Z] chars
     :text            - string of ASCII characters  "
  [
   {:name :submitter-id :format :text :length 4} ; #todo it is a number like "30000062649905                "
   {:name :capitated-plan-id :format :text :length 20}
   {:name :related-plan-id :format :text :length 20}
   {:name :submission-number :format :alphanumeric :length 30}
   {:name :encounter-reference-number :format :text :length 30}
   {:name :encounter-line-number :format :text :length 3}
   {:name :record-type :format :text :length 1}
   {:name :record-category :format :text :length 1}
   {:name :error-number :format :text :length 5}
   {:name :error-severity :format :text :length 2}
   {:name :error-field :format :text :length 20}
   {:name :tcn :format :text :length 18 :length-strict? false}
   ])

;---------------------------------------------------------------------------------------------------
(s/def format->pattern :- tsk/KeyMap
  "Map from format kw to regex pattern."
  {:alpha        #"\p{Alpha}*" ; *** empty string allowed ***
   :digit        #"\p{Digit}*" ; *** empty string allowed ***
   :alphanumeric #"\p{Alnum}*" ; *** empty string allowed ***
   :text         #"\p{Print}*" ; *** empty string allowed ***
   :ascii        #"\p{ASCII}*" ; *** empty string allowed ***
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

; #todo refactor to calculate start/stop idxs, then use (substr ...) with length check or yield "" if allowed
(s/defn spec-slice
  [spec-in :- tsk/KeyMap
   char-seq-in :- [Character]]
  (let [spec (glue spec-opts-default spec-in)]
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
                      (let [rec1 (parse-string-fields iowa-encounter-response-specs line)
                            rec2 (glue rec1 {:fname-str base-str})]
                        rec2))]
      data-recs)))


