(ns enc-response-parse.core
  (:use tupelo.core)
  (:require
    [flatland.ordered.map :as omap]
    [schema.core :as s]
    [tupelo.core :as t]
    [tupelo.misc :as misc]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    )
  (:import
    [java.io File])
  (:gen-class))

(s/def encounter-response-filename-patt
  "Regex pattern for encounter response files (no parent dirs!)"
  #"^ENC_RESPONSE_D_.*.TXT$") ; eg `ENC_RESPONSE_D_20200312_062014.TXT`

(s/def spec-opts-default :- tsk/KeyMap
  "Default options for field specs"
  {:trim? true ; trim leading/trailing blanks from returned string

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
   {:name :error-field-value :format :alphanumeric :length 80 :length-strict? false}  ; #todo seems to be missing (all blanks!)
   ])

;---------------------------------------------------------------------------------------------------
(def format->pattern
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

(s/defn discard-grep-pretext ; :- s/Str
  [out :- s/Str]
  (let [out (str/trim out)]
    (xsecond (re-matches #"^.*ENC_.*.TXT:(.*)$" out))))

(s/defn extract-enc-resp-fields ; :- s/Str
  [shell-result :- tsk/KeyMap]
  (with-map-vals shell-result [exit out err]
    (assert (= 0 exit))
    (assert (= "" err))
    (let [enc-response-line   (discard-grep-pretext out)
          enc-response-parsed (parse-string-fields iowa-encounter-response-specs enc-response-line)]
      enc-response-parsed)))

(s/defn orig-icn->response-parsed :- tsk/KeyMap
  [encounter-response-root-dir :- s/Str
   icn-str :- s/Str]
  (let [icn-str             (str/trim icn-str)
        shell-cmd-str       (format "grep '^%s' %s/ENC_*.TXT" icn-str encounter-response-root-dir)
        shell-result        (misc/shell-cmd shell-cmd-str)
        enc-response-parsed (extract-enc-resp-fields shell-result)]
    enc-response-parsed))

(defn -main
  [& args]
  (spy :main--enter)
  (spy :main--leave))
