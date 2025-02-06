(ns enc-response.parse.specs
  (:use tupelo.core)
  (:require
    [clojure.java.io :as io]
    [flatland.ordered.map :as omap]
    [schema.core :as s]
    [tupelo.profile :as prof]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    ))


(s/def opts-default :- tsk/KeyMap
  "Default options for field specs"
  {:trim?          true ; trim leading/trailing blanks from returned string
   :validate?      true ; preform regex validation of fields

   ; Don't crash if insufficient chars found in line.  Should only be used for last N fields
   :length-strict? true})
; #todo add option for :failure-type [:exception or :default-result :- tsk/KeyMap]

(s/def format->pattern :- tsk/KeyMap
  "Map from format kw to regex pattern."
  {:alpha        #"\p{Alpha}*" ; *** empty string allowed ***
   :digit        #"\p{Digit}*" ; *** empty string allowed ***
   :alphanumeric #"\p{Alnum}*" ; *** empty string allowed ***
   :text         #"\p{Print}*" ; *** empty string allowed ***
   :ascii        #"\p{ASCII}*" ; *** empty string allowed ***
   })

(s/def iowa-encounter-response :- [tsk/KeyMap]
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

(s/def utah-encounter-response-hdr :- [tsk/KeyMap]
  "Field specs (in order) for the Utah Encounter Response files. Named like `4950_DOHHT007992-001_15007163_20231123.txt`.
  NOTE: all fields are strings.
     :alpha           - string of [a-zA-Z] chars
     :digit           - string of [0-9] chars
     :alphanumeric    - string of [0-9a-zA-Z] chars
     :text            - string of ASCII characters  "
  [
   {:name :field-01 :format :text :length 4} ; HDDR
   {:name :field-02 :format :text :length 4} ; MAMM
   {:name :field-03 :format :text :length 2} ; IS
   {:name :file-type :format :text :length 4} ;  4950
   {:name :file-date-01 :format :digit :length 8} ; 20231123
   {:name :file-date-02 :format :digit :length 8} ; 20231123
   {:name :field-04 :format :text :length 4} ;  blank
   {:name :file-type-02 :format :text :length 4} ;  4950
   {:name :category :format :text :length 4} ; PROD
   ])

(s/def utah-encounter-response-common :- [tsk/KeyMap]
  "Field specs (in order) for the Utah Encounter Response files. Named like `4950_DOHHT007992-001_15007163_20231123.txt`. "
  [{:name :submitter-id :format :text :length 4} ; #todo it is a number like "30000062649905                "
   {:name :capitated-plan-id :format :text :length 20}
   {:name :related-plan-id :format :text :length 20}
   {:name :submission-number :format :alphanumeric :length 30}
   {:name :encounter-reference-number :format :text :length 30}
   {:name :encounter-line-number :format :text :length 3}
   {:name :record-type :format :text :length 1}
   {:name :record-category :format :text :length 1}
   {:name :error-number :format :text :length 5}
   {:name :error-severity :format :text :length 2}
   {:name :error-field :format :text :length 20  :length-strict? false}
   {:name :tcn :format :text :length 18 :length-strict? false}
   ]
  )

(s/def utah-encounter-response-rec00 :- [tsk/KeyMap]
  "Field specs (in order) for the Utah Encounter Response files. Named like `4950_DOHHT007992-001_15007163_20231123.txt`.
  NOTE: all fields are strings.
     :alpha           - string of [a-zA-Z] chars
     :digit           - string of [0-9] chars
     :alphanumeric    - string of [0-9a-zA-Z] chars
     :text            - string of ASCII characters  "
  [{:name :submitter-id :format :text :length 4} ; #todo it is a number like "30000062649905                "
   {:name :capitated-plan-id :format :text :length 20}
   {:name :related-plan-id :format :text :length 20}
   {:name :submission-number :format :alphanumeric :length 30}
   {:name :encounter-reference-number :format :text :length 30}
   {:name :encounter-line-number :format :text :length 3}
   {:name :record-type :format :text :length 1}
   {:name :record-category :format :text :length 1}
   {:name :error-number :format :text :length 5}
   {:name :error-severity :format :text :length 2}
   {:name :error-field :format :text :length 20 :length-strict? false }
   {:name :tcn :format :text :length 18 :length-strict? false}]
  )

(s/def utah-encounter-response-rec99999 :- [tsk/KeyMap]
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

