(ns tst.enc-response-parse.core
  (:use enc-response-parse.core
        tupelo.core
        tupelo.test)
  (:require
    [clojure.pprint :as pp]
    [schema.core :as s]
    [tupelo.core :as t]
    [tupelo.parse :as parse]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    )
  (:import [java.lang Character]))


(verify
  (throws? (validate-format :charxxx "abc")) ; must be valid format

  (throws-not? (validate-format :char "")) ; empty str legal
  (throws-not? (validate-format :numeric ""))
  (throws-not? (validate-format :alphanumeric ""))
  (throws? (validate-format :char (vec "abc"))) ; must be str, not charseq

  (is= "a" (validate-format :char "a")) ; alphabetic
  (is= "abc" (validate-format :char "abc"))
  (throws? (validate-format :numeric "abc"))
  (throws? (validate-format :char "abc9"))

  (is= "a" (validate-format :alphanumeric "a")) ; alphanumeric
  (is= "9" (validate-format :alphanumeric "9"))
  (is= "abc9" (validate-format :alphanumeric "abc9"))
  (throws? (validate-format :alphanumeric "#abc"))

  (is= "9" (validate-format :numeric "9")) ; numeric
  (is= "123" (validate-format :numeric "123"))
  (throws? (validate-format :numeric "abc"))
  (throws? (validate-format :numeric "abc9")))

(verify
  (throws? (spec-slice {:name :xxx :format :char :length 0} (vec "abcdefg"))) ; zero length
  (throws? (spec-slice {:name :xxx :format :char :length 3} (vec "ab"))) ; insufficient chars
  (throws? (spec-slice {:name :xxx :format :char :length 3 :length-strict? true} (vec "ab"))) ; insufficient chars
  (throws-not? (spec-slice {:name :xxx :format :char :length 3 :length-strict? false} (vec "ab"))) ; insufficient chars

  (is= (spec-slice {:name :xxx :format :char :length 3} (vec "abc"))
    {:state  {:chars-remaining []}
     :output {:xxx "abc"}})
  (is= (spec-slice {:name :xxx :format :char :length 3} (vec "abcdefg"))
    {:state  {:chars-remaining (vec "defg")}
     :output {:xxx "abc"}}))

(verify
  (is= 123 (parse/parse-int "123"))
  (is= -1 (with-exception-default -1
            (parse/parse-int "12x")))
  (is= 123 (parse/parse-int "123" :default 0))
  (is= 0 (parse/parse-int "12x" :default 0)))

(verify
  (let [field-specs [{:name :a :format :char :length 1}
                     {:name :bb :format :numeric :length 2}
                     {:name :ccc :format :alphanumeric :length 3 :length-strict? false}]]
    (is= (parse-string-fields field-specs "a23cc3")
      {:a "a", :bb "23", :ccc "cc3"})
    (is= (parse-string-fields field-specs "a23cc3dddd") ; extra chars ignored
      {:a "a", :bb "23", :ccc "cc3"})
    (throws? (parse-string-fields field-specs "abbccc")) ; bb wrong format
    (throws-not? (parse-string-fields field-specs "a23cc")) ; insufficient chars ok in last field
    ))

(verify
  (let [rec-1    "30000062649905                6213360078000001312022021D71704114C0311202119527117801124202100000002296800A00PAID          "
        rec-2    "30000062649906                6213360078000001412022021D11704114C0701202119527117801124202100000000000000A00DENIED        "
        parsed-1 (parse-string-fields iowa-encounter-response-specs rec-1)
        parsed-2 (parse-string-fields iowa-encounter-response-specs rec-2)]
    ; (pp/pprint parsed-1)
    (is= parsed-1
      {:mco-claim-number                "30000062649905" ; Note:  numeric fields still returned as strings!
       :iowa-transaction-control-number "62133600780000013"
       :iowa-processing-date            "12022021"
       :claim-type                      "D"
       :claim-frequency-code            "7"
       :member-id                       "1704114C"
       :first-date-of-service           "03112021"
       :billing-provider-npi            "1952711780"
       :mco-paid-date                   "11242021"
       :total-paid-amount               "000000022968"
       :line-number                     "00"
       :error-code                      "A00"
       :field                           "PAID"
       :error-field-value               ""})

    ; (pp/pprint parsed-2)
    (is= parsed-2
      {:mco-claim-number                "30000062649906"
       :iowa-transaction-control-number "62133600780000014"
       :iowa-processing-date            "12022021"
       :claim-type                      "D"
       :claim-frequency-code            "1"
       :member-id                       "1704114C"
       :first-date-of-service           "07012021"
       :billing-provider-npi            "1952711780"
       :mco-paid-date                   "11242021"
       :total-paid-amount               "000000000000"
       :line-number                     "00"
       :error-code                      "A00"
       :field                           "DENIED"
       :error-field-value               ""})
    ))
