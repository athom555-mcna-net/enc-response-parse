(ns tst.enc-response-parse.core
  (:use enc-response-parse.core
        tupelo.core
        tupelo.test)
  (:require
    [clojure.data :as data]
    [clojure.java.io :as io]
    [clojure.pprint :as pp]
    [schema.core :as s]
    [tupelo.parse :as parse]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    )
  (:import
    [java.lang Character]
    [java.io File]
    ))


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

(verify
  ; works only on filename w/o parent dirs
  (isnt (enc-resp-file-name? "xyzENC_RESPONSE_D_20200312_062014.TXT"))
  (is (enc-resp-file-name? "ENC_RESPONSE_D_20200312_062014.TXT"))

  ; ignores parent dirs in path
  (is (enc-resp-file? (File. "/Users/athom555/work/iowa-response/ENC_RESPONSE_D_20200312_062014.TXT")))

  ; OK if not exist, as long as pattern matches
  (is (enc-resp-file? (File. "/Users/athom555/work/iowa-response/ENC_RESPONSE_D_xxxxxxx_062014.TXT")))
  (isnt (enc-resp-file? (File. "/Users/athom555/work/iowa-response/xxxxENC_RESPONSE_D_xxxxxxx_062014.TXT")))
  )

;---------------------------------------------------------------------------------------------------
(verify
  (let [out      "/Users/athom555/work/iowa-response/ENC_RESPONSE_D_20211202_065818.TXT:30000062649906                6213360078000001412022021D1170411          \r\n"
        expected "30000062649906                6213360078000001412022021D1170411"
        actual   (discard-grep-pretext out)]
    (is= actual expected)))

;---------------------------------------------------------------------------------------------------
(comment  ; sample output
  (verify
    (let [encounter-response-root-dir "/Users/athom555/work/iowa-response"
          enc-resp-root-dir-File      (io/file encounter-response-root-dir)
          all-files                   (file-seq enc-resp-root-dir-File) ; returns a tree like `find`
          enc-resp-files              (vec (sort-by str (keep-if enc-resp-file? all-files)))
          ]
      ;(take 5 all-files) =>
      ;[#object[java.io.File 0x15fd8daa "/Users/athom555/work/iowa-response"]
      ; #object[java.io.File 0x762b698 "/Users/athom555/work/iowa-response/ENC_RESPONSE_D_20200312_062014.TXT"]
      ; #object[java.io.File 0x5ee9866c "/Users/athom555/work/iowa-response/ENC_RESPONSE_D_20180104_065621.TXT"]
      ; #object[java.io.File 0x2f6268d9 "/Users/athom555/work/iowa-response/ENC_RESPONSE_D_20181108_061817.TXT"]
      ; #object[java.io.File 0x6d244c88 "/Users/athom555/work/iowa-response/ENC_RESPONSE_D_20210715_115630.TXT"]]
      ;(take 5 enc-resp-files) =>
      ;[#object[java.io.File 0x66a9013e "/Users/athom555/work/iowa-response/ENC_RESPONSE_D_20170413_132207.TXT"]
      ; #object[java.io.File 0x65236427 "/Users/athom555/work/iowa-response/ENC_RESPONSE_D_20170424_125320.TXT"]
      ; #object[java.io.File 0x425c5389 "/Users/athom555/work/iowa-response/ENC_RESPONSE_D_20170505_160755.TXT"]
      ; #object[java.io.File 0x3f348a38 "/Users/athom555/work/iowa-response/ENC_RESPONSE_D_20170509_144929.TXT"]
      ; #object[java.io.File 0x1e7822fb "/Users/athom555/work/iowa-response/ENC_RESPONSE_D_20170515_094001.TXT"]]
      )))

(verify
  (let [shell-result        {:exit     0
                             :out      "/Users/athom555/work/iowa-response/ENC_RESPONSE_D_20211202_065818.TXT:30000062649906                6213360078000001412022021D11704114C0701202119527117801124202100000000000000A00DENIED                                                                                                  \r\n"
                             :err      ""
                             :cmd-str  "grep '^30000062649906' /Users/athom555/work/iowa-response/ENC_*.TXT"
                             :os-shell "/bin/bash"}
        enc-response-parsed (extract-enc-resp-fields shell-result)]
    (is= enc-response-parsed {:mco-claim-number                "30000062649906"
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
                              :error-field-value               ""})))

(verify
  (let [encounter-response-root-dir "./enc-response-files-test"
        icn-str                     "30000062649906"
        enc-resp-parsed (orig-icn->response-parsed encounter-response-root-dir icn-str)]
    (is= enc-resp-parsed
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
    )
  )

