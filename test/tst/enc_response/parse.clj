(ns tst.enc-response.parse
  (:use enc-response.parse
        tupelo.core
        tupelo.test)
  (:require
    [clojure.data :as data]
    [clojure.java.io :as io]
    [clojure.pprint :as pp]
    [enc-response.datomic :as datomic]
    [enc-response.proc :as proc]
    [tupelo.parse :as parse]
    )
  (:import
    [java.io File]
    ))

; Enable to see progress printouts
(def ^:dynamic verbose-tests?
  false)

(def ctx-local
  {:encounter-response-root-dir "./enc-response-files-test" ; full data:  "/Users/athom555/work/iowa-response"
   :missing-icn-fname           "resources/missing-icns-5.edn"
   :icn-maps-aug-fname          "icn-maps-aug.edn"
   :tx-data-chunked-fname       "tx-data-chuncked.edn"
   :tx-size-limit               2
   })

;-----------------------------------------------------------------------------
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
  (throws? (validate-format :numeric "abc9"))

  (is= "a" (validate-format :text "a")) ; any ASCII character
  (is= "9" (validate-format :text "9"))
  (is= "abc9" (validate-format :text "abc9"))
  (is= "#abc" (validate-format :text "#abc"))
  (is= "#ab,c!" (validate-format :text "#ab,c!"))
  (is= "#ab, c!" (validate-format :text "#ab, c!")))

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
  (let [out      "./iowa-response/ENC_RESPONSE_D_20211202_065818.TXT:30000062649906                6213360078000001412022021D1170411          \r\n"
        actual   (parse-grep-result out)
        expected {:content "30000062649906                6213360078000001412022021D1170411",
                  :fname   "./iowa-response/ENC_RESPONSE_D_20211202_065818.TXT"}]
    (is= actual expected)))

;---------------------------------------------------------------------------------------------------
(comment  ; sample output
  (verify
    (with-redefs [encounter-response-root-dir "./enc-response-files-test" ; full data:  "/Users/athom555/work/iowa-response"
                  ]
      (let [enc-resp-root-dir-File (io/file encounter-response-root-dir)
            all-files              (file-seq enc-resp-root-dir-File) ; returns a tree like `find`
            enc-resp-files         (vec (sort-by str (keep-if enc-resp-file? all-files)))
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
        ))))

(verify
  (with-redefs [verbose? verbose-tests?]
    (let [shell-result        {:exit     0
                               :out      "./iowa-response/ENC_RESPONSE_D_20211202_065818.TXT:30000062649906                6213360078000001412022021D11704114C0701202119527117801124202100000000000000A00DENIED                                                                                                  \r\n"
                               :err      ""
                               :cmd-str  "grep '^30000062649906' ./iowa-response/ENC_*.TXT"
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
                                :error-field-value               ""}))))

(verify
  (with-redefs [verbose? verbose-tests?]
    (let [icn-str         "30000062649906"
          enc-resp-parsed (grep-orig-icn->response-parsed ctx-local icn-str)]
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
      ))
  )

(comment  ; sample datomic record from heron-qa
  (pp/pprint (d/pull db '[*] 17592186108600))
  {:encounter-transmission/generation               1673971295
   :encounter-transmission/frequency                #:db{:id 17592186045418}
   :encounter-transmission/access-point-medicaid-id "399584705"
   :encounter-transmission/status                   #:db{:id 17592186045428}
   :encounter-transmission/facility-ein             "453596313"
   :encounter-transmission/payer-claim-ids          ["1166980510631" "1167180886508"]
   :encounter-transmission/plan                     "tx-medicaid"
   :db/id                                           17592186108600
   :encounter-transmission/encounter-data           #uuid "637f06c2-de73-40f3-a671-1e8ff01d56bd"
   :encounter-transmission/icn                      "30000000165819"
   :inbound-encounter-status/timestamp              #inst "2023-02-08T17:19:12.995-00:00"
   :encounter-transmission/previous-icn             "30000000165694"
   :encounter-transmission/billing-provider-npi     "1831475300"}
  )

(verify
  (when false
    (let [icn-str         "30000062649906"
          ; Each call requires about 0.5 sec for full data search
          enc-resp-parsed (grep-orig-icn->response-parsed ctx-local icn-str)]
      (pp/pprint enc-resp-parsed))))

(verify
  (when false
    ; (prn :-----------------------------------------------------------------------------)
    (with-redefs [verbose? verbose-tests?]
      (proc/load-missing-icns ctx-local))
    ; (prn :-----------------------------------------------------------------------------)
    ))

(comment
  (verify
    (prn :-----------------------------------------------------------------------------)
    (with-redefs [verbose? verbose-tests?]
      (let [icn-maps-aug (proc/create-icn-maps-aug-grep ctx-local)]
        ; (spyx-pretty icn-maps-aug)
        (is (->> icn-maps-aug
              (wild-submatch?
                [{:db/id                           datomic/eid?
                  :encounter-transmission/icn      "30000000100601",
                  :encounter-transmission/plan     "id-medicaid",
                  :encounter-transmission/plan-icn "62133600780000001",
                  :encounter-transmission/status
                  #:db{:db/id datomic/eid?
                       :ident :encounter-transmission.status/rejected-by-validation}}
                 {:db/id                           17592186047700,
                  :encounter-transmission/icn      "30000000102936",
                  :encounter-transmission/plan     "id-medicaid",
                  :encounter-transmission/plan-icn "62133600780000002",
                  :encounter-transmission/status
                  #:db{:ident :encounter-transmission.status/accepted}}
                 {:db/id                           17592186047701,
                  :encounter-transmission/icn      "30000000102990",
                  :encounter-transmission/plan     "id-medicaid",
                  :encounter-transmission/plan-icn "62134500780000003",
                  :encounter-transmission/status
                  #:db{:ident :encounter-transmission.status/accepted}}
                 {:db/id                           17592186126919,
                  :encounter-transmission/icn      "30000000217708",
                  :encounter-transmission/plan     "ut-medicaid",
                  :encounter-transmission/plan-icn "62135000780000004",
                  :encounter-transmission/status
                  #:db{:ident :encounter-transmission.status/rejected}}
                 {:encounter-transmission/icn      "30000000222291",
                  :encounter-transmission/plan     "tx-medicaid",
                  :encounter-transmission/plan-icn "62200600780000005",
                  :encounter-transmission/status
                  #:db{:ident :encounter-transmission.status/accepted}}]))))

      (let [tx-data-chunked (proc/create-tx-data-chunked ctx-local)]
        (is (->> tx-data-chunked ; alternate style with variable "first"
              (wild-match?
                [[{:db/id                           datomic/eid?
                   :encounter-transmission/plan-icn "62133600780000001"}
                  {:db/id                           datomic/eid?
                   :encounter-transmission/plan-icn "62133600780000002"}]
                 [{:db/id                           datomic/eid?
                   :encounter-transmission/plan-icn "62134500780000003"}
                  {:db/id                           datomic/eid?
                   :encounter-transmission/plan-icn "62135000780000004"}]
                 [{:db/id                           datomic/eid?
                   :encounter-transmission/plan-icn "62200600780000005"}]]
                )))))
    (prn :-----------------------------------------------------------------------------)
    ))
