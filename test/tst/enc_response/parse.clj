(ns
  tst.enc-response.parse
  (:use enc-response.parse
        tupelo.core
        tupelo.test)
  (:require
    [clojure.data :as data]
    [clojure.java.io :as io]
    [clojure.pprint :as pp]
    [enc-response.datomic :as datomic]
    [enc-response.proc :as proc]
    [enc-response.parse.specs :as specs]
    [schema.core :as s]
    [tupelo.csv :as csv]
    [tupelo.io :as tio]
    [tupelo.string :as str]
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
   :tx-data-fname               "tx-data.edn"
   :max-tx-size                 3
   })

;-----------------------------------------------------------------------------
(verify
  (throws? (validate-format :charxxx "abc")) ; format kw must be valid

  (throws-not? (validate-format :alpha "")) ; empty str legal
  (throws-not? (validate-format :digit ""))
  (throws-not? (validate-format :alphanumeric ""))
  (throws? (validate-format :alpha (vec "abc"))) ; must be str not charseq

  (is= "a" (validate-format :alpha "a")) ; :char => alpha only
  (is= "abc" (validate-format :alpha "abc"))
  (throws? (validate-format :digit "abc"))
  (throws? (validate-format :alpha "abc9"))

  (is= "a" (validate-format :alphanumeric "a")) ; alphanumeric
  (is= "9" (validate-format :alphanumeric "9"))
  (is= "abc9" (validate-format :alphanumeric "abc9"))
  (throws? (validate-format :alphanumeric "#abc"))

  (is= "9" (validate-format :digit "9")) ; numeric only
  (is= "123" (validate-format :digit "123"))
  (throws? (validate-format :digit "abc"))
  (throws? (validate-format :digit "abc9"))

  (is= "a" (validate-format :text "a")) ; any ASCII character
  (is= "9" (validate-format :text "9"))
  (is= "abc9" (validate-format :text "abc9"))
  (is= "#abc" (validate-format :text "#abc"))
  (is= "#abc!" (validate-format :text "#abc!"))
  (is= "#ab c!" (validate-format :text "#ab c!")))

(verify
  (throws? (spec-slice {:name :xxx :format :alpha :length 0} (vec "abcdefg"))) ; zero length
  (throws-not? (spec-slice {:name :xxx :format :alpha :length 3} (vec "ab"))) ; insufficient chars

  (is= (spec-slice {:name :xxx :format :alpha :length 3} (vec "abc")) ; input str matches expected length
    {:state  {:chars-remaining []}
     :output {:xxx "abc"}})
  (is= (spec-slice {:name :xxx :format :alpha :length 3} (vec "abcdefg")) ;input str too long => truncated
    {:state  {:chars-remaining (vec "defg")}
     :output {:xxx "abc"}}))

(verify   ; document normal and error cases
  (let [field-specs            [{:name :a :format :alpha :length 1}
                                {:name :bb :format :digit :length 2}
                                {:name :ccc :format :alphanumeric :length 3 }]
        field-specs-novalidate [{:name :a :format :alpha :length 1}
                                {:name :bb :format :digit :length 2}
                                {:name           :ccc :format :alphanumeric :length 3
                                 :validate? false}]
        ]
    (is= (parse-string-fields field-specs "a23cc3")
      {:a "a" :bb "23" :ccc "cc3"})
    (is= (parse-string-fields field-specs "a23cc3dddd") ; extra chars ignored
      {:a "a" :bb "23" :ccc "cc3"})
    (throws? (parse-string-fields field-specs "abbccc")) ; bb wrong format
    (throws-not? (parse-string-fields field-specs "a23cc")) ; insufficient chars ok in last field

    (throws? (parse-string-fields field-specs-novalidate "abbccc")) ; bb wrong format but no validation
    ))

(verify
  (let [rec-1    "30000062649905                6213360078000001312022021D71704114C0311202119527117801124202100000002296800A00PAID          "
        rec-2    "30000062649906                6213360078000001412022021D11704114C0701202119527117801124202100000000000000A00DENIED        "
        parsed-1 (parse-string-fields specs/iowa-encounter-response rec-1)
        parsed-2 (parse-string-fields specs/iowa-encounter-response rec-2)]
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
       :error-field-value               ""})))

(verify
  (let [hdr          "HDDRMAMMIS49502023112320231123    4950PROD"
        line-1       "HT00300693301                               100870530                     0000000000                         99999IO"
        line-2       "HT00300693301           300693301           100870530                     30000445278160                0007D1468 IO            20231122332332570063657000"
        line-3       "HT00300693301           300693301           100870530                     30000445278160                0067D20154RL                    332332570063657000"
        parsed-hdr   (parse-string-fields specs/utah-encounter-response-hdr hdr)
        parsed-1     (parse-string-fields specs/utah-encounter-response-rec99999 line-1)
        parsed-1-cmn (parse-string-fields specs/utah-encounter-response-common line-1)
        parsed-2     (parse-string-fields specs/utah-encounter-response-rec00 line-2)
        parsed-3     (parse-string-fields specs/utah-encounter-response-rec00 line-3)
        ]
    (is (utah-9999-line? line-1))
    (isnt (utah-9999-line? line-2))
    (isnt (utah-9999-line? line-3))

    (is= parsed-hdr
      {:category     "PROD"
       :field-01     "HDDR"
       :field-02     "MAMM"
       :field-03     "IS"
       :field-04     ""
       :file-date-01 "20231123"
       :file-date-02 "20231123"
       :file-type    "4950"
       :file-type-02 "4950"})
    (is= parsed-1
      {:capitated-plan-id     "300693301"
       :error-number          "99999"
       :error-severity        "IO"
       :filler-01             ""
       :filler-02             ""
       :filler-03             ""
       :filler-04             ""
       :filler-05             ""
       :filler-06             ""
       :filler-07             ""
       :rejected-record-count "0000000000"
       :submission-number     "100870530"
       :submitter-id          "HT00"})

    (is (->> parsed-1-cmn
          (submatch?
            {:encounter-line-number      ""
             :encounter-reference-number "0000000000"
             :error-field                ""
             :error-number               "99999"
             :record-type                ""
             :related-plan-id            ""
             :submission-number          "100870530"
             :tcn                        ""})))

    (is= parsed-2
      {:capitated-plan-id          "300693301"
       :encounter-line-number      "000"
       :encounter-reference-number "30000445278160"
       :error-field                "20231122"
       :error-number               "1468"
       :error-severity             "IO"
       :record-category            "D"
       :record-type                "7"
       :related-plan-id            "300693301"
       :submission-number          "100870530"
       :submitter-id               "HT00"
       :tcn                        "332332570063657000"})
    (is= parsed-3
      {:capitated-plan-id          "300693301"
       :encounter-line-number      "006"
       :encounter-reference-number "30000445278160"
       :error-field                ""
       :error-number               "20154"
       :error-severity             "RL"
       :record-category            "D"
       :record-type                "7"
       :related-plan-id            "300693301"
       :submission-number          "100870530"
       :submitter-id               "HT00"
       :tcn                        "332332570063657000"})
    ))

(verify
  (let [sample-fname "resources/sample-utah-4950.txt"
        result       (utah-enc-response-fname->parsed sample-fname)
        result-4     (xtake 4 result)]
    (is= result-4
      [{:capitated-plan-id          "300693301"
        :encounter-line-number      "000"
        :encounter-reference-number "30000445230835"
        :error-field                "00000000000000000000"
        :error-number               "00000"
        :error-severity             "00"
        :record-category            "D"
        :record-type                "1"
        :related-plan-id            "300693301"
        :submission-number          "100870502"
        :submitter-id               "HT00"
        :tcn                        "332332410004013000"}
       {:capitated-plan-id          "300693301"
        :encounter-line-number      "000"
        :encounter-reference-number "30000445278160"
        :error-field                "20231122"
        :error-number               "1468"
        :error-severity             "IO"
        :record-category            "D"
        :record-type                "7"
        :related-plan-id            "300693301"
        :submission-number          "100870530"
        :submitter-id               "HT00"
        :tcn                        "332332570063657000"}
       {:capitated-plan-id          "300693301"
        :encounter-line-number      "006"
        :encounter-reference-number "30000445278160"
        :error-field                ""
        :error-number               "20154"
        :error-severity             "RL"
        :record-category            "D"
        :record-type                "7"
        :related-plan-id            "300693301"
        :submission-number          "100870530"
        :submitter-id               "HT00"
        :tcn                        "332332570063657000"}
       {:capitated-plan-id          "300693301"
        :encounter-line-number      "000"
        :encounter-reference-number "30000445278201"
        :error-field                "20231122"
        :error-number               "1468"
        :error-severity             "IO"
        :record-category            "D"
        :record-type                "7"
        :related-plan-id            "300693301"
        :submission-number          "100870530"
        :submitter-id               "HT00"
        :tcn                        "332332570063722000"}])

    (is= "warning"
      (error-severity->outstr "io")
      (error-severity->outstr "IO"))
    (is= "error"
      (error-severity->outstr "re")
      (error-severity->outstr "rl")
      (error-severity->outstr "rb")
      (error-severity->outstr "RE")
      (error-severity->outstr "RL")
      (error-severity->outstr "RB"))
    (is= "unknown"
      (error-severity->outstr "")
      (error-severity->outstr "garbage"))

    (is= (xtake 4 (utah-enc-response-fname->by-enc-ref sample-fname))
      {"30000445230835" [{:capitated-plan-id          "300693301"
                          :encounter-line-number      "000"
                          :encounter-reference-number "30000445230835"
                          :error-field                "00000000000000000000"
                          :error-number               "00000"
                          :error-severity             "00"
                          :record-category            "D"
                          :record-type                "1"
                          :related-plan-id            "300693301"
                          :submission-number          "100870502"
                          :submitter-id               "HT00"
                          :tcn                        "332332410004013000"}]
       "30000445278160" [{:capitated-plan-id          "300693301"
                          :encounter-line-number      "000"
                          :encounter-reference-number "30000445278160"
                          :error-field                "20231122"
                          :error-number               "1468"
                          :error-severity             "IO"
                          :record-category            "D"
                          :record-type                "7"
                          :related-plan-id            "300693301"
                          :submission-number          "100870530"
                          :submitter-id               "HT00"
                          :tcn                        "332332570063657000"}
                         {:capitated-plan-id          "300693301"
                          :encounter-line-number      "006"
                          :encounter-reference-number "30000445278160"
                          :error-field                ""
                          :error-number               "20154"
                          :error-severity             "RL"
                          :record-category            "D"
                          :record-type                "7"
                          :related-plan-id            "300693301"
                          :submission-number          "100870530"
                          :submitter-id               "HT00"
                          :tcn                        "332332570063657000"}]
       "30000445278201" [{:capitated-plan-id          "300693301"
                          :encounter-line-number      "000"
                          :encounter-reference-number "30000445278201"
                          :error-field                "20231122"
                          :error-number               "1468"
                          :error-severity             "IO"
                          :record-category            "D"
                          :record-type                "7"
                          :related-plan-id            "300693301"
                          :submission-number          "100870530"
                          :submitter-id               "HT00"
                          :tcn                        "332332570063722000"}]
       "30000445284956" [{:capitated-plan-id          "300693301"
                          :encounter-line-number      "000"
                          :encounter-reference-number "30000445284956"
                          :error-field                "00000000000000000000"
                          :error-number               "00000"
                          :error-severity             "00"
                          :record-category            "D"
                          :record-type                "1"
                          :related-plan-id            "300693301"
                          :submission-number          "100870530"
                          :submitter-id               "HT00"
                          :tcn                        "332332610002571000"}]})

    (is= (take 5 utah-rejected-icns)
      ["30000445325964"
       "30000445326204"
       "30000445326234"
       "30000445326470"
       "30000445326530"])
    (let [dummy-File          (File. "./tsv-out-test.txt") ; (tio/create-temp-file "tsv" ".tmp")
          dummy-rejected-icns #{"30000445278160" ; # keep even icns for unit tests
                                "30000445284956"
                                "30000445325958"
                                "30000445325960"
                                "30000445325962"
                                "30000445325964"}]
      (with-redefs [utah-rejected-icns dummy-rejected-icns]
        (is= (utah-enc-response-fname->tsv-recs sample-fname)
          [{"30000445278160" [{"code" "1468", "severity" "warning"}
                              {"code" "20154", "severity" "error"}]}
           {"30000445284956" [{"code" "00000", "severity" "unknown"}]}
           {"30000445325958" [{"code" "00000", "severity" "unknown"}]}
           {"30000445325960" [{"code" "00000", "severity" "unknown"}]}
           {"30000445325962" [{"code" "00000", "severity" "unknown"}]}
           {"30000445325964" [{"code" "2076", "severity" "error"}
                              {"code" "2645", "severity" "warning"}
                              {"code" "20121", "severity" "error"}
                              {"code" "20121", "severity" "error"}
                              {"code" "20121", "severity" "error"}]}])

        (utah-enc-response-fname->tsv-file sample-fname dummy-File)
        (let [result-str (str/quotes->single (slurp dummy-File))]
          (is-nonblank= result-str
            "30000445278160	[{'code':'1468','severity':'warning'},{'code':'20154','severity':'error'}]
             30000445284956	[{'code':'00000','severity':'unknown'}]
             30000445325958	[{'code':'00000','severity':'unknown'}]
             30000445325960	[{'code':'00000','severity':'unknown'}]
             30000445325962	[{'code':'00000','severity':'unknown'}]
             30000445325964	[{'code':'2076','severity':'error'},{'code':'2645','severity':'warning'},{'code':'20121','severity':'error'},{'code':'20121','severity':'error'},{'code':'20121','severity':'error'}] ")))

      ; Write full result
      (when false ; only run manually for full output
        (let [fname-enc-responses-all "/Users/athom555/work/utah-response/4950_DOHHT007992-001_15007163_20231123.txt"
              full-output-file        "utah-out-full.tsv"]
          (utah-enc-response-fname->tsv-file fname-enc-responses-all full-output-file)
          (let [result-str (str/quotes->single (slurp full-output-file))]
            (println result-str)
            )))
      )
    ))

(verify
  ; works only on filename w/o parent dirs
  (isnt (iowa-enc-resp-file-name? "a/b/ENC_RESPONSE_D_20200312_062014.TXT"))
  (is (iowa-enc-resp-file-name? "ENC_RESPONSE_D_20200312_062014.TXT"))

  ; ignores parent dirs in path
  (is (iowa-enc-resp-File? (File. "/Users/athom555/work/iowa-response/ENC_RESPONSE_D_20200312_062014.TXT")))

  ; OK if not exist as long as pattern matches
  (is (iowa-enc-resp-File? (File. "/Users/athom555/work/iowa-response/ENC_RESPONSE_D_xxxxxxx_062014.TXT")))
  (isnt (iowa-enc-resp-File? (File. "/Users/athom555/work/iowa-response/xxxxENC_RESPONSE_D_xxxxxxx_062014.TXT"))))

;---------------------------------------------------------------------------------------------------
(verify
  (let [encounter-response-root-dir "./enc-response-files-test"
        enc-resp-root-dir-File      (io/file encounter-response-root-dir)
        all-files                   (file-seq enc-resp-root-dir-File) ; returns a tree of File objects like `find`
        enc-resp-fnames             (vec (sort (mapv str (keep-if iowa-enc-resp-File? all-files))))]
    (is (it-> all-files
          (mapv type it)
          (every? #(= % File) it))) ; every element is a java.io.File
    ; (spyx-pretty all-files)
    ; all-files =>   *** sample result ***
    ; [#object[java.io.File 0x4f39a53 "./enc-response-files-test"]
    ;  #object[java.io.File 0x436528c3 "./enc-response-files-test/ENC_RESPONSE_D_20220106_062929.TXT"]
    ;  #object[java.io.File 0x33e76e5 "./enc-response-files-test/ENC_RESPONSE_D_20211211_061725.TXT"]
    ;  #object[java.io.File 0x27f6d10a "./enc-response-files-test/ENC_RESPONSE_D_20211216_070153.TXT"]
    ;  #object[java.io.File 0x527e2e3e "./enc-response-files-test/ENC_RESPONSE_D_20211202_065818.TXT"]]

    (is= enc-resp-fnames
      ["./enc-response-files-test/ENC_RESPONSE_D_20211202_065818.TXT"
       "./enc-response-files-test/ENC_RESPONSE_D_20211211_061725.TXT"
       "./enc-response-files-test/ENC_RESPONSE_D_20211216_070153.TXT"
       "./enc-response-files-test/ENC_RESPONSE_D_20220106_062929.TXT"])))

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
   :encounter-transmission/billing-provider-npi     "1831475300"})

(verify
  (with-redefs [verbose? verbose-tests?]
    (let [result (proc/load-missing-icns ctx-local)]
      (is (->> result
            (submatch? [{:encounter-transmission/icn  "30000000100601"
                         :encounter-transmission/plan "id-medicaid"
                         :encounter-transmission/status
                         #:db{:ident :encounter-transmission.status/rejected-by-validation}}
                        {:encounter-transmission/icn  "30000000102936"
                         :encounter-transmission/plan "id-medicaid"
                         :encounter-transmission/status
                         #:db{:ident :encounter-transmission.status/accepted}}
                        {:encounter-transmission/icn  "30000000102990"
                         :encounter-transmission/plan "id-medicaid"
                         :encounter-transmission/status
                         #:db{:ident :encounter-transmission.status/accepted}}
                        {:encounter-transmission/icn  "30000000217708"
                         :encounter-transmission/plan "ut-medicaid"
                         :encounter-transmission/status
                         #:db{:ident :encounter-transmission.status/rejected}}
                        {:encounter-transmission/icn  "30000000222291"
                         :encounter-transmission/plan "tx-medicaid"
                         :encounter-transmission/status
                         #:db{:ident :encounter-transmission.status/accepted}}]))))))

(verify
  (let [fname "./enc-response-files-test-small/ENC_RESPONSE_D_20211202_065818.TXT"]
    (is= (iowa-enc-response-fname->utc-str fname)
      "2021-12-02T06:58:18")
    (is= (iowa-enc-response-fname->base-str fname)
      "ENC_RESPONSE_D_20211202_065818.TXT")))

(verify
  (let [ctx {:db-uri                      "datomic:dev://localhost:4334/enc-response-test"

             :encounter-response-root-dir "./enc-response-files-test-small" ; full data:  "/Users/athom555/work/iowa-response"
             :missing-icn-fname           "resources/missing-icns-prod-small.edn"
             :icn-maps-aug-fname          "icn-maps-aug.edn"
             :tx-data-fname               "tx-data.edn"
             }]

    ; full data: "/Users/athom555/work/iowa-response"
    (let [enc-resp-fnames (iowa-enc-response-dir->fnames ctx) ; uses :encounter-response-root-dir
          fname-first     (xfirst enc-resp-fnames)]
      ; verify found all files in dir
      (is= enc-resp-fnames
        ["./enc-response-files-test-small/ENC_RESPONSE_D_20211202_065818.TXT"
         "./enc-response-files-test-small/ENC_RESPONSE_D_20211211_061725.TXT"
         "./enc-response-files-test-small/ENC_RESPONSE_D_20211216_070153.TXT"
         "./enc-response-files-test-small/ENC_RESPONSE_D_20220106_062929.TXT"])
      ; verify read all lines from first file
      (let [lines           (enc-response-fname->lines fname-first)
            lines-collapsed (mapv str/whitespace-collapse lines)]
        (is= lines-collapsed
          ["30000000100601 6213360078000000112022021D12610850C0630202119527117800820202100000000476300A00PAID"
           "30000000102936 6213360078000000212022021D13183010G1025202119527117801124202100000002492400A00PAID"
           "30000062649895 6213360078000000312022021D12906224H1025202119527117801124202100000002492400A00PAID"
           "30000062649896 6213360078000000412022021D11574993J1025202119527117801124202100000000800000A00PAID"
           "30000062649897 6213360078000000512022021D14037045B1027202119527117801124202100000003457400A00PAID"]))
      ; verify parsed all lines => records from first file
      (let [data-recs (iowa-enc-response-fname->parsed fname-first)
            rec-1     (xfirst data-recs)
            rec-5     (xlast data-recs)]
        (is= 5 (count data-recs))

        (is= rec-1 {:billing-provider-npi            "1952711780"
                    :claim-frequency-code            "1"
                    :claim-type                      "D"
                    :error-code                      "A00"
                    :error-field-value               ""
                    :field                           "PAID"
                    :first-date-of-service           "06302021"
                    :iowa-processing-date            "12022021"
                    :iowa-transaction-control-number "62133600780000001"
                    :line-number                     "00"
                    :mco-claim-number                "30000000100601"
                    :mco-paid-date                   "08202021"
                    :member-id                       "2610850C"
                    :total-paid-amount               "000000004763"
                    :fname-str                       "ENC_RESPONSE_D_20211202_065818.TXT"})
        (is= rec-5 {:billing-provider-npi            "1952711780"
                    :claim-frequency-code            "1"
                    :claim-type                      "D"
                    :error-code                      "A00"
                    :error-field-value               ""
                    :field                           "PAID"
                    :first-date-of-service           "10272021"
                    :iowa-processing-date            "12022021"
                    :iowa-transaction-control-number "62133600780000005"
                    :line-number                     "00"
                    :mco-claim-number                "30000062649897"
                    :mco-paid-date                   "11242021"
                    :member-id                       "4037045B"
                    :total-paid-amount               "000000034574"
                    :fname-str                       "ENC_RESPONSE_D_20211202_065818.TXT"})

        ; verify parsed all 5 records from file first & last match expected values
        (is (->> data-recs
              (wild-match?
                [rec-1
                 :*
                 :*
                 :*
                 rec-5])))))))
