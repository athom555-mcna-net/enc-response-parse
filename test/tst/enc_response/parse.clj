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
   :max-tx-size               3
   })

;-----------------------------------------------------------------------------
(verify
  (throws? (validate-format :charxxx "abc")) ; format kw must be valid

  (throws-not? (validate-format :char "")) ; empty str legal
  (throws-not? (validate-format :numeric ""))
  (throws-not? (validate-format :alphanumeric ""))
  (throws? (validate-format :char (vec "abc"))) ; must be str not charseq

  (is= "a" (validate-format :char "a")) ; :char => alpha only
  (is= "abc" (validate-format :char "abc"))
  (throws? (validate-format :numeric "abc"))
  (throws? (validate-format :char "abc9"))

  (is= "a" (validate-format :alphanumeric "a")) ; alphanumeric
  (is= "9" (validate-format :alphanumeric "9"))
  (is= "abc9" (validate-format :alphanumeric "abc9"))
  (throws? (validate-format :alphanumeric "#abc"))

  (is= "9" (validate-format :numeric "9")) ; numeric only
  (is= "123" (validate-format :numeric "123"))
  (throws? (validate-format :numeric "abc"))
  (throws? (validate-format :numeric "abc9"))

  (is= "a" (validate-format :text "a")) ; any ASCII character
  (is= "9" (validate-format :text "9"))
  (is= "abc9" (validate-format :text "abc9"))
  (is= "#abc" (validate-format :text "#abc"))
  (is= "#abc!" (validate-format :text "#abc!"))
  (is= "#ab c!" (validate-format :text "#ab c!")))

(verify
  (throws? (spec-slice {:name :xxx :format :char :length 0} (vec "abcdefg"))) ; zero length
  (throws? (spec-slice {:name :xxx :format :char :length 3} (vec "ab"))) ; insufficient chars
  (throws? (spec-slice {:name :xxx :format :char :length 3 :length-strict? true} (vec "ab"))) ; insufficient chars
  (throws-not? (spec-slice {:name :xxx :format :char :length 3 :length-strict? false} (vec "ab"))) ; insufficient chars

  (is= (spec-slice {:name :xxx :format :char :length 3} (vec "abc")) ; input str matches expected length
    {:state  {:chars-remaining []}
     :output {:xxx "abc"}})
  (is= (spec-slice {:name :xxx :format :char :length 3} (vec "abcdefg")) ;input str too long => truncated
    {:state  {:chars-remaining (vec "defg")}
     :output {:xxx "abc"}}))

(verify   ; document behavior for normal and failure cases
  (is= 123 (parse/parse-int "123"))
  (is= -1 (with-exception-default -1
            (parse/parse-int "12x")))
  (is= 123 (parse/parse-int "123" :default 0))
  (is= 0 (parse/parse-int "12x" :default 0)))

(verify   ; document normal and error cases
  (let [field-specs [{:name :a :format :char :length 1}
                     {:name :bb :format :numeric :length 2}
                     {:name :ccc :format :alphanumeric :length 3 :length-strict? false}]]
    (is= (parse-string-fields field-specs "a23cc3")
      {:a "a" :bb "23" :ccc "cc3"})
    (is= (parse-string-fields field-specs "a23cc3dddd") ; extra chars ignored
      {:a "a" :bb "23" :ccc "cc3"})
    (throws? (parse-string-fields field-specs "abbccc")) ; bb wrong format
    (throws-not? (parse-string-fields field-specs "a23cc")) ; insufficient chars ok in last field
    ))

(verify
  (let [rec-1    "30000062649905                6213360078000001312022021D71704114C0311202119527117801124202100000002296800A00PAID          "
        rec-2    "30000062649906                6213360078000001412022021D11704114C0701202119527117801124202100000000000000A00DENIED        "
        parsed-1 (parse-string-fields iowa-encounter-response-specs rec-1)
        parsed-2 (parse-string-fields iowa-encounter-response-specs rec-2)]
    ; (spyx-pretty parsed-1)
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

    ; (spyx-pretty parsed-2)
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
  ; works only on filename w/o parent dirs
  (isnt (enc-resp-file-name? "a/b/ENC_RESPONSE_D_20200312_062014.TXT"))
  (is (enc-resp-file-name? "ENC_RESPONSE_D_20200312_062014.TXT"))

  ; ignores parent dirs in path
  (is (enc-resp-file? (File. "/Users/athom555/work/iowa-response/ENC_RESPONSE_D_20200312_062014.TXT")))

  ; OK if not exist as long as pattern matches
  (is (enc-resp-file? (File. "/Users/athom555/work/iowa-response/ENC_RESPONSE_D_xxxxxxx_062014.TXT")))
  (isnt (enc-resp-file? (File. "/Users/athom555/work/iowa-response/xxxxENC_RESPONSE_D_xxxxxxx_062014.TXT"))))

;---------------------------------------------------------------------------------------------------
(verify
  (let [encounter-response-root-dir "./enc-response-files-test"
        enc-resp-root-dir-File      (io/file encounter-response-root-dir)
        all-files                   (file-seq enc-resp-root-dir-File) ; returns a tree of File objects like `find`
        enc-resp-fnames             (vec (sort (mapv str (keep-if enc-resp-file? all-files))))]
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
  (let [ctx {:db-uri                      "datomic:dev://localhost:4334/enc-response-test"

             :encounter-response-root-dir "./enc-response-files-test-small" ; full data:  "/Users/athom555/work/iowa-response"
             :missing-icn-fname           "resources/missing-icns-prod-small.edn"
             :icn-maps-aug-fname          "icn-maps-aug.edn"
             :tx-data-fname               "tx-data.edn"
             }]

    ; full data: "/Users/athom555/work/iowa-response"
    (let [enc-resp-fnames (proc/enc-response-dir->fnames ctx) ; uses :encounter-response-root-dir
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
      (let [data-recs (enc-response-fname->parsed fname-first)
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
                    :total-paid-amount               "000000004763"})
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
                    :total-paid-amount               "000000034574"})

        ; verify parsed all 5 records from file first & last match expected values
        (is (->> data-recs
              (wild-match?
                [rec-1
                 :*
                 :*
                 :*
                 rec-5])))))))
