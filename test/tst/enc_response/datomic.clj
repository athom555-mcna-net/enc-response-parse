(ns       ;    ^:test-refresh/focus
  tst.enc-response.datomic
  (:use enc-response.datomic
        tupelo.core
        tupelo.test)
  (:require
    [clojure.data :as data]
    [clojure.java.io :as io]
    [clojure.pprint :as pp]
    [datomic.api :as d]
    [enc-response.parse :as parse]
    [tupelo.core :as t]
    [tupelo.string :as str]
    [tupelo.test.jvm :as ttj]
    ))

; Enable to see progress printouts
(def ^:dynamic verbose-tests?
  false)

; Defines URI for local transactor in `dev` mode. Uses `data-dir` in transactor *.properties file.
; Default entry `data-dir=data` => /opt/datomic/data/...
; Absolute path entry like `data-dir=/Users/myuser/datomic-data` => that directory.
(def db-uri-disk-test "datomic:dev://localhost:4334/enc-response-test")

(ttj/define-fixture :each
  {:enter (fn [ctx]
            (cond-it-> (validate boolean? (d/delete-database db-uri-disk-test)) ; returns true/false
              verbose-tests? (println "  Deleted prior db: " it))
            (cond-it-> (validate boolean? (d/create-database db-uri-disk-test))
              verbose-tests? (println "  Creating db:      " it)))
   :leave (fn [ctx]
            (cond-it-> (validate boolean? (d/delete-database db-uri-disk-test))
              verbose-tests? (println "  Deleting db:      " it)))
   })

(def ctx-local
  {:encounter-response-root-dir "./enc-response-files-test-small" ; full data:  "/Users/athom555/work/iowa-response"
   :missing-icn-fname           "resources/missing-icns-5.edn"
   :icn-maps-aug-fname          "icn-maps-aug.edn"
   :tx-data-chunked-fname       "tx-data-chuncked.edn"
   :tx-size-limit               2

   :db-uri  "datomic:dev://localhost:4334/enc-response-test"})

(verify
  (let [seq1       (range 5)
        arr1       (array-1d->2d 2 seq1)
        seq2       (array-2d->1d arr1)

        inc-1d     (fn->vec-fn inc)
        inc-2d     (fn->vec-fn inc-1d)

        seq1-inc-a (inc-1d seq1)
        arr1-inc   (inc-2d arr1)
        seq1-inc-b (array-2d->1d arr1-inc)
        ]
    (is= seq1 [0 1 2 3 4])
    (is= arr1 [[0 1]
               [2 3]
               [4]])
    (is= seq1 seq2)

    (is= seq1-inc-a [1 2 3 4 5])

    (is= arr1-inc [[1 2]
                   [3 4]
                   [5]])
    (is= seq1-inc-b [1 2 3 4 5])))

(verify
  (let [resp1       (enc-response-schema->datomic ctx-local)
        ; >>          (pp/pprint resp1)

        rec-1       {:mco-claim-number                "30000062649905"
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
                     :error-field-value               ""}
        rec-2       {:mco-claim-number                "30000062649906"
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
                     :error-field-value               ""}
        sample-recs [rec-1
                     rec-2]
        resp3       (enc-response-recs->datomic ctx-local sample-recs)
        ; >>          (pp/pprint resp3)
        ]
    (with-map-vals ctx-local [db-uri]
      (let [conn (d/connect db-uri)
            db   (d/db conn)]
        (let [raw-result (only2 (d/q '[:find (pull ?e [*])
                                       :where [?e :mco-claim-number "30000062649905"]]
                                  db))]
          (is (submatch? rec-1 raw-result)))
        (let [raw-result (only2 (d/q '[:find (pull ?e [*])
                                       :where [?e :mco-claim-number "30000062649906"]]
                                  db))]
          (is (submatch? rec-2 raw-result)))))))

; sample output
(verify
  ; full data: "/Users/athom555/work/iowa-response"
  (let [enc-resp-root-dir-File (io/file (:encounter-response-root-dir ctx-local))
        all-files              (file-seq enc-resp-root-dir-File) ; returns a tree like `find`
        enc-resp-fnames        (sort (mapv str (keep-if parse/enc-resp-file? all-files)))
        fname-first            (xfirst enc-resp-fnames)]
    (is= enc-resp-fnames
      ["./enc-response-files-test-small/ENC_RESPONSE_D_20211202_065818.TXT"
       "./enc-response-files-test-small/ENC_RESPONSE_D_20211211_061725.TXT"
       "./enc-response-files-test-small/ENC_RESPONSE_D_20211216_070153.TXT"
       "./enc-response-files-test-small/ENC_RESPONSE_D_20220106_062929.TXT"])
    (let [lines           (parse/enc-response-fname->lines fname-first)
          lines-collapsed (mapv str/whitespace-collapse lines)]
      (is= lines-collapsed
        ["30000000100601 6213360078000000112022021D12610850C0630202119527117800820202100000000476300A00PAID"
         "30000000102936 6213360078000000212022021D13183010G1025202119527117801124202100000002492400A00PAID"
         "30000062649895 6213360078000000312022021D12906224H1025202119527117801124202100000002492400A00PAID"
         "30000062649896 6213360078000000412022021D11574993J1025202119527117801124202100000000800000A00PAID"
         "30000062649897 6213360078000000512022021D14037045B1027202119527117801124202100000003457400A00PAID"]))
    (let [data-recs (parse/enc-response-fname->parsed fname-first)
          rec-1     (xfirst data-recs)
          rec-5     (xlast data-recs)]
      (is= 5 (count data-recs))

      (is= rec-1 {:billing-provider-npi            "1952711780",
                  :claim-frequency-code            "1",
                  :claim-type                      "D",
                  :error-code                      "A00",
                  :error-field-value               "",
                  :field                           "PAID",
                  :first-date-of-service           "06302021",
                  :iowa-processing-date            "12022021",
                  :iowa-transaction-control-number "62133600780000001",
                  :line-number                     "00",
                  :mco-claim-number                "30000000100601",
                  :mco-paid-date                   "08202021",
                  :member-id                       "2610850C",
                  :total-paid-amount               "000000004763"})
      (is= rec-5 {:billing-provider-npi            "1952711780",
                  :claim-frequency-code            "1",
                  :claim-type                      "D",
                  :error-code                      "A00",
                  :error-field-value               "",
                  :field                           "PAID",
                  :first-date-of-service           "10272021",
                  :iowa-processing-date            "12022021",
                  :iowa-transaction-control-number "62133600780000005",
                  :line-number                     "00",
                  :mco-claim-number                "30000062649897",
                  :mco-paid-date                   "11242021",
                  :member-id                       "4037045B",
                  :total-paid-amount               "000000034574"})

      (is (->> data-recs
            (wild-match?
              [rec-1
               :*
               :*
               :*
               rec-5])))
      (enc-response-schema->datomic ctx-local)
      (enc-response-recs->datomic ctx-local  data-recs)
      (let [conn  (d/connect db-uri-disk-test)
            db    (d/db conn)]
        (let [result (only2 (d/q '[:find (pull ?e [*])
                                   :where [?e :mco-claim-number "30000000100601"]]
                              db))]
          (is (submatch? rec-1 result)))
        (let [result (only2 (d/q '[:find (pull ?e [*])
                                   :where [?e :mco-claim-number "30000062649897"]]
                              db))]
          (is (submatch? rec-5 result))))

      )))

