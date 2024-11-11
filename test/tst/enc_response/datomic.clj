(ns tst.enc-response.datomic
  (:use enc-response.datomic
        tupelo.core
        tupelo.test)
  (:require
    [clojure.data :as data]
    [clojure.java.io :as io]
    [clojure.pprint :as pp]
    [datomic.api :as d.peer]
    [enc-response.parse :as parse]
    [enc-response.proc :as proc]
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
            (cond-it-> (validate boolean? (d.peer/delete-database db-uri-disk-test)) ; returns true/false
              verbose-tests? (println "  Deleted prior db: " it))
            (cond-it-> (validate boolean? (d.peer/create-database db-uri-disk-test))
              verbose-tests? (println "  Creating db:      " it)))
   :leave (fn [ctx]
            (cond-it-> (validate boolean? (d.peer/delete-database db-uri-disk-test))
              verbose-tests? (println "  Deleting db:      " it)))
   })

(def ctx-local
  {:encounter-response-root-dir "./enc-response-files-test-small" ; full data:  "/Users/athom555/work/iowa-response"
   :missing-icn-fname           "resources/missing-icns-prod-small.edn"
   :icn-maps-aug-fname          "icn-maps-aug.edn"
   :tx-data-chunked-fname       "tx-data-chuncked.edn"
   :tx-size-limit               2

   :db-uri                      "datomic:dev://localhost:4334/enc-response-test"})

(verify
  (let [resp1       (enc-response-schema->datomic ctx-local) ; commit schema into datomic
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
        resp3       (enc-response-recs->datomic ctx-local sample-recs) ; commit records into datomic
        ; >>          (pp/pprint resp3)
        ]
    ; Query datomic to verify can retrieve records
    (with-map-vals ctx-local [db-uri]
      (let [conn (d.peer/connect db-uri)
            db   (d.peer/db conn)]
        (let [raw-result (only2 (d.peer/q '[:find (pull ?e [*])
                                            :where [?e :mco-claim-number "30000062649905"]]
                                  db))]
          (is (submatch? rec-1 raw-result)))
        (let [raw-result (only2 (d.peer/q '[:find (pull ?e [*])
                                            :where [?e :mco-claim-number "30000062649906"]]
                                  db))]
          (is (submatch? rec-2 raw-result)))))))

; #todo move parsing tests => tst.enc-response/parse
(verify
  ; full data: "/Users/athom555/work/iowa-response"
  (let [enc-resp-fnames (proc/get-enc-response-fnames ctx-local)
        fname-first     (xfirst enc-resp-fnames)]
    ; verify found all files in dir
    (is= enc-resp-fnames
      ["./enc-response-files-test-small/ENC_RESPONSE_D_20211202_065818.TXT"
       "./enc-response-files-test-small/ENC_RESPONSE_D_20211211_061725.TXT"
       "./enc-response-files-test-small/ENC_RESPONSE_D_20211216_070153.TXT"
       "./enc-response-files-test-small/ENC_RESPONSE_D_20220106_062929.TXT"])
    ; verify read all lines from first file
    (let [lines           (parse/enc-response-fname->lines fname-first)
          lines-collapsed (mapv str/whitespace-collapse lines)]
      (is= lines-collapsed
        ["30000000100601 6213360078000000112022021D12610850C0630202119527117800820202100000000476300A00PAID"
         "30000000102936 6213360078000000212022021D13183010G1025202119527117801124202100000002492400A00PAID"
         "30000062649895 6213360078000000312022021D12906224H1025202119527117801124202100000002492400A00PAID"
         "30000062649896 6213360078000000412022021D11574993J1025202119527117801124202100000000800000A00PAID"
         "30000062649897 6213360078000000512022021D14037045B1027202119527117801124202100000003457400A00PAID"]))
    ; verify parsed all lines => records from first file
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

      ; verify parsed all 5 records from file, first & last match expected values
      (is (->> data-recs
            (wild-match?
              [rec-1
               :*
               :*
               :*
               rec-5])))
      (enc-response-schema->datomic ctx-local) ; commit schema
      (enc-response-recs->datomic ctx-local data-recs) ; commit records

      ; verify can retrieve first & last records from datomic
      (let [conn (d.peer/connect db-uri-disk-test)
            db   (d.peer/db conn)]
        (let [result (only2 (d.peer/q '[:find (pull ?e [*])
                                        :where [?e :mco-claim-number "30000000100601"]]
                              db))]
          (is (submatch? rec-1 result)))
        (let [result (only2 (d.peer/q '[:find (pull ?e [*])
                                        :where [?e :mco-claim-number "30000062649897"]]
                              db))]
          (is (submatch? rec-5 result)))))))

(verify
  (enc-response-schema->datomic ctx-local) ; commit schema
  (proc/enc-response-files->datomic ctx-local)

  ; verify can retrieve first & last records from datomic
  (let [conn (d.peer/connect db-uri-disk-test)
        db   (d.peer/db conn)]
    (let [enc-resp-recs (onlies (d.peer/q '[:find (pull ?e [*])
                                            :where [?e :mco-claim-number]]
                                  db))
          recs-sorted   (vec (sort-by :mco-claim-number enc-resp-recs))]
      (is= (count enc-resp-recs) 14)
      (is= (xfirst recs-sorted)
        {:billing-provider-npi            "1952711780"
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
         :db/id                           17592186045418})
      (is= (xlast recs-sorted)
        {:billing-provider-npi            "1952711780"
         :claim-frequency-code            "7"
         :claim-type                      "D"
         :error-code                      "A00"
         :error-field-value               ""
         :field                           "DENIED"
         :first-date-of-service           "08062021"
         :iowa-processing-date            "01062022"
         :iowa-transaction-control-number "62200600780000002"
         :line-number                     "00"
         :mco-claim-number                "30000063295501"
         :mco-paid-date                   "12292021"
         :member-id                       "3382022I"
         :total-paid-amount               "000000000000"
         :db/id                           17592186045438}))))

(comment
  (verify
    (let [ctx {:db-uri             "datomic:dev://localhost:4334/enc-response"
               :tx-size-limit      500
               :missing-icn-fname  "resources/missing-icns-prod-small.edn"
               :icn-maps-aug-fname "icn-maps-aug.edn"}]
      (with-map-vals ctx [db-uri]
        (spyx (count-enc-response-recs ctx))
        (let [conn (d.peer/connect db-uri)
              db   (d.peer/db conn)
              rec  (enc-response-query-icn->plan-icn db "30000019034534") ; missing file => :encounter-transmission/icn
              ]
          (spyx-pretty rec)
          )))))

; search for ICNs with multiple encounter response records
#_(verify-focus
  (let [ctx {:db-uri             "datomic:dev://localhost:4334/enc-response"
             :tx-size-limit      500
             :missing-icn-fname  "resources/missing-icns-prod-small.edn"
             :icn-maps-aug-fname "icn-maps-aug.edn"}]
    (with-map-vals ctx [db-uri]
      (spyx (count-enc-response-recs ctx))
      #_(let [missing-recs (loadmis)
            conn (d.peer/connect db-uri)
            db   (d.peer/db conn)
            rec  (enc-response-query-icn->plan-icn db "30000019034534") ; missing file => :encounter-transmission/icn
            ]
        (spyx-pretty rec)
        )
      )))
