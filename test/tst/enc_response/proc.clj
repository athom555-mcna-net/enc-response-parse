(ns tst.enc-response.proc
  (:use enc-response.proc
        tupelo.core
        tupelo.test)
  (:require
    [clojure.data :as data]
    [clojure.java.io :as io]
    [clojure.pprint :as pp]
    [datomic.api :as d.peer]
    [enc-response.datomic :as datomic]
    [schema.core :as s]
    [tupelo.csv :as csv]
    [tupelo.io :as tio]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    [tupelo.test.jvm :as ttj]
    )
  (:import
    [java.io File]
    ))

; Enable to see progress printouts
(def ^:dynamic verbose-tests?
  false)

; Defines URI for local transactor in `dev` mode. Uses `data-dir` in transactor *.properties file.
; Default entry `data-dir=data` => /opt/datomic/data/...
; Absolute path entry like `data-dir=/Users/myuser/datomic-data` => that directory.
(def db-uri "datomic:dev://localhost:4334/enc-response-test")

(comment
  (verify
    (let [ctx {:db-uri             "datomic:dev://localhost:4334/enc-response"
               :max-tx-size        500
               :missing-icn-fname  "resources/missing-icns-prod-small.edn"
               :icn-maps-aug-fname "icn-maps-aug.edn"}]
      (with-map-vals ctx [db-uri]
        (spyx (count-enc-response-recs ctx))
        (let [db  (datomic/curr-db db-uri)
              rec (enc-response-query-icn->plan-icn db "30000019034534") ; missing file => :encounter-transmission/icn
              ]
          (spyx-pretty rec)
          )))))

; search for ICNs with multiple encounter response records
(comment
  (verify
    (let [ctx {:db-uri            "datomic:dev://localhost:4334/enc-response"
               :max-tx-size       500
               :missing-icn-fname "resources/missing-icns-prod-small.edn"}]
      (spyx (datomic/count-enc-response-recs ctx))
      (enc-resp-disp-diff ctx))))

; check can discard all but newest record
(verify
  (let [response-rec-1      {:billing-provider-npi            "1952711780"
                             :claim-frequency-code            "1"
                             :claim-type                      "D"
                             :error-code                      "A00"
                             :error-field-value               ""
                             :field                           "PAID"
                             :first-date-of-service           "00000000"
                             :iowa-processing-date            "04132017"
                             :iowa-transaction-control-number "61710200783000021"
                             :line-number                     "00"
                             :mco-claim-number                "30000019034555"
                             :mco-paid-date                   "00000000"
                             :member-id                       "1626808D"
                             :total-paid-amount               "000000017750"
                             :db/id                           17592186045438}

        response-rec-2      {:billing-provider-npi            "1952711780"
                             :claim-frequency-code            "7"
                             :claim-type                      "D"
                             :error-code                      "A00"
                             :error-field-value               ""
                             :field                           "PAID"
                             :first-date-of-service           "00000000"
                             :iowa-processing-date            "10012019"
                             :iowa-transaction-control-number "61927400780000019"
                             :line-number                     "00"
                             :mco-claim-number                "30000019034555"
                             :mco-paid-date                   "00000000"
                             :member-id                       "1626808D"
                             :total-paid-amount               "000000017750"
                             :db/id                           17592186233847}
        response-recs-multi [response-rec-1
                             response-rec-2]
        result              (resp-recs->newest response-recs-multi)]
    (is= result response-rec-2)))

; add 2 unique recs to datomic, query and verify
(verify
  (let [ctx         {:db-uri                      "datomic:dev://localhost:4334/enc-response-test"
                     :max-tx-size                 3

                     :encounter-response-root-dir "./enc-response-files-test-small" ; full data:  "/Users/athom555/work/iowa-response"
                     :missing-icn-fname           "resources/missing-icns-prod-small.edn"
                     :icn-maps-aug-fname          "icn-maps-aug.edn"
                     :tx-data-fname               "tx-data.edn"}

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
                     rec-2]]
    (datomic/enc-response-datomic-init ctx)
    (enc-response-recs->datomic ctx sample-recs) ; insert records into datomic

    ; Query datomic to verify can retrieve records
    (with-map-vals ctx [db-uri]
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

; Encounter Response recs have been parsed & saved to Datomic. Use them to augment
; entitie-maps with missing ICN values for `:plan-icn`
(verify
  (let [ctx {:db-uri             "datomic:dev://localhost:4334/enc-response" ;
             :max-tx-size        3
             :missing-icn-fname  "/Users/athom555/work/missing-icns-prod-small.edn"
             ; :missing-icn-fname  "/Users/athom555/work/missing-icns-prod-orig.edn"
             :icn-maps-aug-fname "icn-maps-aug.edn"}]

    (let [icn-maps-aug (create-icn-maps-aug->file ctx)]
      (is (->> (xlast icn-maps-aug)
            (submatch? {:encounter-transmission/icn      "30000019034555",
                        :encounter-transmission/plan     "ia-medicaid",
                        :encounter-transmission/plan-icn "61927400780000019",
                        :encounter-transmission/status
                        #:db{:ident :encounter-transmission.status/accepted}}))))
    (when false
      (nl)
      (let [r1 (icn-maps-aug->tx-data ctx)]
        (spyx-pretty r1)
        ))
    ))

; parse data from all encounter response files => datomic
(verify
  (let [ctx {:db-uri                      "datomic:dev://localhost:4334/enc-response-test"
             :max-tx-size                 3
             :encounter-response-root-dir "./enc-response-files-test-small" ; full data:  "/Users/athom555/work/iowa-response"
             }]

    (init-enc-response-files->datomic ctx)

    ; verify can retrieve first & last records from datomic
    (let [conn           (d.peer/connect db-uri)
          db             (d.peer/db conn)
          enc-resp-recs  (onlies (d.peer/q '[:find (pull ?e [*])
                                             :where [?e :mco-claim-number]]
                                   db))
          recs-sorted    (vec (sort-by :mco-claim-number enc-resp-recs))
          expected-first {:billing-provider-npi "1952711780"
                          :claim-frequency-code "1"
                          :claim-type "D"
                          :error-code "A00"
                          :error-field-value ""
                          :field "PAID"
                          :first-date-of-service "06302021"
                          :fname-str "ENC_RESPONSE_D_20211202_065818.TXT"
                          :iowa-processing-date "12022021"
                          :iowa-transaction-control-number "62133600780000001"
                          :line-number "00"
                          :mco-claim-number "30000000100601"
                          :mco-paid-date "08202021"
                          :member-id "2610850C"
                          :total-paid-amount "000000004763"}
          expected-last {:billing-provider-npi "1952711780"
                         :claim-frequency-code "7"
                         :claim-type "D"
                         :error-code "A00"
                         :error-field-value ""
                         :field "DENIED"
                         :first-date-of-service "08062021"
                         :fname-str "ENC_RESPONSE_D_20220106_062929.TXT"
                         :iowa-processing-date "01062022"
                         :iowa-transaction-control-number "62200600780000002"
                         :line-number "00"
                         :mco-claim-number "30000063295501"
                         :mco-paid-date "12292021"
                         :member-id "3382022I"
                         :total-paid-amount "000000000000"}
          result-first   (xfirst recs-sorted)
          result-last    (xlast recs-sorted)]
      (is= (count enc-resp-recs) 14)
      (is (submatch? expected-first result-first))
      (is (submatch? expected-last result-last)))))

(verify
  (let [dummy-File (tio/create-temp-file "tsv" ".tmp")]
    (is= java.io.File (type dummy-File))

    ; Create an empty file, append lines to it one at a time
    (let [; plan-icn-update-tsv-File (tio/->File plan-icn-update-tsv-fname)
          sample-text  "hello
                        there
                        again"
          sample-lines (mapv str/trim
                         (str/split-lines sample-text))]
      (is= 3 (count sample-lines))
      (is= sample-lines ["hello"
                         "there"
                         "again"])

      ; write each line to file
      (tio/delete-file-if-exists dummy-File)
      ; (.createNewFile dummy-File) ; unnecessary
      (doseq [line-str sample-lines]
        (let [line-out (str line-str \newline)] ; must add newline!!!
          (spit dummy-File line-out :append true)))
      (let [result (it-> dummy-File
                     (slurp it)
                     (str/split-lines it)
                     (mapv str/trim it))]
        (is= result ["hello"
                     "there"
                     "again"])))

    ; demo creating a csv text file from 2 blocks of data
    (let [data-1 [{:icn 101 :plan-icn 201 :status :accepted}]
          data-2 [{:icn 102 :plan-icn 202 :status :accepted}
                  {:icn 103 :plan-icn 203 :status :rejected}]

          csv-1  (csv/entities->csv data-1 {:separator \tab})
          csv-2  (csv/entities->csv data-2 {:separator \tab :header? false})]
      (is= csv-1 "icn\tplan-icn\tstatus\n101\t201\taccepted\n") ; header + 1 row
      (is= csv-2 "102\t202\taccepted\n103\t203\trejected\n") ; no header, 2 rows

      ; Use Writer to create file
      (with-open [writer (io/writer dummy-File)]
        (.write writer csv-1)
        (.write writer csv-2))
      (let [result (slurp dummy-File)]
        (is-nonblank= result
          "icn	plan-icn	status
           101	201       accepted
           102	202       accepted
           103	203       rejected    "))

      ; Use spit to create file
      (spit dummy-File csv-1)
      (spit dummy-File csv-2 :append true)
      (let [result (slurp dummy-File)]
        (is-nonblank= result
          "icn	plan-icn	status
           101	201       accepted
           102	202       accepted
           103	203       rejected    ")))

    (let [data-1 [{:mco-claim-number 101 :iowa-transaction-control-number 201 :error-code "A00"}]
          data-2 [{:mco-claim-number 102 :iowa-transaction-control-number 202 :error-code "A00"}
                  {:mco-claim-number 103 :iowa-transaction-control-number 203 :error-code "B99"}]
          ctx    {:plan-icn-update-tsv-fname dummy-File}]
      (enc-resp-parsed->tsv ctx data-1 true)
      (enc-resp-parsed->tsv ctx data-2 false)
      (let [result (slurp dummy-File)]
        (prn :-----------------------------------------------------------------------------)
        (println result)
        (prn :-----------------------------------------------------------------------------)
        (is-nonblank= result
          "icn	plan-icn	status
           101	201       accepted
           102	202       accepted
           103	203       rejected ")))
    ))

(verify
  (is= "accepted" (error-code->status-str "A00"))
  (is= "accepted" (error-code->status-str "a00"))

  (is= "rejected" (error-code->status-str "A1S"))
  (is= "rejected" (error-code->status-str "B00"))
  (is= "rejected" (error-code->status-str "xyz"))

  (let [ctx {:encounter-response-root-dir "./enc-response-files-test-small" ; full data:
             ; "/Users/athom555/work/iowa-response"
             :plan-icn-update-tsv-fname   "./plan-icn-update-small.tsv"}]
    (init-enc-response-files->updates-tsv ctx)
    (with-map-vals ctx [plan-icn-update-tsv-fname]
      (let [result (slurp plan-icn-update-tsv-fname)]
        ;(prn :-----------------------------------------------------------------------------)
        ;(println result)
        ;(prn :-----------------------------------------------------------------------------)
        (is-nonblank= result
          " icn	            plan-icn	      status
          30000000100601	62133600780000001	accepted
          30000000102936	62133600780000002	accepted
          30000062649895	62133600780000003	accepted
          30000062649896	62133600780000004	accepted
          30000062649897	62133600780000005	accepted
          30000062771575	62134500780000001	accepted
          30000062778189	62134500780000002	accepted
          30000000102990	62134500780000003	accepted
          30000062903232	62135000780000001	accepted
          30000062903233	62135000780000002	accepted
          30000062903234	62135000780000003	accepted
          30000000217708	62135000780000004	accepted
          30000063295500	62200600780000001	accepted
          30000063295501	62200600780000002	accepted "
          )))))

(verify
  (let [ctx {:invoke-fn                   enc-response.proc/init-enc-response->datomic
             :db-uri                      "datomic:dev://localhost:4334/enc-response-test-blk"

             :encounter-response-root-dir "./enc-response-files-test-small"
             :max-tx-size                 3}]
    (init-enc-response->datomic ctx)
    ))
