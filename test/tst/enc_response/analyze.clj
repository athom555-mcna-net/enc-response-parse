(ns tst.enc-response.analyze
  (:use enc-response.analyze
        tupelo.core
        tupelo.test)
  (:require
    [clojure.data :as data]
    [clojure.java.io :as io]
    [clojure.pprint :as pp]
    [crockery.core :as tbl]
    [datomic.api :as d.peer]
    [enc-response.proc :as proc]
    [schema.core :as s]
    [tupelo.csv :as csv]
    [tupelo.io :as tio]
    [tupelo.schema :as tsk]
    )
  (:import
    [java.io File]
    ))


(verify
  (let [r1 {:fname-str "a" :mco-claim-number "30" :iowa-transaction-control-number "3"}
        r2 {:fname-str "b" :mco-claim-number "20" :iowa-transaction-control-number "2"}
        r3 {:fname-str "c" :mco-claim-number "10" :iowa-transaction-control-number "1"}]
    (is= [r1 r2 r3]
      (sort-by keyfn-enc-resp [r1 r2 r3])
      (sort-by keyfn-enc-resp [r1 r3 r2])
      (sort-by keyfn-enc-resp [r2 r3 r1])
      (sort-by keyfn-enc-resp [r2 r1 r3])
      (sort-by keyfn-enc-resp [r3 r2 r1])
      (sort-by keyfn-enc-resp [r3 r1 r2])))
  (let [r1 {:fname-str "a" :mco-claim-number "1" :iowa-transaction-control-number "3"}
        r2 {:fname-str "a" :mco-claim-number "2" :iowa-transaction-control-number "2"}
        r3 {:fname-str "a" :mco-claim-number "3" :iowa-transaction-control-number "1"}]
    (is= [r1 r2 r3]
      (sort-by keyfn-enc-resp [r1 r2 r3])
      (sort-by keyfn-enc-resp [r1 r3 r2])
      (sort-by keyfn-enc-resp [r2 r3 r1])
      (sort-by keyfn-enc-resp [r2 r1 r3])
      (sort-by keyfn-enc-resp [r3 r2 r1])
      (sort-by keyfn-enc-resp [r3 r1 r2])))
  (let [r1 {:fname-str "a" :mco-claim-number "1" :iowa-transaction-control-number "1"}
        r2 {:fname-str "a" :mco-claim-number "1" :iowa-transaction-control-number "2"}
        r3 {:fname-str "a" :mco-claim-number "1" :iowa-transaction-control-number "3"}]
    (is= [r1 r2 r3]
      (sort-by keyfn-enc-resp [r1 r2 r3])
      (sort-by keyfn-enc-resp [r1 r3 r2])
      (sort-by keyfn-enc-resp [r2 r3 r1])
      (sort-by keyfn-enc-resp [r2 r1 r3])
      (sort-by keyfn-enc-resp [r3 r2 r1])
      (sort-by keyfn-enc-resp [r3 r1 r2]))))

#_(let [eid-first (only (first (d.peer/q '[:find ?e
                                           :where [?e :mco-claim-number ?id]]
                                 db)))
        rec-first (d.peer/pull db '[*] eid-first)
        ])

(verify
  (when false
    (let [count-recs             (count all-recs)
          count-mco-claim-number (only2 (d.peer/q '[:find (count ?e)
                                                    :where [?e :mco-claim-number]]
                                          db))
          rec-first              (xfirst all-recs)
          s1                     (xfirst all-recs-sorted)
          s2                     (xsecond all-recs-sorted)
          ]
      (is= 923307 count-recs)
      (is= 923307 count-mco-claim-number)

      (is (->> rec-first
            (wild-match?
              {:billing-provider-npi            "1952711780"
               :claim-frequency-code            "1"
               :claim-type                      "D"
               :error-code                      "A00"
               :error-field-value               ""
               :field                           "PAID"
               :first-date-of-service           "00000000"
               :fname-str                       "ENC_RESPONSE_D_20170413_132207.TXT"
               :iowa-processing-date            "04132017"
               :iowa-transaction-control-number "61710200783000001"
               :line-number                     "00"
               :mco-claim-number                "30000019034534"
               :mco-paid-date                   "00000000"
               :member-id                       "1728543G"
               :total-paid-amount               "000000005076"
               :db/id                           :*})))
      (is (->> s1
            (wild-match?
              {:billing-provider-npi            "1952711780"
               :claim-frequency-code            "1"
               :claim-type                      "D"
               :error-code                      "A00"
               :error-field-value               ""
               :field                           "PAID"
               :first-date-of-service           "00000000"
               :fname-str                       "ENC_RESPONSE_D_20170413_132207.TXT"
               :iowa-processing-date            "04132017"
               :iowa-transaction-control-number "61710200783000001"
               :line-number                     "00"
               :mco-claim-number                "30000019034534"
               :mco-paid-date                   "00000000"
               :member-id                       "1728543G"
               :total-paid-amount               "000000005076"
               :db/id                           :*})))))

  (when false
    (let [num-keys            (count (keys mco-icn->recs))
          grp-by-mco-number-3 (into {} (take 3 mco-icn->recs))]
      (is= 862918 num-keys)
      (spyx-pretty (into {} (take 5 mco-icn->count)))
      (spyx-pretty grp-by-mco-number-3)
      (spyx num-keys)
      (spyx-pretty (count mco-icn->count-1))
      (spyx-pretty (count mco-icn->count-2))
      (spyx-pretty (count mco-icn->count-3))
      (spyx-pretty (count mco-icn->count-4))
      (spyx-pretty (count mco-icn->count-5))
      (spyx-pretty (count mco-icn->count-6+))
      (spyx-pretty mco-icn->count-6+)
      )))

(s/defn print-table
  [datomic-recs :- [tsk/KeyMap]]
  (let [recs-sorted (sort-by keyfn-enc-resp datomic-recs)]
    (tbl/print-table
      [{:name :fname-str :title "FName"}
       {:name :mco-claim-number :title "MCO ICN"}
       {:name :iowa-transaction-control-number :title "Iowa ICN"}
       :billing-provider-npi
       {:name :claim-frequency-code :title "Claim Code"}
       :claim-type
       :error-code
       {:name :error-field-value :title "Error Field"}
       :field
       {:name :first-date-of-service :title "First DOS"}
       {:name :iowa-processing-date :title "Iowa DOS"}
       :line-number
       :mco-paid-date
       :member-id
       :total-paid-amount]
      recs-sorted)))

(s/defn data-dump
  [recs :- [tsk/KeyMap]]
  (doseq [rec recs]
    (let [out-rec (submap-by-keys rec [:mco-claim-number
                                       :iowa-transaction-control-number
                                       :error-code])]
      (spyx out-rec))))

(s/defn print-samples :- [tsk/KeyMap]
  [snip-vec :- [s/Int]
   keys :- [s/Any]]
  (let [icns (drop-if keyword?
               (snip* {:snip-sizes snip-vec
                       :data       keys}))]
    ; (spyx-pretty icns)
    (doseq [icn icns]
      (let [recs (grab icn mco-icn->recs)]
        (data-dump recs)
        (print-table recs)))))

(verify   ; -focus
  (when true
    ; (print-samples [3 2 2 2 2] (keys mco-icn->count-2))
    ; (print-samples [3 2 2 2 2] (keys mco-icn->count-3))
    (print-samples [3 2 2 2 2] (keys mco-icn->count-4))
    ; (print-samples [3 2 2 2 2] (keys mco-icn->count-5))
    ; (print-samples [3 2 2 2 2] (keys mco-icn->count-6+))
    ))

(s/defn claim-accepted? :- s/Bool
  [claim-rec :- tsk/KeyMap]
  (= "A00" (:error-code claim-rec)))

(s/defn valid-claim-rec? :- s/Bool
  [claim-rec :- tsk/KeyMap]
  (with-exception-default false
    (with-map-vals claim-rec [:mco-claim-number :iowa-transaction-control-number :error-code]
      (and
        (string? mco-claim-number) (< 5 (count mco-claim-number))
        (string? iowa-transaction-control-number) (< 5 (count iowa-transaction-control-number))
        (string? error-code)))))

(s/defn extract-key-rec :- (s/maybe tsk/KeyMap)
  "If any claim record is 'accepted', return the last accepted record.
   If no claims are accepted, return the last record."
  [recs :- [tsk/KeyMap]]
  (let [recs-valid    (keep-if valid-claim-rec? recs)
        recs-accepted (keep-if claim-accepted? recs-valid)]
    (when (pos? (count recs-valid))
      (if (pos? (count recs-accepted))
        (xlast recs-accepted)
        (xlast recs-valid)))))

(verify-focus
  (is (claim-accepted? {:error-code "A00"}))
  (isnt (claim-accepted? {:error-code "D09"}))
  (isnt (claim-accepted? {}))

  (is= [{:mco-claim-number "123456" :iowa-transaction-control-number "234567" :error-code "dummy"}]
    (keep-if valid-claim-rec?
      [{:mco-claim-number "123456" :iowa-transaction-control-number "234567" :error-code "dummy"}
       {:mco-claim-number "12345" :iowa-transaction-control-number "234567" :error-code "A0"}
       {:mco-claim-number nil :iowa-transaction-control-number "234567" :error-code "21"}
       {:mco-claim-number "456" :iowa-transaction-control-number "234567" :error-code "D9"}
       {:mco-claim-number 1234567 :iowa-transaction-control-number "234567" :error-code "blah"}
       {:iowa-transaction-control-number "234567" :error-code "A0"}
       {:mco-claim-number "9123456" :error-code "21"}
       {:mco-claim-number "9123456" :iowa-transaction-control-number "999234567"}])))

(is= {:mco-claim-number "000005" :iowa-transaction-control-number "000005" :error-code "A00"}
  (extract-key-rec [{:mco-claim-number "000001" :iowa-transaction-control-number "000001" :error-code "A00"}
                    {:mco-claim-number "000002" :iowa-transaction-control-number "000002" :error-code "A00"}
                    {:mco-claim-number "000003" :iowa-transaction-control-number "000003" :error-code "D09"}
                    {:mco-claim-number "000005" :iowa-transaction-control-number "000005" :error-code "A00"}]))
(is= {:mco-claim-number "000002" :iowa-transaction-control-number "000002" :error-code "A00"}
  (extract-key-rec [{:mco-claim-number "000001" :iowa-transaction-control-number "000001" :error-code "A00"}
                    {:mco-claim-number "000002" :iowa-transaction-control-number "000002" :error-code "A00"}
                    {:mco-claim-number "000003" :iowa-transaction-control-number "000003" :error-code "D09"}]))
(is= {:mco-claim-number "000001" :iowa-transaction-control-number "000001" :error-code "A00"}
  (extract-key-rec [{:mco-claim-number "000001" :iowa-transaction-control-number "000001" :error-code "A00"}
                    {:mco-claim-number "000002" :iowa-transaction-control-number "000002" :error-code "xxx"}
                    {:mco-claim-number "000003" :iowa-transaction-control-number "000003" :error-code "D09"}
                    {:mco-claim-number "000005" :iowa-transaction-control-number "000005" :error-code "blah"}]))
(is= {:mco-claim-number "000005" :iowa-transaction-control-number "000005" :error-code "blah"}
  (extract-key-rec [{:mco-claim-number "000002" :iowa-transaction-control-number "000002" :error-code "xxx"}
                    {:mco-claim-number "000003" :iowa-transaction-control-number "000003" :error-code "D09"}
                    {:mco-claim-number "000005" :iowa-transaction-control-number "000005" :error-code "blah"}]))

(s/defn transform-claim-recs->tsv-recs
  [icn->claims :- {s/Str [tsk/KeyMap]}] ; Map:  { <mco icn str>  <vec of claim recs> }
  (let [key-recs (drop-if nil?
                   (forv [[icn-str claim-recs] icn->claims]
                     (extract-key-rec claim-recs)))
        out-recs (forv [rec key-recs]
                   (with-map-vals rec [:mco-claim-number :iowa-transaction-control-number :error-code]
                     {:icn      mco-claim-number
                      :plan_icn iowa-transaction-control-number
                      :status   (proc/error-code->status-str error-code)}))]
    out-recs))

(s/defn write-claim-recs->tsv-File
  [out-File :- java.io.File
   icn->claims :- {s/Str [tsk/KeyMap]}] ; Map:  { <mco icn str>  <vec of claim recs> }
  (let [tsv-recs (transform-claim-recs->tsv-recs icn->claims)
        csv-str (csv/entities->csv tsv-recs {:separator \tab})]
    (spit out-File csv-str)
    nil))

(verify-focus
  (let [dummy-File        (tio/create-temp-file "tsv" ".tmp")
        icn->claims       {"100000" [{:mco-claim-number "100000" :iowa-transaction-control-number "200000" :error-code "A00"}]
                           "100001" [{:mco-claim-number "100001" :iowa-transaction-control-number "200001" :error-code "A00"}]
                           "123"    [{:mco-claim-number "123" :iowa-transaction-control-number "200001" :error-code "A00"}]}
        tsv-recs-expected [{:icn "100000", :plan_icn "200000", :status "accepted"}
                           {:icn "100001", :plan_icn "200001", :status "accepted"}]
        ]
    (is= java.io.File (type dummy-File))
    (tio/delete-file-if-exists dummy-File)

    (is= (transform-claim-recs->tsv-recs icn->claims) tsv-recs-expected)
    (let [csv-1 (csv/entities->csv tsv-recs-expected {:separator \tab})]
      (is-nonblank= csv-1
        " icn	    plan_icn	status
          100000	200000	  accepted
          100001	200001    accepted"))

    (write-claim-recs->tsv-File dummy-File icn->claims)
    (let [result (slurp dummy-File)]
      (when false
        (prn :-----------------------------------------------------------------------------)
        (println result)
        (prn :-----------------------------------------------------------------------------))
      (is-nonblank-lines= result
        " icn	    plan_icn	status
          100000	200000	  accepted
          100001	200001    accepted "))
    )
  )

