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


(verify
  (let [r1 {:fname-str "a" :mco-claim-number "30" :iowa-transaction-control-number "3" }
        r2 {:fname-str "b" :mco-claim-number "20" :iowa-transaction-control-number "2" }
        r3 {:fname-str "c" :mco-claim-number "10" :iowa-transaction-control-number "1" }]
    (is= [r1 r2 r3]
      (sort-by keyfn-enc-resp [r1 r2 r3])
      (sort-by keyfn-enc-resp [r1 r3 r2])
      (sort-by keyfn-enc-resp [r2 r3 r1])
      (sort-by keyfn-enc-resp [r2 r1 r3])
      (sort-by keyfn-enc-resp [r3 r2 r1])
      (sort-by keyfn-enc-resp [r3 r1 r2])))
  (let [r1 {:fname-str "a" :mco-claim-number "1" :iowa-transaction-control-number "3" }
        r2 {:fname-str "a" :mco-claim-number "2" :iowa-transaction-control-number "2" }
        r3 {:fname-str "a" :mco-claim-number "3" :iowa-transaction-control-number "1" }]
    (is= [r1 r2 r3]
      (sort-by keyfn-enc-resp [r1 r2 r3])
      (sort-by keyfn-enc-resp [r1 r3 r2])
      (sort-by keyfn-enc-resp [r2 r3 r1])
      (sort-by keyfn-enc-resp [r2 r1 r3])
      (sort-by keyfn-enc-resp [r3 r2 r1])
      (sort-by keyfn-enc-resp [r3 r1 r2])))
  (let [r1 {:fname-str "a" :mco-claim-number "1" :iowa-transaction-control-number "1" }
        r2 {:fname-str "a" :mco-claim-number "1" :iowa-transaction-control-number "2" }
        r3 {:fname-str "a" :mco-claim-number "1" :iowa-transaction-control-number "3" }]
    (is= [r1 r2 r3]
      (sort-by keyfn-enc-resp [r1 r2 r3])
      (sort-by keyfn-enc-resp [r1 r3 r2])
      (sort-by keyfn-enc-resp [r2 r3 r1])
      (sort-by keyfn-enc-resp [r2 r1 r3])
      (sort-by keyfn-enc-resp [r3 r2 r1])
      (sort-by keyfn-enc-resp [r3 r1 r2]))))

(verify-focus

  (when true
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

      (is= rec-first
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
         :db/id                           17592186045418})
      (is= s1
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
         :db/id                           17592186045418})
      (is= s2
        {:billing-provider-npi            "1952711780"
         :claim-frequency-code            "1"
         :claim-type                      "D"
         :error-code                      "A00"
         :error-field-value               ""
         :field                           "PAID"
         :first-date-of-service           "00000000"
         :fname-str                       "ENC_RESPONSE_D_20170413_132207.TXT"
         :iowa-processing-date            "04132017"
         :iowa-transaction-control-number "61710200783000002"
         :line-number                     "00"
         :mco-claim-number                "30000019034535"
         :mco-paid-date                   "00000000"
         :member-id                       "1750178H"
         :total-paid-amount               "000000010017"
         :db/id                           17592186045419})

      ; (spyx-pretty (data/diff s1 s2) )

      (let [num-keys            (count (keys grp-by-mco-number))
            grp-by-mco-number-3 (into {} (take 3 grp-by-mco-number))]
        (is= 862918 num-keys)
        (spyx-pretty (into {} (take 5 mco-number->count)))
        (spyx-pretty grp-by-mco-number-3)
        (spyx num-keys)
        (spyx-pretty (count mco-number->count-1))
        (spyx-pretty (count mco-number->count-2))
        (spyx-pretty (count mco-number->count-3))
        (spyx-pretty (count mco-number->count-4))
        (spyx-pretty (count mco-number->count-5))
        (spyx-pretty (count mco-number->count-6+))
        (spyx-pretty mco-number->count-6+)
        (newline)
        (let [mco-1 (ffirst mco-number->count-3)
              r3a   (grab mco-1 grp-by-mco-number)]
          (print-table r3a))
        (let [mco-1 (nth (keys mco-number->count-3) 3)
              r3b   (grab mco-1 grp-by-mco-number)]
          (print-table r3b))
        (let [mco-1 (nth (keys mco-number->count-4) 1)
              r4    (sort-by :fname-str (grab mco-1 grp-by-mco-number))]
          (print-table r4))
        (let [mco-1 (nth (keys mco-number->count-5) 1)
              r5    (sort-by :fname-str (grab mco-1 grp-by-mco-number))]
          (print-table r5))

        ))
    ))

#_(let [eid-first (only (first (d.peer/q '[:find ?e
                                           :where [?e :mco-claim-number ?id]]
                                 db)))
        rec-first (d.peer/pull db '[*] eid-first)
        ])

