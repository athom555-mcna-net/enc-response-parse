(ns tst.enc-response.analyze
  (:use enc-response.analyze
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

(verify-focus
  (when true
    (let [count-recs             (count all-recs)
          count-mco-claim-number (only2 (d.peer/q '[:find (count ?e)
                                                    :where [?e :mco-claim-number]]
                                          db))
          rec-first              (xfirst all-recs)
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
         :iowa-processing-date            "04132017"
         :iowa-transaction-control-number "61710200783000001"
         :line-number                     "00"
         :mco-claim-number                "30000019034534"
         :mco-paid-date                   "00000000"
         :member-id                       "1728543G"
         :total-paid-amount               "000000005076"
         :db/id                           17592186045418})

      (is= (xfirst all-recs-sorted)
        {:billing-provider-npi            "1952711780"
         :claim-frequency-code            "1"
         :claim-type                      "D"
         :error-code                      "A00"
         :error-field-value               ""
         :field                           "PAID"
         :first-date-of-service           "00000000"
         :iowa-processing-date            "04132017"
         :iowa-transaction-control-number "61710200783000001"
         :line-number                     "00"
         :mco-claim-number                "30000019034534"
         :mco-paid-date                   "00000000"
         :member-id                       "1728543G"
         :total-paid-amount               "000000005076"
         :db/id                           17592186045418})
      (is= (xsecond all-recs-sorted)
        {:billing-provider-npi "1952711780",
         :claim-frequency-code "7",
         :claim-type "D",
         :error-code "A00",
         :error-field-value "",
         :field "PAID",
         :first-date-of-service "00000000",
         :iowa-processing-date "10012019",
         :iowa-transaction-control-number "61927400780000001",
         :line-number "00",
         :mco-claim-number "30000019034534",
         :mco-paid-date "00000000",
         :member-id "1728543G",
         :total-paid-amount "000000005076",
         :db/id 17592186233517})

      )

    ))


#_(let [eid-first (only (first (d.peer/q '[:find ?e
                                           :where [?e :mco-claim-number ?id]]
                                 db)))
        rec-first (d.peer/pull db '[*] eid-first)
        ])

