(ns enc-response-parse.scratch)

(comment

  (require
    '[clojure.pprint :as pp]
    '[clojure.walk :as walk]
    '[datomic.api :as d.peer] ; peer api
    )

  (def datomic-uri "datomic:sql://encounters")
  (def postgres-uri "jdbc:postgresql://postgres.qa:5432/topaz?user=datomic&password=geheim")
  (def db-uri (str datomic-uri \? postgres-uri))
  (def conn (d.peer/connect db-uri))
  (def db (d.peer/db conn))


  (defn iowa-prefix?
    [s] (boolean (= "ia-" (subs s 0 3))))

  (pp/pprint (vec (sort-by second
                    (d.peer/q '[:find ?eid ?db-ident
                                :where [?eid :db/ident ?db-ident]]
                      db))))

  (pp/pprint
    (first
      (d.peer/q '[:find ?eid ?icn ?plan-icn ?previous-icn
                  :where
                  [?eid :encounter-transmission/plan-icn ?plan-icn]
                  [?eid :encounter-transmission/icn ?icn]
                  [?eid :encounter-transmission/previous-icn ?previous-icn]]
        db)))

  (pp/pprint
    (first
      (d.peer/q '[:find (pull ?eid [*])
                  :where
                  [?eid :encounter-transmission/plan-icn ?plan-icn]
                  [?eid :encounter-transmission/icn ?icn]
                  [?eid :encounter-transmission/previous-icn ?previous-icn]]
        db)))
  (comment ; result (still wrapped!)
    [{:encounter-transmission/generation               1645901773
      :encounter-transmission/plan-icn                 "7022019001441"
      :encounter-transmission/frequency                #:db{:id 17592186045418}
      :encounter-transmission/access-point-medicaid-id "128325631"
      :inbound-encounter-status/status-code            "no-code-given"
      :encounter-transmission/status                   #:db{:id 17592186045429}
      :encounter-transmission/facility-ein             "710755488"
      :encounter-transmission/payer-claim-ids          ["1164275820802" "1164275820802A1"]
      :encounter-transmission/plan                     "ar-medicaid"
      :db/id                                           17592186096074
      :encounter-transmission/encounter-data           #uuid "bedb1442-d827-4a36-8bce-bd4180f35163"
      :encounter-transmission/icn                      "30000000155772"
      :inbound-encounter-status/timestamp              #inst "2022-06-07T12:00:00.000-00:00"
      :encounter-transmission/previous-icn             "30000000155672"
      :encounter-transmission/billing-provider-npi     "1508889478"}])


  (count
    (d.peer/q '[:find ?eid ?icn ?previous-icn
                :where
                [(missing? $ ?eid :encounter-transmission/plan-icn)]
                [?eid :encounter-transmission/icn ?icn]
                [?eid :encounter-transmission/previous-icn ?previous-icn]
                ]
      db
      ))  ; => 58

  (pp/pprint
    (vec (take 9
           (d.peer/q '[:find ?eid ?icn ?previous-icn
                       :where
                       [(missing? $ ?eid :encounter-transmission/plan-icn)]
                       [?eid :encounter-transmission/icn ?icn]
                       [?eid :encounter-transmission/previous-icn ?previous-icn]]
             db))))

  (pp/pprint
    (first
      (d.peer/q '[:find (pull ?eid [*])
                  :where
                  [(missing? $ ?eid :encounter-transmission/plan-icn)]
                  [?eid :encounter-transmission/icn ?icn]
                  [?eid :encounter-transmission/previous-icn ?previous-icn]]
        db)))
  (comment ; result
    [[17592186108600 "30000000165819" "30000000165694"]
     [17592186108650 "30000000165883" "30000000165713"]
     [17592186108655 "30000000165888" "30000000165720"]
     [17592186108616 "30000000165811" "30000000165685"]
     [17592186108628 "30000000165807" "30000000165681"]
     [17592186108662 "30000000165895" "30000000165715"]
     [17592186108663 "30000000165896" "30000000165726"]
     [17592186109245 "30000000167521" "30000000167405"]
     [17592186108617 "30000000165817" "30000000165692"]])

  (pp/pprint (d.peer/pull db '[*] 17592186108600))
  (comment ; result
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

  (let [results (vec (d.peer/q '[:find (pull ?eid [:encounter-transmission/icn
                                                   :encounter-transmission/plan
                                                   {:encounter-transmission/status [*]}])
                                 :where
                                 [?eid :encounter-transmission/plan ?plan]
                                 (or
                                   [?eid :encounter-transmission/status :encounter-transmission.status/accepted]
                                   [?eid :encounter-transmission/status :encounter-transmission.status/rejected]
                                   [?eid :encounter-transmission/status :encounter-transmission.status/rejected-by-validation])
                                 [(user/iowa-prefix? ?plan)]]
                       db))
        outpp   (with-out-str
                  (pp/pprint results))]
    (prn :count (count results))
    (spit "file.txt" outpp))

  (let [results (vec (d.peer/q '[:find (pull ?eid [:encounter-transmission/icn
                                                   :encounter-transmission/plan
                                                   {:encounter-transmission/status [*]}]
                                         )
                                 :in $ [?status ...]
                                 :where
                                 [?eid :encounter-transmission/status ?status]
                                 [?eid :encounter-transmission/plan ?plan]
                                 [(user/iowa-prefix? ?plan)]
                                 ]

                       db
                       [:encounter-transmission.status/accepted
                        :encounter-transmission.status/rejected
                        :encounter-transmission.status/rejected-by-validation]))
        outpp   (with-out-str
                  (pp/pprint results))]
    (spit "file.txt" outpp)
    )

  (let [outpp (with-out-str
                (pp/pprint results))]
    (spit "file.txt" outpp)
    )

  )

