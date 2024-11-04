(ns tst.enc-response-parse.parse-to-datomic
  (:use enc-response-parse.parse-to-datomic
        tupelo.core
        tupelo.test)
  (:require
    [clojure.pprint :as pp]
    [datomic.api :as d]
    [demo.util :as u]
    [schema.core :as s]
    [tst.conf :as conf]
    [tupelo.core :as t]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    [tupelo.test.jvm :as ttj]
    ))

(def verbose? true)

; Defines URI for local transactor in `dev` mode. Uses `data-dir` in transactor *.properties file.
; Default entry `data-dir=data` => /opt/datomic/data/...
; Absolute path entry like `data-dir=/Users/myuser/datomic-data` => that directory.
(def db-uri-disk "datomic:dev://localhost:4334/enc-response-test")

(ttj/define-fixture :each
  {:enter (fn [ctx]
            (cond-it-> (validate boolean? (d/delete-database db-uri-disk)) ; returns true/false
              verbose? (println "  Deleted prior db: " it))
            (cond-it-> (validate boolean? (d/create-database db-uri-disk))
              verbose? (println "  Creating db:      " it)))
   :leave (fn [ctx]
            (cond-it-> (validate boolean? (d/delete-database db-uri-disk))
              verbose? (println "  Deleting db:      " it)))
   })

(def ctx-local
  {:encounter-response-root-dir "./enc-response-files-test-small" ; full data:  "/Users/athom555/work/iowa-response"
   :missing-icn-fname           "resources/missing-icns-5.edn"
   :icn-maps-aug-fname          "icn-maps-aug.edn"
   :tx-data-chunked-fname       "tx-data-chuncked.edn"
   :tx-size-limit               2
   })

(verify
  (let [
        conn         (d/connect db-uri-disk)

        resp2        @(d/transact conn enc-response-schema)


        ; >>           (pprint/pprint resp2)
        first-movies [{:movie/title        "The Goonies"
                       :movie/genre        "action/adventure"
                       :movie/release-year 1985}
                      {:movie/title        "Commando"
                       :movie/genre        "thriller/action"
                       :movie/release-year 1985}
                      {:movie/title        "Repo Man"
                       :movie/genre        "punk dystopia"
                       :movie/release-year 1984}]
        resp3        @(d/transact conn first-movies)
        ; >>           (pprint/pprint resp3)

        db           (d/db conn)
        all-titles-q '[:find ?movie-title
                       :where [_ :movie/title ?movie-title]]
        raw-results  (d/q all-titles-q db)
        ; >> (spyxx raw-results)
        movies       (onlies raw-results)
        ]
    (is-set= movies ["Commando" "The Goonies" "Repo Man"])

    ; Show the archaic "single result" notation (single `.` at end of `:find` clause)
    (let [movies-count-1 (d/q '[:find (count ?movie-title)
                                :where [_ :movie/title ?movie-title]]
                           db)
          movies-count-2 (d/q '[:find (count ?movie-title) . ; <= NOTE tiny `.` (dot) here
                                :where [_ :movie/title ?movie-title]]
                           db)]
      (is= [[3]] movies-count-1)
      (is= 3 (only2 movies-count-1)) ; i.e. (only (only movies-count-1)), or (ffirst movies-count-1)
      (is= 3 movies-count-2))

    (let [eid          (only2 (d/q '[:find ?eid
                                     :where [?eid :movie/title "Commando"]]
                                db))
          entity       (d/entity db eid) ; like #:db{:id 17592186045421}
          entity-db-id (:db/id entity)]
      (is (pos-int? eid)) ; like 96757023244365
      (is (pos-int? entity-db-id))
      (is= (type entity) datomic.query.EntityMap)
      ; NOTE: Datomic Entity is a "lazy associative object". It must be forced to reveal its contents.
      ; Regular print results in this:
      (let [pr-str (with-out-str (pr entity))]
        (is= pr-str "#:db{:id 17592186045421}"))
      ; Must use `(unlazy entity)` or equiv to force evaluation
      (is= (into {} entity)
        #:movie{:genre "thriller/action" :release-year 1985 :title "Commando"})

      (is (wild-match? {:db/id              :*
                        :movie/genre        "thriller/action",
                        :movie/release-year 1985,
                        :movie/title        "Commando"}
            (d/pull (d/db conn) '[*] eid))))

    (let [titles-from-1985     '[:find ?movie-title
                                 :where [?e :movie/title ?movie-title]
                                 [?e :movie/release-year 1985]]
          titles-1985          (d/q titles-from-1985 db)
          q-all-data-from-1985 '[:find ?title ?year ?genre
                                 :where [?e :movie/title ?title]
                                 [?e :movie/release-year ?year]
                                 [?e :movie/genre ?genre]
                                 [?e :movie/release-year 1985]]
          all-data-from-1985   (d/q q-all-data-from-1985 db)]
      (is-set= titles-1985 [["Commando"] ["The Goonies"]])
      (is-set= all-data-from-1985
        [["Commando" 1985 "thriller/action"]
         ["The Goonies" 1985 "action/adventure"]])

      (let [commando-id (only2 (d/q '[:find ?e
                                      :where [?e :movie/title "Commando"]]
                                 db))]
        ; (spyx commando-id)
        @(d/transact conn [{:db/id       commando-id
                            :movie/genre "future governor"}]) ; NOTE:  no `@` deref for print!!!

        ; No change!!! Since used old value of DB
        (is-set= (d/q q-all-data-from-1985 db)
          [["Commando" 1985 "thriller/action"]
           ["The Goonies" 1985 "action/adventure"]])

        ; Use new DB value
        (is-set= (d/q q-all-data-from-1985 (d/db conn))
          [["Commando" 1985 "future governor"]
           ["The Goonies" 1985 "action/adventure"]])
        ))
    ))


