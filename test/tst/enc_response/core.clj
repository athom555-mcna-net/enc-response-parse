(ns tst.enc-response.core
  (:use enc-response.core tupelo.core tupelo.test)
  (:require
    [clojure.data :as data]
    [clojure.java.io :as io]
    [clojure.pprint :as pp]
    [clojure.tools.reader.edn :as edn]
    [enc-response.util :as util]
    [schema.core :as s]
    [tupelo.parse :as parse]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    ))

; Enable to see progress printouts
(def ^:dynamic verbose-tests?
  false)

;---------------------------------------------------------------------------------------------------
(verify
  (is= :dummy-fn--result (enc-response.core/dummy-fn))

  ; verify config-load->ctx works
  (let [config-fname "config-tmp.edn"]
    (spit config-fname
      (with-out-str
        (pp/pprint
          (quote
            {:datomic-uri                 "datomic:sql://encounters"
             :postgres-uri                "jdbc:postgresql://postgres.qa:5432/topaz?user=datomic&password=geheim"
             :invoke-fn                   tupelo.core/noop
             :encounter-response-root-dir "/some/path/to/root"
             }))))

    (let [ctx (config-load->ctx config-fname)]
      (is= ctx
        (quote
          {:db-uri                      "datomic:sql://encounters?jdbc:postgresql://postgres.qa:5432/topaz?user=datomic&password=geheim"
           :encounter-response-root-dir "/some/path/to/root"
           :icn-maps-aug-fname          "icn-maps-aug.edn"
           :invoke-fn                   tupelo.core/noop
           :missing-icn-fname           "missing-icns.edn"
           :tx-data-chunked-fname       "tx-data-chuncked.edn" :tx-size-limit 500}
          )))

    ; verify `dispatch` works
    (spit config-fname
      (with-out-str
        (pp/pprint
          (quote
            {:datomic-uri  "aaa"
             :postgres-uri "bbb"
             :invoke-fn    enc-response.core/dummy-fn}))))
    (let [ctx (config-load->ctx config-fname)]
      (is (submatch? '{:db-uri    "aaa?bbb"
                       :invoke-fn enc-response.core/dummy-fn}
            ctx))
      (is= (dispatch ctx) :dummy-fn--result))

    ; verify -main calls :invoke-fn & returns result
    (let [out-txt (with-out-str
                    (let [result (-main config-fname)]
                      (is= result :dummy-fn--result)))]
      ; (spyx-pretty  out-txt)
      (is (str/contains-str-frags? out-txt
            ":main--enter"
            ":main--leave")))
    ))

