(ns tst.enc-response.util
  (:use enc-response.util
        tupelo.core
        tupelo.test)
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
    )
  (:import
    [java.io File]
    ))

(verify
  (let [s "abcdef"]
    (is= "abc" (subs s 0 3)))
  (is (iowa-prefix? "ia-plan"))
  (is (iowa-prefix? "ia-"))
  (is (iowa-prefix? "ia-medicare"))
  (isnt (iowa-prefix? "ia"))
  (isnt (iowa-prefix? "id-"))
  (isnt (iowa-prefix? "id-medicare"))
  )

