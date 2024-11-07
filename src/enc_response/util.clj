(ns enc-response.util
  (:use tupelo.core)
  (:require
    [clojure.java.io :as io]
    [clojure.pprint :as pp]
    [clojure.tools.reader.edn :as edn]
    [datomic.api :as d.peer]
    [flatland.ordered.map :as omap]
    [schema.core :as s]
    [tupelo.core :as t]
    [tupelo.math :as math]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    ))

;-----------------------------------------------------------------------------
(s/defn iowa-prefix? :- s/Bool
  [s :- s/Str]
  (t/with-exception-default false ; in case less than 3 chars
    (t/truthy? (= "ia-" (subs s 0 3)))))

