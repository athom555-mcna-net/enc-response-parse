(defproject enc-response-parse "1.0.0-SNAPSHOT"
  ;-----------------------------------------------------------------------------
  ; ***** NOTE *****
  ; See `deps.edn` re 401 error if minimal `project.clj` not present for UHC/MCNA
  ;-----------------------------------------------------------------------------

  :dependencies [
                 [com.datomic/peer          "1.0.7277"]
                 [org.clojure/clojure       "1.12.0"]
                 [org.flatland/ordered      "1.15.12"]
                 [org.postgresql/postgresql "42.7.5"]
                 [prismatic/schema          "1.4.1"]
                 [tupelo/tupelo             "24.12.03b"]
                 ]

  :jvm-opts ["-Xmx4g"]
  :main enc-response.core

  )
