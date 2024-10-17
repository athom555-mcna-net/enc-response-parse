(defproject enc-response-parse "1.0.0-SNAPSHOT"
  ;-----------------------------------------------------------------------------
  ; ***** NOTE *****
  ; See `deps.edn` re 401 error if minimal `project.clj` not present for UHC/MCNA
  ;-----------------------------------------------------------------------------

  :dependencies [
                 [com.datomic/local "1.0.285"]
                 [com.datomic/peer "1.0.7187"]
                 [org.clojure/clojure "1.12.0"]
                 [org.flatland/ordered "1.15.12"]
                 [org.postgresql/postgresql "42.5.1"]
                 [prismatic/schema "1.4.1"]
                 [tupelo/tupelo "24.09.30"]
                 ]

  :jvm-opts ["-Xmx4g"]
  :main enc-response-parse.core

  )

