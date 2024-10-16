(defproject enc-response-parse "1.0.0-SNAPSHOT"
  ;-----------------------------------------------------------------------------
  ; ***** NOTE *****
  ; See `deps.edn` re 401 error if minimal `project.clj` not present for UHC/MCNA
  ;-----------------------------------------------------------------------------

  :dependencies [
                 [org.clojure/clojure "1.12.0"]
                 [prismatic/schema "1.4.1"]
                 [tupelo "24.09.30"]
                 ]

  :jvm-opts ["-Xmx4g"]
  :main enc-response-parse.core

  )

