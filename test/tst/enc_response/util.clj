(ns tst.enc-response.util
  (:use enc-response.util
        tupelo.core
        tupelo.test)
  (:require
    [enc-response.util :as util]
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

