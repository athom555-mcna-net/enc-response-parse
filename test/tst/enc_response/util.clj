(ns tst.enc-response.util
  (:use enc-response.util
        tupelo.core
        tupelo.test)
  (:require
    [enc-response.util :as util]
    [tupelo.profile :as prof]
    [tupelo.schema :as tsk]
    [schema.core :as s]
    ))

(def verbose? true)

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

(verify
  (is= "2024-07-14" (date-str-mmddyyyy->iso "07142024"))
  (throws? (date-str-mmddyyyy->iso "7142024"))
  (throws? (date-str-mmddyyyy->iso "99887766")) ; validaion detects out-of-range values
  (throws? (date-str-mmddyyyy->iso "xxyy7766")) ; invalid chars )
  )

