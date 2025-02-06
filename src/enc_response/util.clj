(ns enc-response.util
  (:use tupelo.core)
  (:require
    [schema.core :as s]
    [tupelo.core :as t]
    [tupelo.schema :as tsk]
    [tupelo.string :as str]
    )
  (:import
    [java.time LocalDate]
    ))

(s/defn iowa-prefix? :- s/Bool
  [s :- s/Str]
  (t/with-exception-default false ; in case less than 3 chars
    (t/truthy? (= "ia-" (subs s 0 3)))))

(s/defn date-str-mmddyyyy->iso :- s/Str
  "Convert a date string like `07142024` to ISO format like `2024-07-14`"
  [date-str :- s/Str]
  (let [date-str (str/trim date-str)
        nchars   (count date-str)]
    (assert-info (= 8 nchars) "Invalid length" (vals->map date-str nchars))
    (assert-info (re-matches #"\p{Digit}+" date-str)
      "date string must be all digits" (vals->map date-str))
    (let [mm     (subs date-str 0 2)
          dd     (subs date-str 2 4)
          yyyy   (subs date-str 4 8)
          result (str yyyy "-" mm "-" dd)]
      (LocalDate/parse result) ; parse result to validate legal values
      result)))
