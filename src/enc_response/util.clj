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

; modified from clojure.core/with-out-str
;(defmacro discarding-out-str
;  [& body]
;  "Evaluates exprs in a context in which *out* is bound to a fresh StringWriter.
;  Discards the string created by any nested printing calls."
;  `(let [s# (new java.io.StringWriter)]
;     (binding [*out* s#]
;       ~@body)))

;-----------------------------------------------------------------------------
(s/defn iowa-prefix? :- s/Bool
  [s :- s/Str]
  (t/with-exception-default false ; in case less than 3 chars
    (t/truthy? (= "ia-" (subs s 0 3)))))

;-----------------------------------------------------------------------------
(s/defn fn->vec-fn :- tsk/Fn
  "Vectorize a function, so that instead of operating on a scalar value,
  it operates on each value in a 1D array. Used twice, the resulting function operates
  on each value in a 2D array. Not lazy."
  [f :- tsk/Fn]
  (s/fn [block :- [s/Any]]
    (mapv f block)))

(s/defn fn->vec-fn-lazy :- tsk/Fn
  "Vectorize a function, so that instead of operating on a scalar value,
  it operates on each value in a 1D array. Used twice, the resulting function operates
  on each value in a 2D array. Lazy."
  [f :- tsk/Fn]
  (s/fn [block :- [s/Any]]
    (map f block)))

(s/defn array-1d->2d-lazy :- [[s/Any]]
  "Convert a 1D sequence to a lazy 2D array (possibly ragged) in row-major order."
  [row-size :- s/Int
   seq-1d :- [s/Any]]
  (partition-all row-size seq-1d))

(s/defn array-1d->2d :- [[s/Any]]
  "Convert a 1D sequence to a 2D array (possibly ragged) in row-major order."
  [row-size :- s/Int
   seq-1d :- [s/Any]]
  (unlazy (array-1d->2d-lazy row-size seq-1d)))

(s/defn array-2d->1d :- [s/Any]
  "Concatenate rows of a 2D array (possibly ragged), returning a 1-D vector."
  [seq-2d :- [[s/Any]]]
  (apply glue seq-2d))

(s/defn array-2d->1d-lazy :- [s/Any]
  "Concatenate rows of a 2D array (possibly ragged), returning a 1-D vector."
  [seq-2d :- [[s/Any]]]
  (apply concat seq-2d))

; #awt 2024-11-19 works, but no advantage to `(apply concat seq-2d)`
(comment
  (s/defn array-2d->1d-lazy-gen :- [s/Any]
    "Concatenate rows of a 2D array (possibly ragged), returning a 1-D vector."
    [seq-2d :- [[s/Any]]]
    (lazy-gen
      (doseq [row seq-2d]
        (doseq [item row]
          (yield item))))))

#_(s/defn array-2d->1d-lazy-recur :- [s/Any]
    "Concatenate rows of a 2D array (possibly ragged), returning a 1-D vector."
    [seq-2d :- [[s/Any]]]
    (loop [rows seq-2d]
      (let [row-curr  (first rows)
            rows-next (rest rows)]

        )
      )

    (lazy-gen
      (doseq [row seq-2d]
        (doseq [item row]
          (yield item)))))

;-----------------------------------------------------------------------------
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
