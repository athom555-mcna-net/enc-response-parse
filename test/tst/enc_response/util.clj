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

;-----------------------------------------------------------------------------
(verify
  (let [seq1       (range 5)
        arr1       (array-1d->2d 2 seq1)
        seq2       (array-2d->1d arr1)

        inc-1d     (fn->vec-fn inc)
        inc-2d     (fn->vec-fn inc-1d)

        seq1-inc-a (inc-1d seq1)
        arr1-inc   (inc-2d arr1)
        seq1-inc-b (array-2d->1d arr1-inc)
        ]
    (is= seq1 [0 1 2 3 4])
    (is= arr1 [[0 1]
               [2 3]
               [4]])
    (is= seq1 seq2)

    (is= seq1-inc-a [1 2 3 4 5])

    (is= arr1-inc [[1 2]
                   [3 4]
                   [5]])
    (is= seq1-inc-b [1 2 3 4 5])))

