(ns tst.enc-response.util
  (:use enc-response.util
        tupelo.core
        tupelo.test)
  (:require
    [enc-response.util :as util]
    [tupelo.profile :as prof]
    [schema.core :as s]
    ))

(def verbose? false)

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
  (throws-not? (date-str-mmddyyyy->iso "99887766")) ; no validaion of reasonable date
  (throws? (date-str-mmddyyyy->iso "xxyy7766")) ; invalid chars )
  )

;-----------------------------------------------------------------------------
(verify
  (let [seq1          (range 5)
        arr1          (array-1d->2d 2 seq1)
        seq2          (array-2d->1d arr1)

        inc-1d        (fn->vec-fn inc)
        inc-2d        (fn->vec-fn inc-1d)

        seq1-inc-a    (inc-1d seq1)
        arr1-inc      (inc-2d arr1)
        seq1-inc-b    (array-2d->1d arr1-inc)
        seq1-inc-lazy (array-2d->1d arr1-inc)
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
    (is= seq1-inc-b [1 2 3 4 5])
    (is= seq1-inc-lazy [1 2 3 4 5])))

(verify
  ; ***** Need to disable Plumatic Schema validation of arguments or destroys laziness!!! *****
  (s/without-fn-validation

    ; Avoids the concat StackOverflow bug:  https://stuartsierra.com/2015/04/26/clojure-donts-concat
    ; Normal stackoverflow at about 4000 frames on common JVMs
    ; 2024-11-18 will overflow for N=39999
    (let [N                     9999 ; Note:  Use 9999 for real stress test
          num-triangle-elements (it-> N
                                  (* it (inc it))
                                  (/ it 2))
          first-37              [1 1 2 1 2 3 1 2 3 4 1 2 3 4 5 1 2 3 4 5 6 1 2 3 4 5 6 7 1 2 3 4 5 6 7 8 1]

          lazy-fn               (fn lazy-fn []
                                  (let [triangle-2d (map #(range 1 (inc %)) (range 1 (inc N)))
                                        triangle-1d (array-2d->1d-lazy-concat triangle-2d)]
                                    (is= first-37 (take 37 triangle-1d))))
          eager-fn              (fn eager-fn []
                                  (let [triangle-2d (mapv #(thru 1 %) (thru 1 N))
                                        triangle-1d (array-2d->1d triangle-2d)]
                                    (is= first-37 (take 37 triangle-1d))
                                    (is= num-triangle-elements (count triangle-1d))))]

      ; Results:  N=9999  (2024-11-18 Mac Studio)
      ;   :with-timer-print :lazy   0.000087
      ;   :with-timer-print :eager  1.873043
      (if verbose?
        (do
          (nl)
          (prn :-----------------------------------------------------------------------------)
          (prn :v1)
          (prof/with-timer-print :lazy (lazy-fn))
          (prof/with-timer-print :eager (eager-fn))
          )
        (do
          (lazy-fn))))))

(verify
  ; ***** Need to disable Plumatic Schema validation of arguments or destroys laziness!!! *****
  (s/without-fn-validation

    (let [N              1e7 ; Note:  Use 9999 for real stress test
          ncols          (long (Math/round (Math/sqrt N)))
          vals           (range N)

          lazy-fn-concat (fn lazy-fn []
                           (let [triangle-2d (array-1d->2d-lazy ncols vals)
                                 triangle-1d (array-2d->1d-lazy-concat triangle-2d)]
                             (is= (range 37) (take 37 triangle-1d))))

          #_(comment
              lazy-fn-gen (fn lazy-fn []
                            (let [triangle-2d (array-1d->2d-lazy ncols vals)
                                  triangle-1d (array-2d->1d-lazy-gen triangle-2d)]
                              (is= (range 37) (take 37 triangle-1d)))))

          eager-fn       (fn eager-fn []
                           (let [triangle-2d (array-1d->2d ncols vals)
                                 triangle-1d (array-2d->1d triangle-2d)]
                             (is= (range 37) (take 37 triangle-1d))))
          ]

      ; Results:  N=1e7  (2024-11-18 Mac Studio)
      ;   :with-timer-print :lazy-gen     0.000634
      ;   :with-timer-print :lazy-concat  0.000882
      ;   :with-timer-print :eager        1.826692
      (if verbose?
        (do
          (nl)
          (prn :-----------------------------------------------------------------------------)
          (prn :v2)
          ; (prof/with-timer-print :lazy-gen (lazy-fn-gen))
          (prof/with-timer-print :lazy-concat (lazy-fn-concat))
          (prof/with-timer-print :eager (eager-fn))
          )
        (do
          (lazy-fn-concat)))))
  )

