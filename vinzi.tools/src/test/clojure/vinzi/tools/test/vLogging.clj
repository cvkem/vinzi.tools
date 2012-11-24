(ns vinzi.tools.test.vLogging
  (:use clojure.test
        vinzi.tools.vLogging)
  (:require
    [clojure.string :as str]
    [vinzi.tools
     [vLogging :as vLogging]]))


(deftest vLogging-test
  
  )

(comment
  
  (def x {:a 1 :b 2})
  (debug "TESTING"  (with-out-str (pprint x)))
  (error "trigger-print-trace")
  
  
  
  (defn test-it []
  (let [x {:a 1 :b 2}]
    (debug "only text")
    (trace 2 Math/PI "long, double and Text")
    (debug  "with variable:" x)
    (debug "with function over variable:"  (pprint x))
    (doseq [y (range 3)]
      (let [x (assoc x :b y)]
        (debug "Loop-" x  (pprint x))))
    (error "trigger-print-trace")
  ))

  
  )