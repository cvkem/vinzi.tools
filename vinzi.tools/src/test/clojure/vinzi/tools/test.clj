(ns vinzi.tools.test
  (:use clojure.test)
  (:require 
    [vinzi.tools
     [vFile :as vFile]
     [mapCompare :as vMap]]))


(deftest vFile-expansion
  (let [currDir (vFile/get-current-dir)]
    (println "Running tests regarding file-expansion")
    (are [x y] (= (vFile/get-path-dir x) y)
         "test"          currDir
         "dir/test"      (str currDir vFile/FileSep "dir")
         "/dir/test"     "/dir"
         "./test"        currDir
         "./dir/test"    (str currDir vFile/FileSep "dir"))
        ))

(deftest mapCompare-tests
  (let [r {:a 1 :b 2}
        rMiss {:c 3 :a 1}
        rExt  (into r rMiss)
        rDiff (assoc r :a -5)
        cmpFlds '(:a :b)
        mcmp (vMap/get-map-comparator cmpFlds)
        {:keys [get-fields compare-fields compare-maps]} mcmp]
    ;; check get-fields
    (is (= (get-fields r) '(1 2))
        "On original record it should just return two values as a list.")
    (is (= (get-fields rExt) '(1 2))
        "On extended record it should just return two values.")
    ;; check compare-maps
    (is (compare-maps r rExt)
        "The extended record should match on the fields of comparison (cmpFlds)")
    (is (compare-maps rExt r)
        "The extended record should match on the fields of comparison (cmpFlds), with rExt first.")
    (is (not (compare-maps r rMiss))
        "Should signal that a value is missing")
    (is (not (compare-maps rMiss r))
        "Should signal that a value is missing, with rMiss first")
    (is (not (compare-maps r rDiff))
        "Different value, so not comparible")
    (is (not (compare-maps rDiff r))
        "Different value, so not comparible, with rDiff first")
    ;; check compare fields
    (is (compare-fields r '(1 2))
        "Comparing agains list of values should have matched.")
    (is (not (compare-fields r '(1)))
        "List of fields is too short.")
    (is (not (compare-fields r '(1 2 3)))
        "List of fields is too long.")
    (is (not (compare-fields r '(1 3)))
        "Difference in second value.")
    ))


(comment   ;; code for testing and as example
(defn my-tests []
  (let [r {:a 1 :b 2}
        rMiss {:c 3 :a 1}
        rExt  (into r rMiss)
        rDiff (assoc r :a -5)
        cmpFlds '(:a :b)
        mcmp (get-map-comparator cmpFlds)
        {:keys [get-fields compare-fields compare-maps]} mcmp]
    (println " The base record is: " r)
    (println " The selection for comparison are: " cmpFlds)
    (println "The selected fields of " rExt " are: " (get-fields rExt))
    (println " comparison to: " rMiss "  returns " (compare-maps r rMiss))
    (println " rev-comparison to: " rMiss "  returns " (compare-maps rMiss r))
    (println " comparison to: " rExt "  returns " (compare-maps r rExt))
    (println " comparison to: " rDiff "  returns " (compare-maps r rDiff))
    ))
)