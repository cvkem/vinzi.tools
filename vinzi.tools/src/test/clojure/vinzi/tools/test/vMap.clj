(ns vinzi.tools.test.vMap
  (:use clojure.test)
  (:require
    [clojure.string :as str]
    [vinzi.tools
     [vDateTime :as vDate]
     [vMap :as vMap]]))


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
  
    (let [ar [r r]
          arLonger [r r r]
          arShorter [r]
          rDiff1 [r rMiss]
          rDiff2 [rExt r]]
      (is (vMap/compare-map-arrays ar ar))
      (are [x y] (not (vMap/compare-map-arrays x y))
           ar arLonger
           ar arShorter
           ar rDiff1
           rDiff1 ar
           ar rDiff2
           rDiff2 ar)
           rDiff
      )))


(deftest test-map-str-convertor
  (println "testing the get-map-str-convertor")
  (are [tp val res]
       (let [mc (vMap/get-map-str-convertor {:a tp})]
         (= (:a (mc {:a val})) res))
;         (= (:a ((vMap/get-map-str-convertor {:a tp}) {:a val})) 1)
       "integer"   "1"    1
       "integer"   " 1"   1
       "double"    "1.0"  1.0
       "string"    "abc"  "abc"
       "varchar"   "abc"  "abc"
       "varchar(30)" "abc" "abc"
       "TeXt"        "abc" "abc"
       "character varying"  "abc" "abc"
       "date"        "2012-01-02" (vDate/make-sql-date 2012 1 2)
       "timestamp "  "2012-01-02 01:02:03.456" (java.sql.Timestamp. (.getTime (vDate/make-sql-date 2012 1 2    1 2 3)))
       "timestamp without time zone"   "2012-01-02 01:02:03.456" (java.sql.Timestamp. (.getTime (vDate/make-sql-date 2012 1 2    1 2 3)))
       "boolean"     "true"  true
       :boolean      " false " false
       :boolean      "T"       true
       "boolean"      "f"       false
       :boolean      "F"       false
       :double       "2.3" 2.3)
  
  (let [mc (vMap/get-map-str-convertor {:i "integer" :d :double :s "text"})]
    (is (= (map (mc {:i " 1" :d "1.0" :s "abc"}) [:i :d :s]) '(1 1.0 "abc"))
        "Comparing with multiple keys failed."))
  )


