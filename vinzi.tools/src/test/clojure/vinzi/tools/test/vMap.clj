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


(deftest mapComparator-tests
  (let [r1 {:a 1 :b 2}
        r2 {:a 2 :b 2}
        r3 {:a 1 :b 1}
        cmpFlds '(:a :b)
        sign (fn [x] (if (> x 0) 1 (if (< x 0) -1 0)))
        mcmp (vMap/get-map-comparator cmpFlds)
        {:keys [map-comparator]} mcmp]
    (are [x y res] (= (sign (map-comparator x y)) res)
         r1 r1  0
         r1 r2  -1
         r1 r3  1
         r2 r3  1
         )))


(deftest test-map-type-convertor
  (println "testing the get-map-type-convertor")
  (are [tp val res]
       (let [mc (vMap/get-map-type-convertor {:a tp})]
         (= (:a (mc {:a val})) res))
;         (= (:a ((vMap/get-map-type-convertor {:a tp}) {:a val})) 1)
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
       :keyword      "test"    :test
       :keyword      "5"       :5
       :double       "2.3" 2.3
       :string-range "aa-bb,dd,ww-" [["aa" "bb"] "dd" ["ww" nil]]
       )
  
  (let [mc (vMap/get-map-type-convertor {:i "integer" :d :double :s "text"})]
    (is (= (map (mc {:i " 1" :d "1.0" :s "abc"}) [:i :d :s]) '(1 1.0 "abc"))
        "Comparing with multiple keys failed."))
  )

(deftest test-keywordize
  (let [inp {"Test" 1 :KEYW 2}]
    (is (= (vMap/keywordize inp) {:Test 1 :KEYW 2}))
    (is (= (vMap/keywordize inp true) {:test 1 :keyw 2}))))



(deftest test-checked-add-kv
  (are [curr add  res] (= (vMap/checked-add-kv curr add) res)
       {} [:a 1] {:a 1}
       {:b 2} [:a 1] {:a 1 :b 2} 
       {} ["a" 1] {"a" 1}
       {:b 2} ["a" 1] {"a" 1 :b 2} 
       
       )
  (are [kvSeq res]  (= (vMap/kvSeq-to-hashmap kvSeq) res)
       '([:a 1])  {:a 1}
       '([:a 1] ["b" 2])  {:a 1 "b" 2})

  (are [hmSeq kkey kval res]  (= (vMap/hmSeq-to-hashmap hmSeq kkey kval) res)
       '({:k :a :v 1}) :k :v  {:a 1}
       '({:k :a :v 1} {:k :b :v 2}) :k :v  {:a 1 :b 2}
       '({:k :a :v 1} {:k "b" :v 2}) :k :v  {:a 1 "b" 2}
       '({:k :a "kval" 1} {:k "b" "kval" 2}) :k "kval"  {:a 1 "b" 2}
       )
  )



(deftest test-checked-merge
  (are [hm add res] (= (vMap/checked-merge hm add) res)
     {:a 1} {:b 2}  {:a 1 :b 2}
     {} {:b 2}  {:b 2}
  )

  (are [hm add] (thrown? Exception (vMap/checked-merge hm add))
     {:a 1} {:a 2} 
     {:a 1} {:a 1} 
     {:a 1 :b 2} {:b 2} 
  )
)


(deftest test-parse-string-range
  (are [args res] (= (vMap/parse-string-range args) res)
       "aa"   ["aa"]
       "aa-bb" [["aa" "bb"]]
       "aa-" [["aa" nil]]
       "-aa" [[nil "aa"]]
       "aa,bb" ["aa" "bb"]
      )) 


(deftest test-get-map-type-convertor-2
  (let [ loc-args (vMap/get-map-type-convertor {:locs :string-range}) ]
  (are [args res] (= (loc-args args) res)
        {:locs "aa"}   {:locs ["aa"]}
        {:locs "aa-bb"} {:locs [["aa" "bb"]]}
      )) 
    )

