(ns vinzi.tools.test
  (:use clojure.test)
  (:require
    [clojure.string :as str]
    [vinzi.tools
     [vFile :as vFile]
     [vMap :as vMap]
     [vSql :as vSql]
     [vDateTime :as vDate]]))


(deftest vSql-tests
  ;; testing qs
  (are [x res] (= (vSql/qs x) res)
       "test"        "\"test\""
       "\"test\""    "\"test\"")
  ;; unbalanced quotes throw exception
  (are [x res] (thrown? Exception (vSql/qs x))
       "\"test"    
       "test\"")
  
  ;; testing sqs
  (are [x res] (= (vSql/sqs x) res)
       "test"        "'test'"
       "'test'"    "'test'")
  ;; unbalanced quotes throw exception
  (are [x res] (thrown? Exception (vSql/sqs x))
       "'test"    
       "test'")
  
  ;; testing strip-dQuotes
  (are [x res] (= (vSql/strip-dQuotes x) res)
       "\"test\""        "test"
       "test"            "test")
  ;; unbalanced quotes throw exception
  (are [x res] (thrown? Exception (vSql/strip-dQuotes x))
       "\"test"    
       "test\"")
  
  )

(deftest vFile-expansion
  (let [currDir (vFile/get-current-dir)]
    (println "Running tests regarding file-expansion")
    (are [x y] (= (vFile/get-path-dir x) y)
         "test"          currDir
         "test/"          currDir
         "dir/test"      (str currDir vFile/FileSep "dir")
         "/dir/test"     "/dir"
         "./test"        currDir
         "./dir/test"    (str currDir vFile/FileSep "dir")
         "./dir/test/"    (str currDir vFile/FileSep "dir"))
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


(deftest vDate-sql-date
  (let [sd (vDate/make-sql-date 2012 1 2    1 2 3)
        d (java.util.Date. (.getTime sd))
        ts (java.sql.Timestamp. (.getTime d))
;;        sd (java.sql.Date. (.getTime d))
        l  (.getTime d)
        longStr   (str ts)  ;; format YYYY-MM-DD HH:MM:SS.MS  (ms part is ignored)
        shortStr  (first (str/split longStr #" "))  ;; format YYYY-MM-DD
        o  (Object.)]
    ;; convertions to java.sql.Date
    (are [x]  (= sd (vDate/convert-to-date x)) d ts l longStr)
    (is (thrown? java.lang.Exception (vDate/convert-to-date o)))    
    ;; convertions to java.sql.Date
    (are [x]  (= ts (vDate/convert-to-timestamp x)) d sd l longStr)
    (is (thrown? java.lang.Exception (vDate/convert-to-timestamp o)))
    (let [sd (vDate/make-sql-date 2012 1 2)
          ts (java.sql.Timestamp. (.getTime sd))
          longStr   (str ts)
          shortStr  (first (str/split longStr #" "))]  ;; format YYYY-MM-DD
      (is (= sd (vDate/convert-to-date shortStr)))
      (is (= ts (vDate/convert-to-timestamp shortStr)))
    )))


(deftest vDate-day-of-week
  (are [y m d res] (= (vDate/get-day-of-week (vDate/make-sql-date y m d)) res)
       1969 03 24  2   ;; monday
       2012 8 27  2    ;; monday
       2012 8  26  1   ;; sunday
       2012 8 25  7    ;; saterday
       2012 8 06  2)
  )

(deftest vDate-dayOffset
  (are [y m d res] (= (str (vDate/get-date-dayOffset (vDate/make-sql-date y m d) -7)) res)
       2012 8 27  "2012-08-20"
       2012 8  7  "2012-07-31"
       2012 3  7  "2012-02-29"    ;; leap-year
       2011 3  7  "2011-02-28"    ;; no leap-year
;;  2000 has been corrected, but (java-dates assume leap-year !!)       
;;       2000 3  7  "2000-02-28"    ;; no leap-year (special case)
       2012 1  7  "2011-12-31"
  ))

(deftest test_split-qualified-name
  (are [q schema tbl] (= (map (vSql/split-qualified-name q) [:schema :table]) '(schema tbl))
       (vSql/qsp "sch" "tbl")   "sch"      "tbl"
       "\"schema\".\"table\""   "schema"   "table"
       "\"sch_ema\".\"table\""   "sch_ema"   "table"
       "\"schema\".\"t_able\""   "schema"   "t_able"
       ;; new version excepts names without schema, as long as they are quoted.
       (vSql/qs "table")        nil        "table")
  ;; New version of split throws exceptions on errors in the input.
  (are [x] (thrown? Exception (vSql/split-qualified-name x))
       nil
       "noquoted.name"
       "\"unbalanced_quote"))
  
  