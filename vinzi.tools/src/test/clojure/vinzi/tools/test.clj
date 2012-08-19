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
  (are [x res] (= (vSql/qs x) res)
       "test"        "\"test\""
       "\"test\""    "\"test\"")
  ;; unbalanced quotes throw exception
  (are [x res] (thrown? Exception (vSql/qs x))
       "\"test"    
       "test\"")
  (are [x res] (= (vSql/sqs x) res)
       "test"        "'test'"
       "'test'"    "'test'")
  ;; unbalanced quotes throw exception
  (are [x res] (thrown? Exception (vSql/sqs x))
       "'test"    
       "test'")
  )

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
        