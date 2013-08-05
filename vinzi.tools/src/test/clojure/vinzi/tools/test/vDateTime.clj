(ns vinzi.tools.test.vDateTime
  (:use clojure.test)
  (:require
    [clojure.string :as str]
    [vinzi.tools
     [vDateTime :as vDate]]))


(deftest make-sql-date
  (is (= (.toString (vDate/make-sql-date 2011 1 1)) "2011-01-01"))
  ;; on the repl this string returns
;	=> (def d (vDate/make-sql-date 2011 1 1))
;	#'vinzi.tools.test.vDateTime/d
;	=> (.toString d)
;	"2011-01-01"
;	=> d
;	#inst "2010-12-31T23:00:00.000-00:00"
  ;; so it looks like a 2010 date instead of 2011. Probably the repl time is GMT.

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


(deftest start-mid-end-day-test
  (let [start (vDate/make-sql-date 2012 9 10)
        start2 (vDate/make-sql-date 2012 9 10 0 0 0)
        early (vDate/make-sql-date 2012 9 10 0 30 0)
        mid   (vDate/make-sql-date 2012 9 10 12 0 0)
        late (vDate/make-sql-date 2012 9 10 23 30 0)
        end  (vDate/make-sql-date 2012 9 10 23 59 59)
;;        end2  (vDate/make-sql-date 2012 9 10 23 60 0)  already is next day
;;        end3  (vDate/make-sql-date 2012 9 10 23 59 60) already is next day
        ]
    ;; day should stay unchanged
    (are [dt] (= 10 (:day (vDate/get-ymd-date (vDate/convert-to-date (vDate/get-TS-midDay dt)))))
         start
         start2
         early 
         mid
         late
         end)
    ;; day should stay unchanged
    (are [dt] (= 10 (:day (vDate/get-ymd-date (vDate/convert-to-date (vDate/get-TS-endDay dt)))))
         start
         start2
         early 
         mid
         late
         end)
    ;; day should stay unchanged
    (are [dt] (= 10 (:day (vDate/get-ymd-date (vDate/convert-to-date (vDate/get-TS-startDay dt)))))
         start
         start2
         early 
         mid
         late
         end)
    
    ))


(deftest get-ymd-date-test
  (testing "get-ymd-date with correctly formatted dates failed:"
  (are [inp res] (= (vDate/get-ymd-date inp) res)
       "2010-01-01" {:year 2010 :month 1 :day 1}
       "1970-02-03" {:year 1970 :month 2 :day 3}
       ))
  (testing "get-ymd-date with badly formatted dates failed:"
  (are [inp res] (= (vDate/get-ymd-date inp) res)
       "2010-1-01" {:year 2010 :month 1 :day 1}
       "1970-02-3" {:year 1970 :month 2 :day 3}
       ))
  )
(comment 
(deftest comparator-test
  (let [yesterday (vDate/make-sql-date 2012 9 9 23 59 59)
        start (vDate/make-sql-date 2012 9 10)
        start2 (vDate/make-sql-date 2012 9 10 0 0 0)
        early (vDate/make-sql-date 2012 9 10 0 30 0)
        mid   (vDate/make-sql-date 2012 9 10 12 0 0)
        late (vDate/make-sql-date 2012 9 10 23 30 0)
        end  (vDate/make-sql-date 2012 9 10 23 59 59)
        tomorrow (vDate/make-sql-date 2012 9 11)
        tomorrow2 (vDate/make-sql-date 2012 9 11 0 0 0)
        
        startComp (vDate/gen-date-comparator>=startDay mid)
        midComp   (vDate/gen-date-comparator<= mid)
        endComp   (vDate/gen-date-comparator<= end)
        sameComp   (vDate/gen-date-comparator-sameDay end)
        ]
    (are [dt res] (= (startComp dt) res)
         yesterday false
         start true
         early true
         mid   true
         late  true
         end   true
         tomorrow true
         tomorrow2 true)
    (are [dt res] (= (midComp dt) res)
         yesterday true
         start true
         early true
         mid   true
         late  false
         end   false
         tomorrow false
         tomorrow2 false)
    (are [dt res] (= (endComp dt) res)
         yesterday true
         start true
         early true
         mid   true
         late  true
         end   true
         tomorrow false
         tomorrow2 false)
    (are [dt res] (= (sameComp dt) res)
         yesterday false
         start true
         early true
         mid   true
         late  true
         end   true
         tomorrow false
         tomorrow2 false)
))
)
