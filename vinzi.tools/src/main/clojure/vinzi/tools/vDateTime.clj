(ns vinzi.tools.vDateTime
   (:use	[clojure [pprint :only [pprint pp]]]
        [clojure [stacktrace :only [print-stack-trace root-cause]]]
        [clojure.tools [logging :only [error info trace debug warn]]])
  (:require [clojure
             [string :as str]]
            [clojure.java
             [jdbc :as sql]]))

;; Still need to add a few UNIT-test (code examples to the test-file)

;; get the current date/time in two formats
(def ^:dynamic Now  nil)
(def ^:dynamic NowMillis nil)

(defn set-Now []
  (def Now (java.util.Date.))
  (def NowMillis (.getTime Now))
  Now)


(set-Now)
(warn "In vinzi.tools.vDateTime Now is set at: " Now
      "this values will only be updated at the next start of the JVM!!"
      "(use 'vinzi.tools.vDateTime/set-Now to update it)" )


(defn get-timestamp 
  "Returns the time-stamp of the java-util.Date that is passed. If no arguments is passed it returns now()."
  ([]
     (get-timestamp (java.util.Date.)))
  ([date]
   {:pre [(= (type date) java.util.Date)]}
   (java.sql.Timestamp. (.getTime date))))

(defn get-time-millis 
  "Get a millisecond timestamp of a Gregorian Date, where
   (str (java.sql.Date. (get-time-millis 1950 1 2)))
    corresponds to 1950-01-02."
  ([year month day]
    (.getTimeInMillis
      ;; in Gregorian calendar system:
      ;;    - first month is 0  (January)
      ;;    - first day of month is 1
      (java.util.GregorianCalendar. year (dec month) day)))
    ([year month day hours minutes seconds]
      (.getTimeInMillis
        ;; in Gregorian calendar system:
        ;;    - first month is 0  (January)
        ;;    - first day of month is 1
        (java.util.GregorianCalendar. year (dec month) day hours minutes seconds))))

(defn make-sql-date 
  "Prepare an SQL-date based on 'year', 'month', 'day'. Returns a (binary) java.sql.Date object. If you need to include the date in a string you can use the (str java.sql.Date)."
  ([year month day]
    (java.sql.Date.
      (get-time-millis year month day)))
  ([year month day hours minutes seconds]
    (java.sql.Date.
      (get-time-millis year month day hours minutes seconds))))


(def Interval40yrsMillis (- (get-time-millis 2012 1 1)
		   (get-time-millis 1972 1 1)))

(def Interval1yrsMillis  (long (/ Interval40yrsMillis 40)))
(def Interval2yrsMillis  (long (/ Interval40yrsMillis 20)))
(def Interval3yrsMillis  (long (* 3 (/ Interval40yrsMillis 40))))
(def Interval5yrsMillis  (long (/ Interval40yrsMillis 8)))

(def Interval1weekMillis  (long (- (get-time-millis 2012 1 8) (get-time-millis 2012 1 1))))
(def Interval1dayMillis  (long (- (get-time-millis 2012 1 2) (get-time-millis 2012 1 1))))
(def Interval1minuteMillis  (long (/ Interval1dayMillis (* 24 60))))






;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; functions for string-conversion and ymd transformations
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn str-to-sql-date 
  "Parse a string and turn is in an sql-date." [s]
  (let [parts (str/split s #"-")]
    (assert (= (count parts) 3))
    (let [lParts (str/split (last parts) #" ")
          cnt (count lParts)
          parts  (if (= cnt 1)
                   parts
                   (if (= cnt 2)
                     (let [times (str/split (second lParts) #":")
 ;;                          _ (println "last times" (last times))
                           sec   (first (str/split (last times) #"\."))] ;; discard ms
;;                       (println "sec="sec)
                       (concat (drop-last parts) (list (first lParts)) (take 2 times) (list sec)))
                     (throw (Exception. (str "(str-to-sql-date): "
                                  "Can not translate time-part of of string " s)))))]
          ;; assume parts contains year month day in sequence (3 elements)
          ;;   or year month day hour minute second (6 elements)
;;          (println (str/join ";" parts))
          (apply make-sql-date (map #(Integer/parseInt %) parts))))) 
	

(defn ymd-to-string 
  "Return a string with format 'YYYY-MM-DD' based on 'ymd'."
  [ymd]
  {:pre [(and (:year ymd) (:month ymd) (:day ymd))]}
  (let [{:keys [year month day]} ymd
	month (if (< month 10) (str "0" month) (str month))
	day   (if (< day 10) (str "0" day) (str day))]
    (str year "-" month "-" day)))


(defn get-ymd-date 
  "Get the year month and day of 'dt' as a hash-map containing the values as integers.
  The input 'dt' should be either a date or a date-string (yyyy-mm-dd').
  If 'dt' is omitted the current date is returned."
  ([] (get-ymd-date Now))
  ([dt]
     (let [dt (if (string? dt)
                dt
                (if (= (type dt) java.sql.Date)
                  (str dt)
                  (if (= (type dt) java.util.Date)
                    (str (get-timestamp dt))
                    (error "(get-ymd-date): type of " dt " not valid."))))
           values (re-find #"(\d{4})-(\d{2})-(\d{2})" dt)
           [year month day] (map #(Long/parseLong %) (drop 1 values))]
       {:year year
        :month month
        :day day})))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; conversion routines to ensure the correct types of data-values for sql
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(comment  ;; CASE dispatch on classnames does not work with type or class
  ;; java.lang.IllegalArgumentException: No matching clause: class java.sql.Timestamp
(defn convert-to-date [x] 
  (case (class x)
    java.sql.Date      x
    java.sql.Timestamp  (java.sql.Date. (.getTime x))
;;    ('java.util.Date java.sql.Timestamp)    (java.sql.Date. (.getTime x))                                                     
     ))
)

(defn convert-to-date 
  "Convert a date-like object or date-formated string to a java.sql.Date "
  [x] 
  (let [tp (type x)]
    (if (= tp java.sql.Date)
      x
      (if (or (= tp java.sql.Timestamp)
              (= tp java.util.Date))
        (java.sql.Date. (.getTime x))
        (if (= tp java.lang.Long)
          (java.sql.Date. x)
          (if (= tp java.lang.String)
            (str-to-sql-date x)
            (throw (Exception. (str "no conversion for value " x " of type " tp))))))))) 

(defn convert-to-timestamp 
  "Convert a date-like object or date-formated string to a java.sql.Timestamp "
  [x] 
  (let [tp (type x)]
    (if (= tp java.sql.Timestamp)
      x
      (if (or (= tp java.sql.Date)
              (= tp java.util.Date))
        (java.sql.Timestamp. (.getTime x))
        (if (= tp java.lang.Long)
          (java.sql.Timestamp. x)
          (if (= tp java.lang.String)
            (convert-to-timestamp (str-to-sql-date x))
            (throw (Exception. (str "no conversion for value " x " of type " tp))))))))) 




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; functions to manage offsets
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;



(defn get-day-of-week "Get day of week for a date-object using the java.util.GregorianCalendar (SUNDAY=1)."
  ([] (get-day-of-week Now))
  ([dt]
    (let [{:keys [year month day]} (get-ymd-date dt)
          cal  (java.util.GregorianCalendar. year (dec month) day)]
      (.get cal java.util.GregorianCalendar/DAY_OF_WEEK)
      )))

(defn get-TS-dayOffset
  "Get the sql-timestamp at a certain dayOffset of 'dt'. A dayOffset of -7 corresponds to last week."
  [dt dayOffset]
  (let [offset (* dayOffset Interval1dayMillis)
        millis (.getTime dt)
        newMillis (+ millis offset)]
    (java.sql.Timestamp. newMillis)))

(defn get-TS-minuteOffset
  "Get the sql-timestamp at a certain minuteOffset of 'dt'. A dayOffset of -1 corresponds to 1 minute earlier.
   Can operate on time-stamps and on java.util.Date. (java.sql.Date does not have sufficient precision)"
  [dt minuteOffset]
  (let [offset (* minuteOffset Interval1minuteMillis)
        millis (.getTime dt)
        newMillis (+ millis offset)]
    (java.sql.Timestamp. newMillis)))


(def zoneOffsetMillis (let [cal (java.util.Calendar/getInstance)
                            dst (.get cal java.util.Calendar/DST_OFFSET)
                            zone (.get cal java.util.Calendar/ZONE_OFFSET)]
                        (+ zone dst)))

(defn get-TS-midDay
  "Get an approximate timestamp at the middle of the Day. 
   Warning. A timezone correction is used to compensate for the offset relative to UTC.
   Therefore I add 10 hours to midnight (computation works for Europe, did not test wrap-around in Asia)."
  [dt]
  (let [ts (convert-to-timestamp dt)
        hrsOffs (- 12 (.getHours ts))
        mnsOffs (- 0 (.getMinutes ts))
        secOffs (- 0 (.getSeconds ts))
                
        offset  (* 1000 
                  (+ secOffs
                    (* 60
                      (+ mnsOffs
                        (*  60 hrsOffs)))))
        ;;_ (println "hrsOffs=" hrsOffs "   mnsOffs= " mnsOffs " secOffs=" secOffs "  total offset= " offset)
        newMillis (+ (.getTime ts) offset)]
    (java.sql.Timestamp. newMillis)))


(defn get-TS-endDay
  "Get an approximate timestamp at the middle of the Day. Warning. Assuming we are at GMT +2 hours.
   Therefore I add 10 hours to midnight (computation works for Europe, did not test wrap-around in Asia)."
  [dt]
  (let [ts (convert-to-timestamp dt)
        hrsOffs (- 23 (.getHours ts))
        mnsOffs (- 59 (.getMinutes ts))
        secOffs (- 59 (.getSeconds ts))
                
        offset  (* 1000 
                  (+ secOffs
                    (* 60
                      (+ mnsOffs
                        (*  60 hrsOffs)))))
        ;;_ (println "hrsOffs=" hrsOffs "   mnsOffs= " mnsOffs " secOffs=" secOffs "  total offset= " offset)
        newMillis (+ (.getTime ts) offset)]
    (java.sql.Timestamp. newMillis)))

(defn get-TS-startDay
  "Get an approximate timestamp at the middle of the Day. Warning. Assuming we are at GMT +2 hours.
   Therefore I add 10 hours to midnight (computation works for Europe, did not test wrap-around in Asia)."
  [dt]
  (let [ts (convert-to-timestamp dt)
        hrsOffs (- 0 (.getHours ts))
        mnsOffs (- 0 (.getMinutes ts))
        secOffs (- 0 (.getSeconds ts))
                
        offset  (* 1000 
                  (+ secOffs
                    (* 60
                      (+ mnsOffs
                        (*  60 hrsOffs)))))
        ;;_ (println "hrsOffs=" hrsOffs "   mnsOffs= " mnsOffs " secOffs=" secOffs "  total offset= " offset)
        newMillis (+ (.getTime ts) offset)]
    (java.sql.Timestamp. newMillis)))


(defn get-date-dayOffset
  "Get the sql-timestamp at a certain dayOffset of 'dt'. A dayOffset of -7 corresponds to last week.
   The offste is a whole day, so the time-of-day does not change."
  [dt dayOffset]
  (let [offset (* dayOffset Interval1dayMillis)
        millis (.getTime dt)
        newMillis (+ millis offset)]
    (java.sql.Date. newMillis)))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; functions that return a dateTime comparator
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn gen-date-comparator>= 
  "Return a closure (fn [date ...) that checks whether 'date' >= bound."
  [bound]
  (let [bound (if (number? bound)
                (long bound)
                (.getTime bound))]
  (fn [date]
    (>= (.getTime date) bound))))




(defn gen-date-comparator<=
  "Return a closure (fn [date ...) that checks whether 'date' <= bound."
  [bound]
  (let [bound (if (number? bound)
                (long bound)
                (.getTime bound))]
  (fn [date]
    (<= (.getTime date) bound))))

(defn gen-date-comparator-range
  "Return a closure (fn [date ...) that checks whether lower <= 'date' <= upper."
  [lower upper]
  (let [lower (gen-date-comparator>= lower)
        upper (gen-date-comparator<= upper)]
    (fn [date]
      (and (lower date) (upper date)))))


(defn gen-date-comparator<=endDay
  "Return a closure (fn [date ...) that checks whether 'date' <= end-of-day of the data represented by date. 
   Note, even java.sql.Date, which only prints to full days will carry time-information along, 
   which might result in unexpected comparisons."
  [bound]
    (gen-date-comparator<= (get-TS-endDay bound)))



(defn gen-date-comparator>=startDay
  "Return a closure (fn [date ...) that checks whether 'date' >= start-of-day of the data represented by date. 
   Note, even java.sql.Date, which only prints to full days will carry time-information along, 
   which might result in unexpected comparisons."
  [bound]
    (gen-date-comparator>= (get-TS-startDay bound)))

(defn gen-date-comparator-sameDay 
  "Return a closure (fn [date ...) that checks whether start-of-day <= 'date' <= end-of-day of the data represented by date. 
   Note, even java.sql.Date, which only prints to full days will carry time-information along, 
  which might result in unexpected comparisons."
  [day]
  (gen-date-comparator-range (get-TS-startDay day) (get-TS-endDay day)))