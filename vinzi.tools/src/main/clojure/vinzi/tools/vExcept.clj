(ns vinzi.tools.vExcept
  (:use	[clojure [pprint :only [pprint pp]]]
        [clojure [stacktrace :only [print-stack-trace root-cause
              print-cause-trace print-throwable]]]
        [clojure.tools [logging :only [error info trace debug warn]]])
  (:require [clojure.string :as str]))
           



(def lastException (atom nil))

(defn is-lastException? [e]
  (= e @lastException))

(defn set-lastException [ex]
  (swap! lastException (fn [_] ex)))

(defn clear-lastException []
  (set-lastException nil))

;(defmacro throw-except
;  "Construct an exception and throw it at the spot (just saving a few keystrokes)."
;  ([msg]
;    `(throw (Exception. ~msg)))
;  ([msg cause]
;    `(throw (Exception. ~msg ~cause))))
(defmacro throw-except
  "Construct an exception with all msg-items concatenated and throw it on the spot (macro) 
   If the last item of msgCause is a throwable it will be assumed to be a cause."
  [& msgCause]
  ;; TODO: remove the apply st or let it be evaluated at run-time to expand arguments correctly
    (let [msg (if (isa? (last msgCause) java.lang.Throwable)
                           (drop-last msgCause)
                           msgCause)
          cause (when (isa? (last msgCause) java.lang.Throwable)
                  (last msgCause))]
      (if cause
        `(throw (Exception. (str ~@msg) ~cause))    
        `(throw (Exception. (str ~@msg))))))

(defmacro report-throw-except
  "Report the exception and subsequently throw it."
  [& msgCause]
  ;; TODO: remove the apply st or let it be evaluated at run-time to expand arguments correctly
    (let [msg (if (isa? (last msgCause) java.lang.Throwable)
                           (drop-last msgCause)
                           msgCause)
          cause (when (isa? (last msgCause) java.lang.Throwable)
                  (last msgCause))]
      (if cause
        `(let [except# (Exception. (str ~@msg) ~cause)]
           (vinzi.tools.vExcept/report except#)
           (throw except#))
        `(let [except# (Exception. (str ~@msg))]
           (vinzi.tools.vExcept/report except#)
           (throw except#)))))


(defn sql-except-str-extension
  "Show an extension of the stack-trace for SQLExceptions."
  [e]
  (when (isa? (class e) java.sql.SQLException)
    (str "\tSQL-related Exception details:"
         "\n\tErrorCode: " (.getErrorCode e)
         "\n\tSQLState:  " (.getSQLState e)
      (when-let [n (.getNextException e)]
          (str "\nNext-message: " (.getMessage n)
               (when (isa? (class e) java.sql.SQLException)
                   "\n\tNext-errorcode: " (.getErrorCode n)))))))

;; the Clojure trace tracks down all causes (not just the root-cause)
(def ClojureTrace true)
(def CauseDepth 10)

(defn except-str
  "Extract an exception-msg from 'e' and prefix is with 'msg'. 
   If 'e' is an sqlException some additional fields and the next-exception are reported." 
  [msg e]
  (if e
    (with-out-str
      (if ClojureTrace
        (do  
          (when (seq msg)
            (println "DETAILS: " msg))
          (print-throwable e)
          (println "\nSTACKTRACE:")
          (print-stack-trace e)
          (println "CAUSE-TRACE with depth " CauseDepth ":")
          (print-cause-trace e CauseDepth))
        (do 
          (println msg 
                 "\nException of type: " (class e))
          (if-let [rootCause (root-cause e)]
            (print-stack-trace rootCause)
            (println "No root-cause given"))
          (println "Message: " (.getMessage e))))

      ;; additional reporting on sql-exceptions
      (println (sql-except-str-extension e))
      ;;  OLD CODE (replaced by line above.
      #_(when (isa? (class e) java.sql.SQLException)
        (println "\tSQL-related Exception details:"
                 "\n\tErrorCode: " (.getErrorCode e)
                 "\n\tSQLState:  " (.getSQLState e))
        (when-let [n (.getNextException e)]
          (println "\nNext-message: " (.getMessage n)
                   "\n\tNext-errorcode: " (.getErrorCode n))))
      )
    (str msg  "(Exception is nil)")))

(defn report 
  "If this specific exception was not reported already it will be expanded with a (clojure-) stack-trace,
   the expanded exception will be reported in the logging system and nil is returned.
   If this exception matches the last exception it is reported, but the messag is not expanded."
  ([e] (report "" e))
  ([msg e]
    (let [msg (if (is-lastException? e) 
                msg 
                (except-str msg e))]
      (set-lastException e)
      (when (seq msg)
      (error msg)))))



(defn report-rethrow 
  "Report the message and the exception and rethrow the exception."
  [msg e]
  (report msg e)
  (throw e))
 

(comment 

;; typical time for a single subgroup  
;  (def NUMPARSE 1000000)
;; typical time for NO subgroup  
; (time (def x (doall (map #(re-find #"\s+\d*" (str "   0234" %)) (range NUMPARSE)))))
;"Elapsed time: 1059.111982 msecs"
;; typical time for a single subgroup  
;  => (time (def x (doall (map #(re-find #"\s+(\d*) " (str "   0234" %)) (range NUMPARSE)))))
;"Elapsed time: 2075.392263 msecs"
;#'user/x
;;  So a regexp sub-group doubles the time needed
  
  ;; testcode, to check influence on depth of strack-trail on timing of exception-handling
  
  (def NUMPARSE 1000000)
  
  (defn pl-base 
    "Base case to measure timing of an empyt loop (only generates list of integers."
    [] 
    (println "generate string. Num iter: " NUMPARSE) 
    (time (def x (doall (map #(try 
                                 (str %)
                                (catch Exception e)  ;; catch exception and ignore it
                                )(range NUMPARSE))))))
  (defn pl 
    "Base case to measure timing of parsing a series of strings (only generates list of integers)."
    [] 
    (println "parse-long. Num iter: " NUMPARSE) 
    (time (def x (doall (map #(try 
                                (Long/parseLong (str %))
                                (catch Exception e)  ;; catch exception and ignore it
                                )(range NUMPARSE))))))
  
  (defn ple 
    "Code throws an arity exception."
    [] 
    (println "parse-long EXCEPTION. Num iter: " NUMPARSE) 
    (time (def x (doall (map #(try 
                                (Long/parseLong str %)
                                (catch Exception e)  ;; catch exception and ignore it
                                )(range NUMPARSE))))))
  
 (defn exec-at-level 
   "Build a call-stack of depth 'level' levels, and execute function 'f' at this level."
   [level f]
  (if (= level 0)
    (f)
    (exec-at-level (dec level) f))) 
 
(defn test-ple 
  "Run functions pl and ple at different depths of the calling stack to measure time needed for generating an exception"
  [maxLevel]
  (println "Run functions pl and ple at different depths of the calling stack to measure time needed for generating an exception")
  (loop [level 1]
  (when (<= level maxLevel)
    (do
      (println "\nlevel=" level)
      (exec-at-level level pl-base)
      (exec-at-level level pl)
      (exec-at-level level ple)
      (recur (* level 10)))))
  (println "TEST FINISHED"))

(test-ple 1000)
 
;;  CONCLUSION: 
;;     1. Exception-handling does not take too much time (5 times less than a Integer/parseInt)
;;     2. Call stack depth does not have an influence on the timing
;;
;;;  OUTPUT: 
;=> (test-ple 1000)
;Run functions pl and ple at different depths of the calling stack to measure time needed for generating an exception
;
;level= 1
;generate string. Num iter:  1000000
;"Elapsed time: 199.335669 msecs"
;parse-long. Num iter:  1000000
;"Elapsed time: 293.405727 msecs"
;parse-long EXCEPTION. Num iter:  1000000
;"Elapsed time: 126.070213 msecs"
;
;level= 10
;generate string. Num iter:  1000000
;"Elapsed time: 198.469079 msecs"
;parse-long. Num iter:  1000000
;"Elapsed time: 278.709166 msecs"
;parse-long EXCEPTION. Num iter:  1000000
;"Elapsed time: 109.066096 msecs"
;
;level= 100
;generate string. Num iter:  1000000
;"Elapsed time: 162.361697 msecs"
;parse-long. Num iter:  1000000
;"Elapsed time: 583.499299 msecs"
;parse-long EXCEPTION. Num iter:  1000000
;"Elapsed time: 96.351911 msecs"
;
;level= 1000
;generate string. Num iter:  1000000
;"Elapsed time: 174.5923 msecs"
;parse-long. Num iter:  1000000
;"Elapsed time: 581.710806 msecs"
;parse-long EXCEPTION. Num iter:  1000000
;"Elapsed time: 101.065358 msecs"
;TEST FINISHED

 )  ;; end comment

