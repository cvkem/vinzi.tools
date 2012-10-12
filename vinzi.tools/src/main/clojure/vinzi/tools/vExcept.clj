(ns vinzi.tools.vExcept
  (:use	[clojure [pprint :only [pprint pp]]]
        [clojure [stacktrace :only [print-stack-trace root-cause]]]
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


(defn report 
  "Print report for an exception, if this exception has not been reported already.    
   (including one step higher in the exception-chain)."
  [msg e]
  (let [msg (if (is-lastException? e)
              msg
              (with-out-str
                      (println msg 
                               "\nException of type: " (class e))
                      (print-stack-trace (root-cause e))
                      (println "Message: " (.getMessage e))
                      (when (isa? (class e) java.sql.SQLException)
                        (println "\tSQL-related Exception details:"
                                 "\n\tErrorCode: " (.getErrorCode e)
                                 "\n\tSQLState:  " (.getSQLState e))
                        (when-let [n (.getNextException e)]
                          (println "\nNext-message: " (.getMessage n)
                                   "\n\tNext-errorcode: " (.getErrorCode n))))))]
    (set-lastException e)
    (error msg)))



(defn report-rethrow 
  "Report the message and the exception and throw the rethrow the exception."
  [msg e]
  (report msg e)
  (throw e))
 

(comment 
  ;; testcode
  
  )
  