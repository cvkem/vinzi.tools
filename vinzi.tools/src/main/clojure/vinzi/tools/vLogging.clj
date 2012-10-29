(ns vinzi.tools.vLogging
   (:use	[clojure [pprint :only [pprint pp]]]
        [clojure [stacktrace :only [print-stack-trace root-cause]]])
  (:require [clojure.string :as str]
            [clojure.tools.logging :as ctl]))


(def logTrace (atom []))


(defmacro wrap-it [args]
   (for [x args]
     (if (or (and (= (type x) clojure.lang.PersistentList)
                  (symbol? (first x)))
             (= (type x) clojure.lang.Symbol))
         (do 
           (println  "Wrapped: " x)
         `(fn [] ~x))
         (do 
           (println "NO wrap for: "x)
         x))))

;; function will force evaluation of it's argument, so a macro is needed
(defn wrap-it-func [args]
   (for [x args]
     (if (or (and (= (type x) clojure.lang.PersistentList)
                  (symbol? (first x)))
             (= (type x) clojure.lang.Symbol))
         (fn [] x)
       x)))

(defn show-it [x]
  (if (and (= (type x) clojure.lang.PersistentList)
           (symbol? (first x)))
    (x)
    x))

;; example:
;; 
;=> (map show-it (wrap-it-func `("ABC" ~x)))
;("ABC" {:a 1, :b 3})
;=> (debug ("ABC" x))

;(defmacro debug [& args]
;  (println "received args: " args)
;  (let [wargs (wrap-it args)]
;     (println "PREPARED LOG_LINE: " wargs)
;     `(swap! vinzi.tools.vLogging/logTrace (fn [~'lt] (conj ~'lt ~@wargs)))))

;(defmacro debug [& args]
;  (println "received args: " args)
;     `(swap! vinzi.tools.vLogging/logTrace 
;             (fn [~'lt] 
;               (conj ~'lt 
;                      ~@args
;                    ;  ~(wrap-it args)
;                      ))))


;;; remark by Stuart Sierra on bug in Clojure
;; In general, I think, you cannot rely on being able to eval function objects in Clojure, regardless of whether or not they are closures. 
;;It happens to work for some simple examples, mostly to simplify the explanation of things like (eval (list + 1 2)). 
;;Macros should always return literal source code as data structures, not compiled functions.
;; Sniplet of: http://stackoverflow.com/questions/11191992/functions-with-closures-and-eval-in-clojure

;(defmacro debug [& args]
;  (println "received args: " args)
;     `(swap! vinzi.tools.vLogging/logTrace 
;             (fn [~'lt] 
;               (conj ~'lt (vec (quote
;                      ~(for [x# args]
;                         (if (or (and (= (type x#) clojure.lang.PersistentList)
;                                      (symbol? (first x#)))
;                                 (symbol? x#))
;                           (do 
;                             (println  "Wrapped: " x#)
;                             (fn [] x#))
;                           (do 
;                             (println "NO wrap for: " x#)
;                             (eval x#)))))))        )))


;; debug only pushing values
;(defmacro debug [& args]
;  (println "received args: " args)
;     `(swap! vinzi.tools.vLogging/logTrace 
;             (fn [~'lt] 
;               (conj ~'lt (vector ~@args  )))))

(defmacro wrap-item [x]
     (if (or (and (= (type x) clojure.lang.PersistentList)
                  (symbol? (first x)))
             (= (type x) clojure.lang.Symbol))
         (do 
           (println  "Wrapped: " x)
         `(fn [] ~x))
         (do 
           (println "NO wrap for: "x)
         x)))

(defn unwrap-item [x]
  (if (fn? x)
    (with-out-str (x))
    (str x)))

;(defn print-trace [msg]
;  (let [lt @logTrace]
;    (swap! logTrace (fn[_] []))
;    (println "About to submit debug-line: ")
;    (ctl/debug msg ": " (str/join " " (map show-it lt)))))


; basic variant that stores values (and maps identity over it) (identity should be replaced of course)
;(defmacro debug [& args]
;  (println "received args: " args)
;     `(swap! vinzi.tools.vLogging/logTrace 
;             (fn [~'lt] 
;               (conj ~'lt (vec (map identity (list ~@args))  )))))
;; NOTE: in code above we can not replace identity by a function (as it evaluates arguments)
;;  and we can not change it to a macro (macro's can not be passed a mapping function).


(defmacro info "Wired directly to clojure.tools.logging/info"
  [& args]
  `(ctl/info ~@args))

(defmacro warn "Wired directly to clojure.tools.logging/info"
  [& args]
  `(ctl/warn ~@args))


(defmacro vLogging-it 
   "Add logging information to logTrace (not shown yet).
    The log-level is addedas first item of the list of log-items."
   [level & args]
;;  (println "received args: " args)
     `(swap! vinzi.tools.vLogging/logTrace 
             (fn [~'lt] 
               (conj ~'lt (concat ~level 
                      ~(for [x# `(list ~@args)]
                         (if (and (= (type x#) clojure.lang.PersistentList)
                                      (symbol? (first x#)))
                             `(fn [] ~x#)
                             x#)))))))

(defmacro debug 
  "Add logging information to logTrace (not shown yet)."
  [& args]
  `(vLogging-it "D" ~@args))

(defmacro trace 
  "Add logging information to logTrace (not shown yet)."
  [& args]
  `(vLogging-it "T" ~@args))

(defn clear-logTrace 
    "Remove all pending log-messages from the logTrace 
   (signals that the last phase was finished succesfully.)"
    []
  (swap! logTrace (fn[_] [])))

(defn print-logTrace
  "Print a numbered logTrace prefixed by a message.
   All lines are prefixed by a number and a token D(debug) or T(race) t,o indicate the log-level."
  [msg]
  (let [lt @logTrace
        line-string (fn [lineItems lineNo]
                     (str lineNo " "(first lineItems) ": "
                          (str/join " " (map unwrap-item (rest lineItems)))))]
    (clear-logTrace)
    (ctl/debug msg ":\n" (str/join "\n" (map line-string lt (rest (range)))))))

(defmacro error 
  "Print the full logTrace with args as a prefix-message."
  [& args]
  `(let [msg# (str ~@args)]
     (print-trace msg#)))


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
