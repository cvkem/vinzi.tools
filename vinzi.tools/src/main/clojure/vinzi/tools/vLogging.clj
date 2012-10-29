(ns vinzi.tools.vLogging
   (:use	[clojure [pprint :only [pprint pp]]]
        [clojure [stacktrace :only [print-stack-trace root-cause]]])
  (:require [clojure.string :as str]
            [clojure.tools.logging :as ctl]))


(def logTrace (atom []))



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
