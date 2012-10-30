(ns vinzi.tools.vLogging
   (:use	[clojure [pprint :only [pprint pp]]]
        [clojure [stacktrace :only [print-stack-trace root-cause]]])
  (:require [clojure.string :as str]
            [clojure.tools.logging :as ctl]))


(def logTrace (atom []))

(def maxTraces 3)

;; when the logTrace exceeds 2*logTraceDepth levels it will be pruned to logTraceDepth
(def logTraceDepth 3)

(def trackTraceCount (atom {}))

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

;;  Old version of code without limitations to the depth of the trail
;;  (and more inlining of code).
;;
;(defmacro vLogging-it 
;   "Add logging information to logTrace (not shown yet).
;    The log-level is addedas first item of the list of log-items."
;   [level & args]
;     `(swap! vinzi.tools.vLogging/logTrace 
;             (fn [~'lt] 
;               (conj ~'lt (concat ~level 
;                      ~(for [x# `(list ~@args)]
;                         (if (and (= (type x#) clojure.lang.PersistentList)
;                                      (symbol? (first x#)))
;                             `(fn [] ~x#)
;                             x#)))))))

(defn add-to-logTrace 
  "Add the trace as last item to logTrace (and limit length of the logTrace)."
  [trace]
  (swap! vinzi.tools.vLogging/logTrace 
                   (fn [lt] 
                     (let [lt (if (>= (count lt) (* 2 logTraceDepth))
                                (vec (drop logTraceDepth lt))  lt)]
                       (conj lt trace))))
  nil) ;; no direct access to queue needed


(defmacro vLogging-it 
   "Add logging information to logTrace (not shown yet).
    The log-level is addedas first item of the list of log-items."
   [level & args]
     `(vinzi.tools.vLogging/add-to-logTrace (concat ~level 
                      ~(for [x# `(list ~@args)]
                         (if (and (= (type x#) clojure.lang.PersistentList)
                                      (symbol? (first x#)))
                             `(fn [] ~x#)
                             x#)))))

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
        get-msg-keyword (fn []
                          ;; get the keyword by removing #123 infixes and suffixes from the message.
                          ;;  (Used to make unique messages that map to the same trackTraceCount key.
                          (keyword (str/replace msg #"\s*#\d+\s*" "")))
        update-cnt (fn []
                    ;; Increase the count for current key and return the current occurance-count.
                    (let [msgKey (get-msg-keyword)]
                      (msgKey (swap! trackTraceCount update-in [msgKey] (fn [v] (if (nil? v) 1 (inc v)))))))
        line-string (fn [lineItems lineNo]
                     (str lineNo " "(first lineItems) ": "
                          (str/join " " (map unwrap-item (rest lineItems)))))]
    (clear-logTrace)
    (when (<= (update-cnt) maxTraces)
      (ctl/debug msg ":\n" (str/join "\n" (map line-string lt (rest (range))))))))

(defmacro error 
  "Print the full logTrace with args as a prefix-message."
  [& args]
  `(let [msg# (str ~@args)]
     (print-logTrace msg#)))


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
