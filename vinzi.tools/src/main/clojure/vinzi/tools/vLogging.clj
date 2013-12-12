(ns vinzi.tools.vLogging
   (:use	[clojure [pprint :only [pprint pp]]]
        [clojure [stacktrace :only [print-stack-trace root-cause]]])
  (:require [clojure.string :as str]
            [clojure.tools.logging :as ctl]))


(def logTrace (atom []))

;; for each identical msg at most 'maxTraces will be generated
(def maxTraces 3)

;; when the logTrace exceeds 2*logTraceDepth levels it will be pruned to logTraceDepth
(def logTraceDepth 100)

(def trackTraceCount (atom {}))

;(defmacro wrap-item [x]
;     (if (or (and (= (type x) clojure.lang.PersistentList)
;                  (symbol? (first x)))
;             (= (type x) clojure.lang.Symbol))
;         (do 
;           (println  "Wrapped: " x)
;         `(fn [] ~x))
;         (do 
;           (println "NO wrap for: "x)
;         x)))

(defn unwrap-item [x]
  (if (and (fn? x)
           (:vLogging/exec (meta x)))  ;; function symbols should be printed. Only wrapper functions should be executed.
    (with-out-str (x))
    (if (and (vector? x)
             (= (:vExcept (meta x)) :with-out-str/pprint))
      (with-out-str (apply pprint x))
      (str x))))



(defmacro info "Wired directly to clojure.tools.logging/info"
  [& args]
  `(ctl/info ~@args))


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
    The log-level is added as first item of the list of log-items."
   ;; NOTE: if you use functions as argument, this might deplete perm-gen
   ;;  Each application generated a wrapper-function that is stored in a list.
   [level & args]
     `(vinzi.tools.vLogging/add-to-logTrace (concat ~level 
                      ~(for [x# `(list ~@args)]
                         (if (and (= (type x#) clojure.lang.PersistentList)
                                      (symbol? (first x#)))
                           (if (and (= (resolve (first x#)) #'clojure.core/with-out-str)
                                    (= (count (rest x#)) 1)
                                    (= (resolve (ffirst (rest x#))) #'clojure.pprint/pprint))
                             (with-meta   (vec (rest (first (rest x#)))) {:vExcept :with-out-str/pprint})
                             `(with-meta (fn [] ~x#) {:vLogging/exec true}))
                           ;; tried to detect function-symbols to prent a call ,
                           ;; however, determining whether a symbol binds to a function can only be detected at run-time.
;                           (if (symbol? x#)   ;; function-symbols should be printed, otherwise it will be executed
;                               `(with-out-str (print ~x#))
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
  [msg level]
  (let [ltHeader (str "<![START LOGTRACE[ " (str/upper-case (name level)) ": ")
        ;; get the current log-trace (or its tail)
        lt (let [maxTraceItems (if (= level :warn) 10 100)
                 lt @logTrace
                 dropTraceItems (- maxTraceItems (count lt))]
             ;; TODO (if (> dropTraceItems 0) ) ;; PREFIX A MESSAGE ABOUT DROPPING
             (drop dropTraceItems lt))
        ltFooter "]END LOGTRACE]>"
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
      ;; TODO: should it be better to print the trace one level higher at warn-level (or should we prune logging via the log4j.xml/logback file"))
      (let [fullMsg (str ltHeader msg "\n" (str/join "\n" (map line-string lt (rest (range)))) ltFooter)]
        (if (= level :warn)
          (ctl/warn fullMsg)
          (ctl/error fullMsg))))))

(defn ddebug 
  "This function outputs directly to the log-file, and does not wait for an error of warning to emit its message."
  [& args]
  (let [fullMsg (apply str args)]
    (ctl/debug fullMsg)))

(defmacro warn
  "Print the full logTrace to the log-file with args as a prefix-message at warn-level."
  [& args]
  `(let [msg# (str ~@args)]
     (print-logTrace msg# :warn)))

(defmacro error 
  "Print the full logTrace to the log-file with args as a prefix-message at error-level."
  [& args]
  `(let [msg# (str ~@args)]
     (print-logTrace msg# :error)))



(defn force-realization 
  "Recursively force realization of datastructure x.
   This function is used to enforce that printing to standard-output
   is realizated before "
  [x]
  (if (map? x)
    (doall (zipmap (keys x) (doall (map force-realization (vals x)))))
    (if (vector? x)
      (vec (map force-realization x)) ;; vector forces realization
      (if (sequential? x)
        (doall (map force-realization x))
        x))))
        
(defmacro lfs 
  "Capture the output of the second application of of form 'x'. 
  The first iteration of is thrown away to ensure the print-statements by 
   lazy computations does not get mingled with the desired output
   for logging (and is neither printed to the standard-output).
   Of course form 'x' should be free of side-effects."
  [x] 
  `(do (force-realization ~x)  
       (with-out-str ~x)))

(defmacro lpps 
  "Capture the pprinted version of form 'x'. The first iteration of
   (pprint ..)  is thrown away to ensure the print-statements by 
   lazy computations does not get mingled with the desired output
   for logging (and is neither printed to the standard-output)."
  [x] 
   `(do (force-realization ~x)
        (with-out-str (pprint ~x)))) 

